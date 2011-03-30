/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;
import org.springframework.beans.BeansException;
import org.springframework.beans.ConfigurablePropertyAccessor;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.document.mongodb.MongoPropertyDescriptors.MongoPropertyDescriptor;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.document.mongodb.convert.SimpleMongoConverter;
import org.springframework.data.document.mongodb.mapping.event.AfterConvertEvent;
import org.springframework.data.document.mongodb.mapping.event.AfterLoadEvent;
import org.springframework.data.document.mongodb.mapping.event.AfterSaveEvent;
import org.springframework.data.document.mongodb.mapping.event.BeforeConvertEvent;
import org.springframework.data.document.mongodb.mapping.event.BeforeSaveEvent;
import org.springframework.data.document.mongodb.mapping.event.MongoMappingEvent;
import org.springframework.data.document.mongodb.query.IndexDefinition;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.document.mongodb.query.Update;
import org.springframework.jca.cci.core.ConnectionCallback;
import org.springframework.util.Assert;

/**
 * Primary implementation of {@link MongoOperations}.
 *
 * @author Thomas Risberg
 * @author Graeme Rocher
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class MongoTemplate implements InitializingBean, MongoOperations, ApplicationContextAware, ApplicationEventPublisherAware {

  private static final Log LOGGER = LogFactory.getLog(MongoTemplate.class);

  private static final String ID = "_id";

  /*
    * WriteConcern to be used for write operations if it has been specified. Otherwise
    * we should not use a WriteConcern defaulting to the one set for the DB or Collection.
    */
  private WriteConcern writeConcern = null;

  /*
    * WriteResultChecking to be used for write operations if it has been specified. Otherwise
    * we should not do any checking.
    */
  private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;

  private MongoConverter mongoConverter;
  private final Mongo mongo;
  private final MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();

  private String defaultCollectionName;
  private String databaseName;
  private String username;
  private String password;
  private ApplicationContext applicationContext;
  private ApplicationEventPublisher eventPublisher;
  private ExecutorService eventPublishers = Executors.newCachedThreadPool();
  private LinkedBlockingQueue<ApplicationEvent> eventQueue = new LinkedBlockingQueue<ApplicationEvent>();

  /**
   * Constructor used for a basic template configuration
   *
   * @param mongo
   * @param databaseName
   */
  public MongoTemplate(Mongo mongo, String databaseName) {
    this(mongo, databaseName, null, null, null, null);
  }

  /**
   * Constructor used for a basic template configuration with a specific {@link com.mongodb.WriteConcern}
   * to be used for all database write operations
   *
   * @param mongo
   * @param databaseName
   * @param writeConcern
   */
  public MongoTemplate(Mongo mongo, String databaseName, WriteConcern writeConcern, WriteResultChecking writeResultChecking) {
    this(mongo, databaseName, null, null, writeConcern, writeResultChecking);
  }

  /**
   * Constructor used for a basic template configuration with a default collection name
   *
   * @param mongo
   * @param databaseName
   * @param defaultCollectionName
   */
  public MongoTemplate(Mongo mongo, String databaseName, String defaultCollectionName) {
    this(mongo, databaseName, defaultCollectionName, null, null, null);
  }

  /**
   * Constructor used for a basic template configuration with a default collection name and
   * with a specific {@link com.mongodb.WriteConcern} to be used for all database write operations
   *
   * @param mongo
   * @param databaseName
   * @param defaultCollectionName
   * @param writeConcern
   */
  public MongoTemplate(Mongo mongo, String databaseName, String defaultCollectionName, WriteConcern writeConcern, WriteResultChecking writeResultChecking) {
    this(mongo, databaseName, defaultCollectionName, null, writeConcern, writeResultChecking);
  }

  /**
   * Constructor used for a template configuration with a default collection name and a custom {@link org.springframework.data.document.mongodb.convert.MongoConverter}
   *
   * @param mongo
   * @param databaseName
   * @param defaultCollectionName
   * @param mongoConverter
   */
  public MongoTemplate(Mongo mongo, String databaseName, String defaultCollectionName, MongoConverter mongoConverter) {
    this(mongo, databaseName, defaultCollectionName, mongoConverter, null, null);
  }

  /**
   * Constructor used for a template configuration with a default collection name and a custom {@link MongoConverter}
   * and with a specific {@link com.mongodb.WriteConcern} to be used for all database write operations
   *
   * @param mongo
   * @param databaseName
   * @param defaultCollectionName
   * @param mongoConverter
   * @param writeConcern
   * @param writeResultChecking
   */
  public MongoTemplate(Mongo mongo, String databaseName, String defaultCollectionName, MongoConverter mongoConverter, WriteConcern writeConcern, WriteResultChecking writeResultChecking) {

    Assert.notNull(mongo);
    Assert.notNull(databaseName);

    this.defaultCollectionName = defaultCollectionName;
    this.mongo = mongo;
    this.databaseName = databaseName;
    this.writeConcern = writeConcern;
    if (writeResultChecking != null) {
      this.writeResultChecking = writeResultChecking;
    }
    setMongoConverter(mongoConverter == null ? new SimpleMongoConverter() : mongoConverter);
  }

  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
    this.eventPublisher = applicationEventPublisher;
  }

  /**
   * Sets the username to use to connect to the Mongo database
   *
   * @param username The username to use
   */
  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * Sets the password to use to authenticate with the Mongo database.
   *
   * @param password The password to use
   */
  public void setPassword(String password) {

    this.password = password;
  }

  /**
   * Sets the name of the default collection to be used.
   *
   * @param defaultCollectionName
   */
  public void setDefaultCollectionName(String defaultCollectionName) {
    this.defaultCollectionName = defaultCollectionName;
  }

  /**
   * Sets the database name to be used.
   *
   * @param databaseName
   */
  public void setDatabaseName(String databaseName) {
    Assert.notNull(databaseName);
    this.databaseName = databaseName;
  }

  /**
   * Returns the default {@link org.springframework.data.document.mongodb.convert.MongoConverter}.
   *
   * @return
   */
  public MongoConverter getConverter() {
    return this.mongoConverter;
  }

  public void setConverter(MongoConverter converter) {
    this.mongoConverter = converter;
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#getDefaultCollectionName()
    */
  public String getDefaultCollectionName() {
    return defaultCollectionName;
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#getDefaultCollection()
    */
  public DBCollection getDefaultCollection() {

    return execute(new DbCallback<DBCollection>() {
      public DBCollection doInDB(DB db) throws MongoException, DataAccessException {
        return db.getCollection(getDefaultCollectionName());
      }
    });
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#executeCommand(java.lang.String)
    */
  public CommandResult executeCommand(String jsonCommand) {
    return executeCommand((DBObject) JSON.parse(jsonCommand));
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#executeCommand(com.mongodb.DBObject)
    */
  public CommandResult executeCommand(final DBObject command) {

    CommandResult result = execute(new DbCallback<CommandResult>() {
      public CommandResult doInDB(DB db) throws MongoException, DataAccessException {
        return db.command(command);
      }
    });

    String error = result.getErrorMessage();
    if (error != null) {
      // TODO: allow configuration of logging level / throw
      //	throw new InvalidDataAccessApiUsageException("Command execution of " +
      //			command.toString() + " failed: " + error);
      LOGGER.warn("Command execution of " +
          command.toString() + " failed: " + error);
    }
    return result;
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.DBCallback)
    */
  public <T> T execute(DbCallback<T> action) {

    Assert.notNull(action);

    try {
      DB db = getDb();
      return action.doInDB(db);
    } catch (MongoException e) {
      throw potentiallyConvertRuntimeException(e);
    }
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.CollectionCallback)
    */
  public <T> T execute(CollectionCallback<T> callback) {
    return execute(getDefaultCollectionName(), callback);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.CollectionCallback, java.lang.String)
    */
  public <T> T execute(String collectionName, CollectionCallback<T> callback) {

    Assert.notNull(callback);

    try {
      DBCollection collection = getDb().getCollection(collectionName);
      return callback.doInCollection(collection);
    } catch (MongoException e) {
      throw potentiallyConvertRuntimeException(e);
    }
  }

  /**
   * Central callback executing method to do queries against the datastore that requires reading a single object from a
   * collection of objects. It will take the following steps <ol> <li>Execute the given {@link ConnectionCallback} for a
   * {@link DBObject}.</li> <li>Apply the given
   * {@link DbObjectCallback} to each of the {@link DBObject}s to obtain the result.</li> <ol>
   *
   * @param <T>
   * @param collectionCallback the callback to retrieve the {@link DBObject} with
   * @param objectCallback     the {@link DbObjectCallback} to transform {@link DBObject}s into the actual domain type
   * @param collectionName     the collection to be queried
   * @return
   */
  private <T> T execute(CollectionCallback<DBObject> collectionCallback,
                        DbObjectCallback<T> objectCallback, String collectionName) {

    try {
      T result = objectCallback.doWith(collectionCallback.doInCollection(getCollection(collectionName)));
      return result;
    } catch (MongoException e) {
      throw potentiallyConvertRuntimeException(e);
    }
  }

  /**
   * Central callback executing method to do queries against the datastore that requires reading a collection of
   * objects. It will take the following steps <ol> <li>Execute the given {@link ConnectionCallback} for a
   * {@link DBCursor}.</li> <li>Prepare that {@link DBCursor} with the given {@link CursorPreparer} (will be skipped
   * if {@link CursorPreparer} is {@literal null}</li> <li>Iterate over the {@link DBCursor} and applies the given
   * {@link DbObjectCallback} to each of the {@link DBObject}s collecting the actual result {@link List}.</li> <ol>
   *
   * @param <T>
   * @param collectionCallback the callback to retrieve the {@link DBCursor} with
   * @param preparer           the {@link CursorPreparer} to potentially modify the {@link DBCursor} before ireating over it
   * @param objectCallback     the {@link DbObjectCallback} to transform {@link DBObject}s into the actual domain type
   * @param collectionName     the collection to be queried
   * @return
   */
  private <T> List<T> executeEach(CollectionCallback<DBCursor> collectionCallback, CursorPreparer preparer,
                                  DbObjectCallback<T> objectCallback, String collectionName) {

    try {
      DBCursor cursor = collectionCallback.doInCollection(getCollection(collectionName));

      if (preparer != null) {
        cursor = preparer.prepare(cursor);
      }

      List<T> result = new ArrayList<T>();

      for (DBObject object : cursor) {
        result.add(objectCallback.doWith(object));
      }

      return result;
    } catch (MongoException e) {
      throw potentiallyConvertRuntimeException(e);
    }
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#executeInSession(org.springframework.data.document.mongodb.DBCallback)
    */
  public <T> T executeInSession(final DbCallback<T> action) {

    return execute(new DbCallback<T>() {
      public T doInDB(DB db) throws MongoException, DataAccessException {
        try {
          db.requestStart();
          return action.doInDB(db);
        } finally {
          db.requestDone();
        }
      }
    });
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#createCollection(java.lang.String)
    */
  public DBCollection createCollection(final String collectionName) {
    return doCreateCollection(collectionName, new BasicDBObject());
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#createCollection(java.lang.String, org.springframework.data.document.mongodb.CollectionOptions)
    */
  public DBCollection createCollection(final String collectionName, final CollectionOptions collectionOptions) {
    return doCreateCollection(collectionName, convertToDbObject(collectionOptions));
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.String)
    */
  public DBCollection getCollection(final String collectionName) {
    return execute(new DbCallback<DBCollection>() {
      public DBCollection doInDB(DB db) throws MongoException, DataAccessException {
        return db.getCollection(collectionName);
      }
    });
  }


  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#collectionExists(java.lang.String)
    */
  public boolean collectionExists(final String collectionName) {
    return execute(new DbCallback<Boolean>() {
      public Boolean doInDB(DB db) throws MongoException, DataAccessException {
        return db.collectionExists(collectionName);
      }
    });
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#dropCollection(java.lang.String)
    */
  public void dropCollection(String collectionName) {

    execute(collectionName, new CollectionCallback<Void>() {
      public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        collection.drop();
        return null;
      }
    });
  }

  // Indexing methods

  public void ensureIndex(IndexDefinition indexDefinition) {
    ensureIndex(getDefaultCollectionName(), indexDefinition);
  }

  public void ensureIndex(String collectionName, final IndexDefinition indexDefinition) {
    execute(collectionName, new CollectionCallback<Object>() {
      public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        DBObject indexOptions = indexDefinition.getIndexOptions();
        if (indexOptions != null) {
          collection.ensureIndex(indexDefinition.getIndexObject(), indexOptions);
        } else {
          collection.ensureIndex(indexDefinition.getIndexObject());
        }
        return null;
      }
    });
  }

  // Find methods that take a Query to express the query and that return a single object.

  public <T> T findOne(Query query, Class<T> targetClass) {
    return findOne(getDefaultCollectionName(), query, targetClass);
  }

  public <T> T findOne(Query query, Class<T> targetClass,
                       MongoReader<T> reader) {
    return findOne(getDefaultCollectionName(), query, targetClass, reader);
  }

  public <T> T findOne(String collectionName, Query query,
                       Class<T> targetClass) {
    return findOne(collectionName, query, targetClass, null);
  }

  public <T> T findOne(String collectionName, Query query,
                       Class<T> targetClass, MongoReader<T> reader) {
    return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), targetClass, reader);
  }

  // Find methods that take a Query to express the query and that return a List of objects.

  public <T> List<T> find(Query query, Class<T> targetClass) {
    return find(getDefaultCollectionName(), query, targetClass);
  }

  public <T> List<T> find(Query query, Class<T> targetClass, MongoReader<T> reader) {
    return find(getDefaultCollectionName(), query, targetClass, reader);
  }

  public <T> List<T> find(String collectionName, final Query query, Class<T> targetClass) {
    CursorPreparer cursorPreparer = null;
    if (query.getSkip() > 0 || query.getLimit() > 0 || query.getSortObject() != null) {
      cursorPreparer = new CursorPreparer() {

        public DBCursor prepare(DBCursor cursor) {
          DBCursor cursorToUse = cursor;
          try {
            if (query.getSkip() > 0) {
              cursorToUse = cursorToUse.skip(query.getSkip());
            }
            if (query.getLimit() > 0) {
              cursorToUse = cursorToUse.limit(query.getLimit());
            }
            if (query.getSortObject() != null) {
              cursorToUse = cursorToUse.sort(query.getSortObject());
            }
          } catch (MongoException e) {
            throw potentiallyConvertRuntimeException(e);
          }
          return cursorToUse;
        }
      };
    }
    return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), targetClass, cursorPreparer);
  }

  public <T> List<T> find(String collectionName, Query query, Class<T> targetClass, MongoReader<T> reader) {
    return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), targetClass, reader);
  }

  public <T> List<T> find(String collectionName, Query query,
                          Class<T> targetClass, CursorPreparer preparer) {
    return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), targetClass, preparer);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#insert(java.lang.Object)
    */
  public void insert(Object objectToSave) {
    insert(getRequiredDefaultCollectionName(), objectToSave);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#insert(java.lang.String, java.lang.Object)
    */
  public void insert(String collectionName, Object objectToSave) {
    insert(collectionName, objectToSave, this.mongoConverter);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#insert(T, org.springframework.data.document.mongodb.MongoWriter)
    */
  public <T> void insert(T objectToSave, MongoWriter<T> writer) {
    insert(getDefaultCollectionName(), objectToSave, writer);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#insert(java.lang.String, T, org.springframework.data.document.mongodb.MongoWriter)
    */
  public <T> void insert(String collectionName, T objectToSave, MongoWriter<T> writer) {
    BasicDBObject dbDoc = new BasicDBObject();

    maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave));
    writer.write(objectToSave, dbDoc);

    maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc));
    Object id = insertDBObject(collectionName, dbDoc);

    populateIdIfNecessary(objectToSave, id);
    maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc));
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.util.List)
    */
  public void insertList(List<? extends Object> listToSave) {
    insertList(getRequiredDefaultCollectionName(), listToSave);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.lang.String, java.util.List)
    */
  public void insertList(String collectionName, List<? extends Object> listToSave) {
    insertList(collectionName, listToSave, this.mongoConverter);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.util.List, org.springframework.data.document.mongodb.MongoWriter)
    */
  public <T> void insertList(List<? extends T> listToSave, MongoWriter<T> writer) {
    insertList(getDefaultCollectionName(), listToSave, writer);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.lang.String, java.util.List, org.springframework.data.document.mongodb.MongoWriter)
    */
  public <T> void insertList(String collectionName, List<? extends T> listToSave, MongoWriter<T> writer) {

    Assert.notNull(writer);

    List<DBObject> dbObjectList = new ArrayList<DBObject>();
    for (T o : listToSave) {
      BasicDBObject dbDoc = new BasicDBObject();

      maybeEmitEvent(new BeforeConvertEvent<T>(o));
      writer.write(o, dbDoc);

      maybeEmitEvent(new BeforeSaveEvent<T>(o, dbDoc));
      dbObjectList.add(dbDoc);
    }
    List<ObjectId> ids = insertDBObjectList(collectionName, dbObjectList);
    for (int i = 0; i < listToSave.size(); i++) {
      if (i < ids.size()) {
        T obj = listToSave.get(i);
        populateIdIfNecessary(obj, ids.get(i));
        maybeEmitEvent(new AfterSaveEvent<T>(obj, dbObjectList.get(i)));
      }
    }
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#save(java.lang.Object)
    */
  public void save(Object objectToSave) {
    save(getRequiredDefaultCollectionName(), objectToSave);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#save(java.lang.String, java.lang.Object)
    */
  public void save(String collectionName, Object objectToSave) {
    save(collectionName, objectToSave, this.mongoConverter);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#save(T, org.springframework.data.document.mongodb.MongoWriter)
    */
  public <T> void save(T objectToSave, MongoWriter<T> writer) {
    save(getDefaultCollectionName(), objectToSave, writer);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#save(java.lang.String, T, org.springframework.data.document.mongodb.MongoWriter)
    */
  public <T> void save(String collectionName, T objectToSave, MongoWriter<T> writer) {
    BasicDBObject dbDoc = new BasicDBObject();

    maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave));
    writer.write(objectToSave, dbDoc);

    maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc));
    Object id = saveDBObject(collectionName, dbDoc);

    populateIdIfNecessary(objectToSave, id);
    maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc));
  }


  protected Object insertDBObject(String collectionName, final DBObject dbDoc) {

    if (dbDoc.keySet().isEmpty()) {
      return null;
    }

    return execute(collectionName, new CollectionCallback<Object>() {
      public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        if (writeConcern == null) {
          collection.insert(dbDoc);
        } else {
          collection.insert(dbDoc, writeConcern);
        }
        return dbDoc.get(ID);
      }
    });
  }

  protected List<ObjectId> insertDBObjectList(String collectionName, final List<DBObject> dbDocList) {

    if (dbDocList.isEmpty()) {
      return Collections.emptyList();
    }

    execute(collectionName, new CollectionCallback<Void>() {
      public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        if (writeConcern == null) {
          collection.insert(dbDocList);
        } else {
          collection.insert(dbDocList.toArray((DBObject[]) new BasicDBObject[dbDocList.size()]), writeConcern);
        }
        return null;
      }
    });

    List<ObjectId> ids = new ArrayList<ObjectId>();
    for (DBObject dbo : dbDocList) {
      Object id = dbo.get(ID);
      if (id instanceof ObjectId) {
        ids.add((ObjectId) id);
      } else {
        // no id was generated
        ids.add(null);
      }
    }
    return ids;
  }

  protected Object saveDBObject(String collectionName, final DBObject dbDoc) {

    if (dbDoc.keySet().isEmpty()) {
      return null;
    }

    return execute(collectionName, new CollectionCallback<Object>() {
      public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        if (writeConcern == null) {
          collection.save(dbDoc);
        } else {
          collection.save(dbDoc, writeConcern);
        }
        return dbDoc.get(ID);
      }
    });
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#updateFirst(com.mongodb.DBObject, com.mongodb.DBObject)
    */
  public WriteResult updateFirst(Query query, Update update) {
    return updateFirst(getRequiredDefaultCollectionName(), query, update);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#updateFirst(java.lang.String, com.mongodb.DBObject, com.mongodb.DBObject)
    */
  public WriteResult updateFirst(String collectionName, final Query query, final Update update) {
    return execute(collectionName, new CollectionCallback<WriteResult>() {
      public WriteResult doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        WriteResult wr;
        if (writeConcern == null) {
          wr = collection.update(query.getQueryObject(), update.getUpdateObject());
        } else {
          wr = collection.update(query.getQueryObject(), update.getUpdateObject(), false, false, writeConcern);
        }
        handleAnyWriteResultErrors(wr, query.getQueryObject(), "update with '" + update.getUpdateObject() + "'");
        return wr;
      }
    });
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#updateMulti(com.mongodb.DBObject, com.mongodb.DBObject)
    */
  public WriteResult updateMulti(Query query, Update update) {
    return updateMulti(getRequiredDefaultCollectionName(), query, update);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#updateMulti(java.lang.String, com.mongodb.DBObject, com.mongodb.DBObject)
    */
  public WriteResult updateMulti(String collectionName, final Query query, final Update update) {
    return execute(collectionName, new CollectionCallback<WriteResult>() {
      public WriteResult doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        WriteResult wr = null;
        if (writeConcern == null) {
          wr = collection.updateMulti(query.getQueryObject(), update.getUpdateObject());
        } else {
          wr = collection.update(query.getQueryObject(), update.getUpdateObject(), false, true, writeConcern);
        }
        handleAnyWriteResultErrors(wr, query.getQueryObject(), "update with '" + update.getUpdateObject() + "'");
        return wr;
      }
    });
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#remove(com.mongodb.DBObject)
    */
  public void remove(Query query) {
    remove(getRequiredDefaultCollectionName(), query);
  }

  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#remove(java.lang.String, com.mongodb.DBObject)
    */
  public void remove(String collectionName, final Query query) {
    execute(collectionName, new CollectionCallback<Void>() {
      public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        WriteResult wr = null;
        if (writeConcern == null) {
          wr = collection.remove(query.getQueryObject());
        } else {
          wr = collection.remove(query.getQueryObject(), writeConcern);
        }
        handleAnyWriteResultErrors(wr, query.getQueryObject(), "remove");
        return null;
      }
    });
  }


  /* (non-Javadoc)
    * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.Class)
    */
  public <T> List<T> getCollection(Class<T> targetClass) {
    return executeEach(new FindCallback(null), null, new ReadDbObjectCallback<T>(mongoConverter, targetClass),
        getDefaultCollectionName());
  }

  public <T> List<T> getCollection(String collectionName, Class<T> targetClass) {
    return executeEach(new FindCallback(null), null, new ReadDbObjectCallback<T>(mongoConverter, targetClass),
        collectionName);
  }

  public Set<String> getCollectionNames() {
    return execute(new DbCallback<Set<String>>() {
      public Set<String> doInDB(DB db) throws MongoException, DataAccessException {
        return db.getCollectionNames();
      }
    });
  }

  public <T> List<T> getCollection(String collectionName, Class<T> targetClass, MongoReader<T> reader) {
    return executeEach(new FindCallback(null), null, new ReadDbObjectCallback<T>(reader, targetClass),
        collectionName);
  }

  public DB getDb() {
    return MongoDbUtils.getDB(mongo, databaseName, username, password == null ? null : password.toCharArray());
  }

  protected <T> void maybeEmitEvent(MongoMappingEvent<T> event) {
    if (null != eventPublisher) {
      eventPublisher.publishEvent(event);
    }
  }

  /**
   * Create the specified collection using the provided options
   *
   * @param collectionName
   * @param collectionOptions
   * @return the collection that was created
   */
  protected DBCollection doCreateCollection(final String collectionName, final DBObject collectionOptions) {
    return execute(new DbCallback<DBCollection>() {
      public DBCollection doInDB(DB db) throws MongoException, DataAccessException {
        DBCollection coll = db.createCollection(collectionName, collectionOptions);
        // TODO: Emit a collection created event
        return coll;
      }
    });
  }

  /**
   * Map the results of an ad-hoc query on the default MongoDB collection to an object using the provided MongoReader
   * <p/>
   * The query document is specified as a standard DBObject and so is the fields specification.
   *
   * @param collectionName name of the collection to retrieve the objects from
   * @param query          the query document that specifies the criteria used to find a record
   * @param fields         the document that specifies the fields to be returned
   * @param targetClass    the parameterized type of the returned list.
   * @param reader         the MongoReader to convert from DBObject to an object.
   * @return the List of converted objects.
   */
  protected <T> T doFindOne(String collectionName, DBObject query, DBObject fields, Class<T> targetClass, MongoReader<T> reader) {
    MongoReader<? super T> readerToUse = reader;
    if (readerToUse == null) {
      readerToUse = this.mongoConverter;
    }
    substituteMappedIdIfNecessary(query, targetClass, readerToUse);
    return execute(new FindOneCallback(query, fields), new ReadDbObjectCallback<T>(readerToUse, targetClass),
        collectionName);
  }

  /**
   * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type.
   * <p/>
   * The object is converted from the MongoDB native representation using an instance of
   * {@see MongoConverter}.  Unless configured otherwise, an
   * instance of SimpleMongoConverter will be used.
   * <p/>
   * The query document is specified as a standard DBObject and so is the fields specification.
   * <p/>
   * Can be overridden by subclasses.
   *
   * @param collectionName name of the collection to retrieve the objects from
   * @param query          the query document that specifies the criteria used to find a record
   * @param fields         the document that specifies the fields to be returned
   * @param targetClass    the parameterized type of the returned list.
   * @param preparer       allows for customization of the DBCursor used when iterating over the result set,
   *                       (apply limits, skips and so on).
   * @return the List of converted objects.
   */
  protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> targetClass, CursorPreparer preparer) {
    substituteMappedIdIfNecessary(query, targetClass, mongoConverter);
    return executeEach(new FindCallback(query, fields), preparer, new ReadDbObjectCallback<T>(mongoConverter, targetClass),
        collectionName);
  }

  /**
   * Map the results of an ad-hoc query on the default MongoDB collection to a List using the provided MongoReader
   * <p/>
   * The query document is specified as a standard DBObject and so is the fields specification.
   *
   * @param collectionName name of the collection to retrieve the objects from
   * @param query          the query document that specifies the criteria used to find a record
   * @param fields         the document that specifies the fields to be returned
   * @param targetClass    the parameterized type of the returned list.
   * @param reader         the MongoReader to convert from DBObject to an object.
   * @return the List of converted objects.
   */
  protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> targetClass, MongoReader<T> reader) {
    substituteMappedIdIfNecessary(query, targetClass, reader);
    return executeEach(new FindCallback(query, fields), null, new ReadDbObjectCallback<T>(reader, targetClass),
        collectionName);
  }

  protected DBObject convertToDbObject(CollectionOptions collectionOptions) {
    DBObject dbo = new BasicDBObject();
    if (collectionOptions != null) {
      if (collectionOptions.getCapped() != null) {
        dbo.put("capped", collectionOptions.getCapped().booleanValue());
      }
      if (collectionOptions.getSize() != null) {
        dbo.put("size", collectionOptions.getSize().intValue());
      }
      if (collectionOptions.getMaxDocuments() != null) {
        dbo.put("max", collectionOptions.getMaxDocuments().intValue());
      }
    }
    return dbo;
  }

  /**
   * Populates the id property of the saved object, if it's not set already.
   *
   * @param savedObject
   * @param id
   */
  protected void populateIdIfNecessary(Object savedObject, Object id) {

    if (id == null) {
      return;
    }

    ConfigurablePropertyAccessor bw = PropertyAccessorFactory.forDirectFieldAccess(savedObject);
    MongoPropertyDescriptor idDescriptor = new MongoPropertyDescriptors(savedObject.getClass()).getIdDescriptor();

    if (idDescriptor == null) {
      return;
    }

    if (bw.getPropertyValue(idDescriptor.getName()) == null) {
      Object target = null;
      if (id instanceof ObjectId) {
        target = this.mongoConverter.convertObjectId((ObjectId) id, idDescriptor.getPropertyType());
      } else {
        target = id;
      }
      bw.setPropertyValue(idDescriptor.getName(), target);
    }
  }

  /**
   * Substitutes the id key if it is found in he query. Any 'id' keys will be replaced with '_id' and the value converted
   * to an ObjectId if possible. This conversion should match the way that the id fields are converted during read
   * operations.
   *
   * @param query
   * @param targetClass
   * @param reader
   */
  protected void substituteMappedIdIfNecessary(DBObject query, Class<?> targetClass, MongoReader<?> reader) {
    MongoConverter converter = null;
    if (reader instanceof SimpleMongoConverter) {
      converter = (MongoConverter) reader;
    } else {
      return;
    }
    String idKey = null;
    if (query.containsField("id")) {
      idKey = "id";
    }
    if (query.containsField("_id")) {
      idKey = "_id";
    }
    if (idKey == null) {
      // no ids in this query
      return;
    }
    final MongoPropertyDescriptor descriptor;
    try {
      descriptor = new MongoPropertyDescriptor(new PropertyDescriptor(idKey, targetClass), targetClass);
    } catch (IntrospectionException e) {
      // no property descriptor for this key
      return;
    }
    if (descriptor.isIdProperty() && descriptor.isOfIdType()) {
      Object value = query.get(idKey);
      ObjectId newValue = null;
      try {
        if (value instanceof String && ObjectId.isValid((String) value)) {
          newValue = converter.convertObjectId(value);
        }
      } catch (ConversionFailedException iae) {
        LOGGER.warn("Unable to convert the String " + value + " to an ObjectId");
      }
      query.removeField(idKey);
      if (newValue != null) {
        query.put(MongoPropertyDescriptor.ID_KEY, newValue);
      } else {
        query.put(MongoPropertyDescriptor.ID_KEY, value);
      }
    }
  }


  private String getRequiredDefaultCollectionName() {
    String name = getDefaultCollectionName();
    if (name == null) {
      throw new IllegalStateException(
          "No 'defaultCollection' or 'defaultCollectionName' specified. Check configuration of MongoTemplate.");
    }
    return name;
  }

  /**
   * Checks and handles any errors.
   * <p/>
   * TODO: current implementation logs errors - will be configurable to log warning, errors or
   * throw exception in later versions
   */
  private void handleAnyWriteResultErrors(WriteResult wr, DBObject query, String operation) {
    if (WriteResultChecking.NONE == this.writeResultChecking) {
      return;
    }
    String error = wr.getError();
    int n = wr.getN();
    if (error != null) {
      String message = "Execution of '" + operation +
          (query == null ? "" : "' using '" + query.toString() + "' query") + " failed: " + error;
      if (WriteResultChecking.EXCEPTION == this.writeResultChecking) {
        throw new DataIntegrityViolationException(message);
      } else {
        LOGGER.error(message);
      }
    } else if (n == 0) {
      String message = "Execution of '" + operation +
          (query == null ? "" : "' using '" + query.toString() + "' query") + " did not succeed: 0 documents updated";
      if (WriteResultChecking.EXCEPTION == this.writeResultChecking) {
        throw new DataIntegrityViolationException(message);
      } else {
        LOGGER.warn(message);
      }
    }

  }

  /**
   * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
   * exception if the conversation failed. Thus allows safe rethrowing of the return value.
   *
   * @param ex
   * @return
   */
  private RuntimeException potentiallyConvertRuntimeException(RuntimeException ex) {
    RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(ex);
    return resolved == null ? ex : resolved;
  }

  private void initializeMappingMongoConverter(MappingMongoConverter converter) {
    converter.setMongo(mongo);
    converter.setDefaultDatabase(databaseName);
  }

  /*
  * (non-Javadoc)
  * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
  */
  public void afterPropertiesSet() {
    if (this.getDefaultCollectionName() != null) {
      if (!collectionExists(getDefaultCollectionName())) {
        createCollection(getDefaultCollectionName(), null);
      }
    }
    if (null != applicationContext) {
      eventPublishers.submit(new Runnable() {
        public void run() {
          while (true) {
            ApplicationEvent event = null;
            try {
              event = eventQueue.take();
              applicationContext.publishEvent(event);
            } catch (InterruptedException e) {
              throw new RuntimeException(e.getMessage(), e);
            }
          }
        }
      });
    }
  }


  /**
   * Simple {@link CollectionCallback} that takes a query {@link DBObject} plus an optional fields specification
   * {@link DBObject} and executes that against the {@link DBCollection}.
   *
   * @author Oliver Gierke
   * @author Thomas Risberg
   */
  private static class FindOneCallback implements CollectionCallback<DBObject> {

    private final DBObject query;

    private final DBObject fields;

    public FindOneCallback(DBObject query, DBObject fields) {
      this.query = query;
      this.fields = fields;
    }

    public DBObject doInCollection(DBCollection collection) throws MongoException, DataAccessException {
      if (fields == null) {
        return collection.findOne(query);
      } else {
        return collection.findOne(query, fields);
      }
    }
  }

  /**
   * Simple {@link CollectionCallback} that takes a query {@link DBObject} plus an optional fields specification
   * {@link DBObject} and executes that against the {@link DBCollection}.
   *
   * @author Oliver Gierke
   * @author Thomas Risberg
   */
  private static class FindCallback implements CollectionCallback<DBCursor> {

    private final DBObject query;

    private final DBObject fields;

    public FindCallback(DBObject query) {
      this(query, null);
    }

    public FindCallback(DBObject query, DBObject fields) {
      this.query = query;
      this.fields = fields;
    }

    public DBCursor doInCollection(DBCollection collection) throws MongoException, DataAccessException {
      if (fields == null) {
        return collection.find(query);
      } else {
        return collection.find(query, fields);
      }
    }
  }

  /**
   * Simple internal callback to allow operations on a {@link DBObject}.
   *
   * @author Oliver Gierke
   */

  private interface DbObjectCallback<T> {

    T doWith(DBObject object);
  }

  /**
   * Simple {@link DbObjectCallback} that will transform {@link DBObject} into the given target type using the given
   * {@link MongoReader}.
   *
   * @author Oliver Gierke
   */
  private class ReadDbObjectCallback<T> implements DbObjectCallback<T> {

    private final MongoReader<? super T> reader;
    private final Class<T> type;

    public ReadDbObjectCallback(MongoReader<? super T> reader, Class<T> type) {
      this.reader = reader;
      this.type = type;
    }

    public T doWith(DBObject object) {
      if (null != object) {
        maybeEmitEvent(new AfterLoadEvent<DBObject>(object));
      }
      T source = reader.read(type, object);
      if (null != source) {
        maybeEmitEvent(new AfterConvertEvent<T>(object, source));
      }
      return source;
    }
  }

  public void setMongoConverter(MongoConverter converter) {
    this.mongoConverter = converter;
    if (null != converter && converter instanceof MappingMongoConverter) {
      initializeMappingMongoConverter((MappingMongoConverter) mongoConverter);
    }
  }

  public void setWriteResultChecking(WriteResultChecking resultChecking) {
    this.writeResultChecking = resultChecking;
  }

}
