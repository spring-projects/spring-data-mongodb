/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *			http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb;

import static org.springframework.data.document.mongodb.query.Criteria.*;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.document.mongodb.convert.SimpleMongoConverter;
import org.springframework.data.document.mongodb.index.IndexDefinition;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntity;
import org.springframework.data.document.mongodb.mapping.MongoPersistentProperty;
import org.springframework.data.document.mongodb.mapping.event.AfterConvertEvent;
import org.springframework.data.document.mongodb.mapping.event.AfterLoadEvent;
import org.springframework.data.document.mongodb.mapping.event.AfterSaveEvent;
import org.springframework.data.document.mongodb.mapping.event.BeforeConvertEvent;
import org.springframework.data.document.mongodb.mapping.event.BeforeSaveEvent;
import org.springframework.data.document.mongodb.mapping.event.MongoMappingEvent;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.document.mongodb.query.QueryMapper;
import org.springframework.data.document.mongodb.query.Update;
import org.springframework.data.mapping.MappingBeanHelper;
import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.MappingException;
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
public class MongoTemplate implements MongoOperations, ApplicationEventPublisherAware {

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

	private final MongoConverter mongoConverter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoDbFactory mongoDbFactory;
	private final MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	private final QueryMapper mapper;

	private ApplicationEventPublisher eventPublisher;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param mongo
	 * @param databaseName
	 */
	public MongoTemplate(Mongo mongo, String databaseName) {
		this(mongo, databaseName, null);
	}

	/**
	 * Constructor used for a template configuration with a custom
	 * {@link org.springframework.data.document.mongodb.convert.MongoConverter}
	 * 
	 * @param mongo
	 * @param databaseName
	 * @param mongoConverter
	 */
	public MongoTemplate(Mongo mongo, String databaseName, MongoConverter mongoConverter) {
		this(new MongoDbFactoryBean(mongo, databaseName), mongoConverter, null, null);
	}

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param mongoDbFactory
	 */
	public MongoTemplate(MongoDbFactory mongoDbFactory) {
		this(mongoDbFactory, null);
	}

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param mongoDbFactory
	 * @param mongoConverter
	 */
	public MongoTemplate(MongoDbFactory mongoDbFactory, MongoConverter mongoConverter) {
		this(mongoDbFactory, null, null, null);
	}

	/**
	 * Constructor used for a template configuration with a custom {@link MongoConverter} and with a specific
	 * {@link com.mongodb.WriteConcern} to be used for all database write operations
	 * 
	 * @param mongo
	 * @param databaseName
	 * @param mongoConverter
	 * @param writeConcern
	 * @param writeResultChecking
	 */
	MongoTemplate(MongoDbFactory mongoDbFactory, MongoConverter mongoConverter, WriteConcern writeConcern,
			WriteResultChecking writeResultChecking) {

		Assert.notNull(mongoDbFactory);

		this.mongoDbFactory = mongoDbFactory;
		this.writeConcern = writeConcern;
		this.mongoConverter = mongoConverter == null ? getDefaultMongoConverter() : mongoConverter;

		if (this.mongoConverter instanceof MappingMongoConverter) {
			initializeMappingMongoConverter((MappingMongoConverter) this.mongoConverter);
		}

		this.mappingContext = this.mongoConverter.getMappingContext();
		this.mapper = new QueryMapper(this.mongoConverter);

		if (writeResultChecking != null) {
			this.writeResultChecking = writeResultChecking;
		}
	}

	private final MongoConverter getDefaultMongoConverter() {

		SimpleMongoConverter converter = new SimpleMongoConverter();
		converter.afterPropertiesSet();
		return converter;
	}

	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.eventPublisher = applicationEventPublisher;
	}

	/**
	 * Returns the default {@link org.springframework.data.document.mongodb.convert.MongoConverter}.
	 * 
	 * @return
	 */
	public MongoConverter getConverter() {
		return this.mongoConverter;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getDefaultCollectionName()
	 */
	public String getCollectionName(Class<?> clazz) {
		return this.determineCollectionName(clazz);
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
			// throw new InvalidDataAccessApiUsageException("Command execution of " +
			// command.toString() + " failed: " + error);
			LOGGER.warn("Command execution of " + command.toString() + " failed: " + error);
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.DBCallback)
	 */
	public <T> T execute(DbCallback<T> action) {

		Assert.notNull(action);

		try {
			DB db = this.getDb();
			return action.doInDB(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.CollectionCallback)
	 */
	public <T> T execute(Class<?> entityClass, CollectionCallback<T> callback) {
		return execute(determineCollectionName(entityClass), callback);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.CollectionCallback, java.lang.String)
	 */
	public <T> T execute(String collectionName, CollectionCallback<T> callback) {

		Assert.notNull(callback);

		try {
			DBCollection collection = getDb().getCollection(collectionName);
			return callback.doInCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	/**
	 * Central callback executing method to do queries against the datastore that requires reading a single object from a
	 * collection of objects. It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ConnectionCallback} for a {@link DBObject}.</li>
	 * <li>Apply the given {@link DbObjectCallback} to each of the {@link DBObject}s to obtain the result.</li>
	 * <ol>
	 * 
	 * @param <T>
	 * @param collectionCallback
	 *          the callback to retrieve the {@link DBObject} with
	 * @param objectCallback
	 *          the {@link DbObjectCallback} to transform {@link DBObject}s into the actual domain type
	 * @param collectionName
	 *          the collection to be queried
	 * @return
	 */
	private <T> T execute(CollectionCallback<DBObject> collectionCallback, DbObjectCallback<T> objectCallback,
			String collectionName) {

		try {
			T result = objectCallback.doWith(collectionCallback.doInCollection(getCollection(collectionName)));
			return result;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	/**
	 * Central callback executing method to do queries against the datastore that requires reading a collection of
	 * objects. It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ConnectionCallback} for a {@link DBCursor}.</li>
	 * <li>Prepare that {@link DBCursor} with the given {@link CursorPreparer} (will be skipped if {@link CursorPreparer}
	 * is {@literal null}</li>
	 * <li>Iterate over the {@link DBCursor} and applies the given {@link DbObjectCallback} to each of the
	 * {@link DBObject}s collecting the actual result {@link List}.</li>
	 * <ol>
	 * 
	 * @param <T>
	 * @param collectionCallback
	 *          the callback to retrieve the {@link DBCursor} with
	 * @param preparer
	 *          the {@link CursorPreparer} to potentially modify the {@link DBCursor} before ireating over it
	 * @param objectCallback
	 *          the {@link DbObjectCallback} to transform {@link DBObject}s into the actual domain type
	 * @param collectionName
	 *          the collection to be queried
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
		} catch (RuntimeException e) {
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

	public void ensureIndex(Class<?> entityClass, IndexDefinition indexDefinition) {
		ensureIndex(determineCollectionName(entityClass), indexDefinition);
	}

	public void ensureIndex(String collectionName, final IndexDefinition indexDefinition) {
		execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject indexOptions = indexDefinition.getIndexOptions();
				if (indexOptions != null) {
					collection.ensureIndex(indexDefinition.getIndexKeys(), indexOptions);
				} else {
					collection.ensureIndex(indexDefinition.getIndexKeys());
				}
				return null;
			}
		});
	}

	// Find methods that take a Query to express the query and that return a single object.

	public <T> T findOne(Query query, Class<T> targetClass) {
		return findOne(determineCollectionName(targetClass), query, targetClass);
	}

	public <T> T findOne(String collectionName, Query query, Class<T> targetClass) {
		return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), targetClass);
	}

	// Find methods that take a Query to express the query and that return a List of objects.

	public <T> List<T> find(Query query, Class<T> targetClass) {
		return find(determineCollectionName(targetClass), query, targetClass);
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
					} catch (RuntimeException e) {
						throw potentiallyConvertRuntimeException(e);
					}
					return cursorToUse;
				}
			};
		}
		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), targetClass, cursorPreparer);
	}

	public <T> List<T> find(String collectionName, Query query, Class<T> targetClass, CursorPreparer preparer) {
		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), targetClass, preparer);
	}

	// Find methods that take a Query to express the query and that return a single object that is
	// also removed from the collection in the database.

	public <T> T findAndRemove(Query query, Class<T> targetClass) {
		return findAndRemove(determineCollectionName(targetClass), query, targetClass);
	}

	public <T> T findAndRemove(String collectionName, Query query, Class<T> targetClass) {
		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(), query.getSortObject(),
				targetClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insert(java.lang.Object)
	 */
	public void insert(Object objectToSave) {
		insert(determineEntityCollectionName(objectToSave), objectToSave);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insert(java.lang.String, java.lang.Object)
	 */
	public void insert(String collectionName, Object objectToSave) {
		doInsert(collectionName, objectToSave, this.mongoConverter);
	}

	protected <T> void doInsert(String collectionName, T objectToSave, MongoWriter<T> writer) {
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
		doInsertList(listToSave, mongoConverter);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.lang.String, java.util.List)
	 */
	public void insertList(String collectionName, List<? extends Object> listToSave) {
		doInsertList(collectionName, listToSave, this.mongoConverter);
	}

	protected <T> void doInsertList(List<? extends T> listToSave, MongoWriter<T> writer) {
		Map<String, List<T>> objs = new HashMap<String, List<T>>();

		for (T o : listToSave) {

			MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(o.getClass());
			if (entity == null) {
				throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
						+ o.getClass().getName());
			}
			String collection = entity.getCollection();

			List<T> objList = objs.get(collection);
			if (null == objList) {
				objList = new ArrayList<T>();
				objs.put(collection, objList);
			}
			objList.add(o);

		}

		for (Map.Entry<String, List<T>> entry : objs.entrySet()) {
			doInsertList(entry.getKey(), entry.getValue(), this.mongoConverter);
		}
	}

	protected <T> void doInsertList(String collectionName, List<? extends T> listToSave, MongoWriter<T> writer) {

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
		save(determineEntityCollectionName(objectToSave), objectToSave);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#save(java.lang.String, java.lang.Object)
	 */
	public void save(String collectionName, Object objectToSave) {
		doSave(collectionName, objectToSave, this.mongoConverter);
	}

	protected <T> void doSave(String collectionName, T objectToSave, MongoWriter<T> writer) {
		BasicDBObject dbDoc = new BasicDBObject();

		maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave));
		writer.write(objectToSave, dbDoc);

		maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc));
		Object id = saveDBObject(collectionName, dbDoc);

		populateIdIfNecessary(objectToSave, id);
		maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc));
	}

	protected Object insertDBObject(String collectionName, final DBObject dbDoc) {

		// DATADOC-95: This will prevent null objects from being saved.
		// if (dbDoc.keySet().isEmpty()) {
		// return null;
		// }

		// TODO: Need to move this to more central place
		if (dbDoc.containsField("_id")) {
			if (dbDoc.get("_id") instanceof String) {
				ObjectId oid = convertIdValue(this.mongoConverter, dbDoc.get("_id"));
				if (oid != null) {
					dbDoc.put("_id", oid);
				}
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insert DBObject containing fields: " + dbDoc.keySet() + " in collection: " + collectionName);
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

		// TODO: Need to move this to more central place
		for (DBObject dbDoc : dbDocList) {
			if (dbDoc.containsField("_id")) {
				if (dbDoc.get("_id") instanceof String) {
					ObjectId oid = convertIdValue(this.mongoConverter, dbDoc.get("_id"));
					if (oid != null) {
						dbDoc.put("_id", oid);
					}
				}
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insert list of DBObjects containing " + dbDocList.size() + " items");
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

		// TODO: Need to move this to more central place
		if (dbDoc.containsField("_id")) {
			if (dbDoc.get("_id") instanceof String) {
				ObjectId oid = convertIdValue(this.mongoConverter, dbDoc.get("_id"));
				if (oid != null) {
					dbDoc.put("_id", oid);
				}
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("save DBObject containing fields: " + dbDoc.keySet());
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
	public WriteResult updateFirst(Class<?> entityClass, Query query, Update update) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#updateFirst(java.lang.String, com.mongodb.DBObject, com.mongodb.DBObject)
	 */
	public WriteResult updateFirst(final String collectionName, final Query query, final Update update) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#updateMulti(com.mongodb.DBObject, com.mongodb.DBObject)
	 */
	public WriteResult updateMulti(Class<?> entityClass, Query query, Update update) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#updateMulti(java.lang.String, com.mongodb.DBObject, com.mongodb.DBObject)
	 */
	public WriteResult updateMulti(String collectionName, final Query query, final Update update) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	protected WriteResult doUpdate(final String collectionName, final Query query, final Update update,
			final Class<?> entityClass, final boolean upsert, final boolean multi) {

		return execute(collectionName, new CollectionCallback<WriteResult>() {
			public WriteResult doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject queryObj = query.getQueryObject();
				DBObject updateObj = update.getUpdateObject();

				String idProperty = "id";
				if (null != entityClass) {
					idProperty = getPersistentEntity(entityClass).getIdProperty().getName();
				}
				for (String key : queryObj.keySet()) {
					if (idProperty.equals(key)) {
						// This is an ID field
						queryObj.put(ID, mongoConverter.maybeConvertObject(queryObj.get(key)));
						queryObj.removeField(key);
					} else {
						queryObj.put(key, mongoConverter.maybeConvertObject(queryObj.get(key)));
					}
				}

				for (String key : updateObj.keySet()) {
					updateObj.put(key, mongoConverter.maybeConvertObject(updateObj.get(key)));
				}

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("calling update using query: " + queryObj + " and update: " + updateObj + " in collection: "
							+ collectionName);
				}

				WriteResult wr;
				if (writeConcern == null) {
					if (multi) {
						wr = collection.updateMulti(queryObj, updateObj);
					} else {
						wr = collection.update(queryObj, updateObj);
					}
				} else {
					wr = collection.update(queryObj, updateObj, upsert, multi, writeConcern);
				}
				handleAnyWriteResultErrors(wr, queryObj, "update with '" + updateObj + "'");
				return wr;
			}
		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#remove(com.mongodb.DBObject)
	 */
	public void remove(Query query) {
		remove(query, null);
	}

	public void remove(Object object) {
		Object idValue = this.getIdValue(object);
		remove(new Query(whereId().is(idValue)), object.getClass());
	}

	public <T> void remove(Query query, Class<T> targetClass) {
		Assert.notNull(query);
		remove(determineCollectionName(targetClass), query, targetClass);
	}

	public <T> void remove(String collectionName, final Query query, Class<T> targetClass) {
		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to remove can't be null");
		}
		final DBObject queryObject = query.getQueryObject();
		final MongoPersistentEntity<?> entity = getPersistentEntity(targetClass);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("remove using query: " + queryObject + " in collection: " + collectionName);
		}
		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject dboq = mapper.getMappedObject(queryObject, entity);
				WriteResult wr = null;
				if (writeConcern == null) {
					wr = collection.remove(dboq);
				} else {
					wr = collection.remove(dboq, writeConcern);
				}
				handleAnyWriteResultErrors(wr, dboq, "remove");
				return null;
			}
		});
	}

	/* (non-Javadoc)
		* @see org.springframework.data.document.mongodb.MongoOperations#remove(java.lang.String, com.mongodb.DBObject)
		*/
	public void remove(String collectionName, final Query query) {
		remove(collectionName, query, null);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.Class)
	 */
	public <T> List<T> getCollection(Class<T> targetClass) {
		return executeEach(new FindCallback(null), null, new ReadDbObjectCallback<T>(mongoConverter, targetClass),
				determineCollectionName(targetClass));
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

	public DB getDb() {
		return mongoDbFactory.getDb();
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
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter
	 * <p/>
	 * The query document is specified as a standard DBObject and so is the fields specification.
	 * 
	 * @param collectionName
	 *          name of the collection to retrieve the objects from
	 * @param query
	 *          the query document that specifies the criteria used to find a record
	 * @param fields
	 *          the document that specifies the fields to be returned
	 * @param targetClass
	 *          the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> T doFindOne(String collectionName, DBObject query, DBObject fields, Class<T> targetClass) {
		MongoReader<? super T> readerToUse = this.mongoConverter;
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(targetClass);
		DBObject mappedQuery = mapper.getMappedObject(query, entity);

		return execute(new FindOneCallback(mappedQuery, fields), new ReadDbObjectCallback<T>(readerToUse, targetClass),
				collectionName);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * The query document is specified as a standard DBObject and so is the fields specification.
	 * <p/>
	 * Can be overridden by subclasses.
	 * 
	 * @param collectionName
	 *          name of the collection to retrieve the objects from
	 * @param query
	 *          the query document that specifies the criteria used to find a record
	 * @param fields
	 *          the document that specifies the fields to be returned
	 * @param targetClass
	 *          the parameterized type of the returned list.
	 * @param preparer
	 *          allows for customization of the DBCursor used when iterating over the result set, (apply limits, skips and
	 *          so on).
	 * @return the List of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> targetClass,
			CursorPreparer preparer) {
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(targetClass);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: " + query + " fields: " + fields + " for class: " + targetClass
					+ " in collection: " + collectionName);
		}
		return executeEach(new FindCallback(mapper.getMappedObject(query, entity), fields), preparer,
				new ReadDbObjectCallback<T>(mongoConverter, targetClass), collectionName);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List using the template's converter.
	 * <p/>
	 * The query document is specified as a standard DBObject and so is the fields specification.
	 * 
	 * @param collectionName
	 *          name of the collection to retrieve the objects from
	 * @param query
	 *          the query document that specifies the criteria used to find a record
	 * @param fields
	 *          the document that specifies the fields to be returned
	 * @param targetClass
	 *          the parameterized type of the returned list.
	 * @param reader
	 *          the MongoReader to convert from DBObject to an object.
	 * @return the List of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> targetClass) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: " + query + " fields: " + fields + " for class: " + targetClass
					+ " in collection: " + collectionName);
		}
		MongoReader<? super T> readerToUse = this.mongoConverter;
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(targetClass);
		return executeEach(new FindCallback(mapper.getMappedObject(query, entity), fields), null,
				new ReadDbObjectCallback<T>(readerToUse, targetClass), collectionName);
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
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The first document that matches the query is returned and also removed from the collection in the database.
	 * <p/>
	 * The query document is specified as a standard DBObject and so is the fields specification.
	 * 
	 * @param collectionName
	 *          name of the collection to retrieve the objects from
	 * @param query
	 *          the query document that specifies the criteria used to find a record
	 * @param targetClass
	 *          the parameterized type of the returned list.
	 * @param reader
	 *          the MongoReader to convert from DBObject to an object.
	 * @return the List of converted objects.
	 */
	protected <T> T doFindAndRemove(String collectionName, DBObject query, DBObject fields, DBObject sort,
			Class<T> targetClass) {
		MongoReader<? super T> readerToUse = this.mongoConverter;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAndRemove using query: " + query + " fields: " + fields + " sort: " + sort + " for class: "
					+ targetClass + " in collection: " + collectionName);
		}
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(targetClass);
		return execute(new FindAndRemoveCallback(mapper.getMappedObject(query, entity), fields, sort),
				new ReadDbObjectCallback<T>(readerToUse, targetClass), collectionName);
	}

	protected Object getIdValue(Object object) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(object.getClass());
		MongoPersistentProperty idProp = entity.getIdProperty();

		if (idProp == null) {
			throw new MappingException("No id property found for object of type " + entity.getType().getName());
		}

		try {
			return MappingBeanHelper.getProperty(object, idProp, Object.class, true);
		} catch (IllegalAccessException e) {
			throw new MappingException(e.getMessage(), e);
		} catch (InvocationTargetException e) {
			throw new MappingException(e.getMessage(), e);
		}
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

		MongoPersistentProperty idProp = getIdPropertyFor(savedObject.getClass());

		if (idProp == null) {
			return;
		}

		try {
			MappingBeanHelper.setProperty(savedObject, idProp, id);
			return;
		} catch (IllegalAccessException e) {
			throw new MappingException(e.getMessage(), e);
		} catch (InvocationTargetException e) {
			throw new MappingException(e.getMessage(), e);
		}
	}

	private MongoPersistentEntity<?> getPersistentEntity(Class<?> type) {
		return type == null ? null : mappingContext.getPersistentEntity(type);
	}

	private MongoPersistentProperty getIdPropertyFor(Class<?> type) {
		return mappingContext.getPersistentEntity(type).getIdProperty();
	}

	private ObjectId convertIdValue(MongoConverter converter, Object value) {
		ObjectId newValue = null;
		try {
			if (value instanceof String && ObjectId.isValid((String) value)) {
				newValue = converter.convertObjectId(value);
			}
		} catch (ConversionFailedException iae) {
			LOGGER.warn("Unable to convert the String " + value + " to an ObjectId");
		}
		return newValue;
	}

	private <T> String determineEntityCollectionName(T obj) {
		if (null != obj) {
			return determineCollectionName(obj.getClass());
		}

		return null;
	}

	private String determineCollectionName(Class<?> clazz) {

		if (clazz == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity collection can't be determined for " + clazz);
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(clazz);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ clazz.getName());
		}
		return entity.getCollection();
	}

	/**
	 * Checks and handles any errors.
	 * <p/>
	 * TODO: current implementation logs errors - will be configurable to log warning, errors or throw exception in later
	 * versions
	 */
	private void handleAnyWriteResultErrors(WriteResult wr, DBObject query, String operation) {
		if (WriteResultChecking.NONE == this.writeResultChecking) {
			return;
		}
		String error = wr.getError();
		int n = wr.getN();
		if (error != null) {
			String message = "Execution of '" + operation + (query == null ? "" : "' using '" + query.toString() + "' query")
					+ " failed: " + error;
			if (WriteResultChecking.EXCEPTION == this.writeResultChecking) {
				throw new DataIntegrityViolationException(message);
			} else {
				LOGGER.error(message);
			}
		} else if (n == 0) {
			String message = "Execution of '" + operation + (query == null ? "" : "' using '" + query.toString() + "' query")
					+ " did not succeed: 0 documents updated";
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
		DB db = this.mongoDbFactory.getDb();
		converter.setMongo(db.getMongo());
		converter.setDefaultDatabase(db.getName());
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
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("findOne using query: " + query + " in db.collection: " + collection.getFullName());
				}
				return collection.findOne(query);
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("findOne using query: " + query + " fields: " + fields + " in db.collection: "
							+ collection.getFullName());
				}
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
	 * Simple {@link CollectionCallback} that takes a query {@link DBObject} plus an optional fields specification
	 * {@link DBObject} and executes that against the {@link DBCollection}.
	 * 
	 * @author Thomas Risberg
	 */
	private static class FindAndRemoveCallback implements CollectionCallback<DBObject> {

		private final DBObject query;

		private final DBObject fields;

		private final DBObject sort;

		public FindAndRemoveCallback(DBObject query, DBObject fields, DBObject sort) {
			this.query = query;
			this.fields = fields;
			this.sort = sort;
		}

		public DBObject doInCollection(DBCollection collection) throws MongoException, DataAccessException {
			return collection.findAndModify(query, fields, sort, true, null, false, false);
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

	public void setWriteResultChecking(WriteResultChecking resultChecking) {
		this.writeResultChecking = resultChecking;
	}

	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

}
