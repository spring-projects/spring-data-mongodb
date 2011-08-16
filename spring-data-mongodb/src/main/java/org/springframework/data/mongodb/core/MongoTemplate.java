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
package org.springframework.data.mongodb.core;

import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoReader;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.MongoMappingEventPublisher;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
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
public class MongoTemplate implements MongoOperations, ApplicationContextAware {

	private static final Log LOGGER = LogFactory.getLog(MongoTemplate.class);
	private static final String ID = "_id";
	private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
	@SuppressWarnings("serial")
	private static final List<String> ITERABLE_CLASSES = new ArrayList<String>() {
		{
			add(List.class.getName());
			add(Collection.class.getName());
			add(Iterator.class.getName());
		}
	};

	/*
	 * WriteConcern to be used for write operations if it has been specified.
	 * Otherwise we should not use a WriteConcern defaulting to the one set for
	 * the DB or Collection.
	 */
	private WriteConcern writeConcern = null;

	/*
	 * WriteResultChecking to be used for write operations if it has been
	 * specified. Otherwise we should not do any checking.
	 */
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;

	/*
	 * Flag used to indicate use of slaveOk() for any operations on collections.
	 */
	private boolean slaveOk = false;

	private final MongoConverter mongoConverter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoDbFactory mongoDbFactory;
	private final MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	private final QueryMapper mapper;

	private ApplicationEventPublisher eventPublisher;
	private MongoPersistentEntityIndexCreator indexCreator;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param mongo
	 * @param databaseName
	 */
	public MongoTemplate(Mongo mongo, String databaseName) {
		this(new SimpleMongoDbFactory(mongo, databaseName), null);
	}

	/**
	 * Constructor used for a template configuration with user credentials in the form of
	 * {@link org.springframework.data.authentication.UserCredentials}
	 * 
	 * @param mongo
	 * @param databaseName
	 * @param userCredentials
	 */
	public MongoTemplate(Mongo mongo, String databaseName, UserCredentials userCredentials) {
		this(new SimpleMongoDbFactory(mongo, databaseName, userCredentials));
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
	 * Constructor used for a basic template configuration.
	 * 
	 * @param mongoDbFactory
	 * @param mongoConverter
	 */
	public MongoTemplate(MongoDbFactory mongoDbFactory, MongoConverter mongoConverter) {
		Assert.notNull(mongoDbFactory);

		this.mongoDbFactory = mongoDbFactory;
		this.mongoConverter = mongoConverter == null ? getDefaultMongoConverter(mongoDbFactory) : mongoConverter;
		this.mapper = new QueryMapper(this.mongoConverter.getConversionService());

		// We always have a mapping context in the converter, whether it's a simple one or not
		mappingContext = this.mongoConverter.getMappingContext();
		// We create indexes based on mapping events
		if (null != mappingContext && mappingContext instanceof MongoMappingContext) {
			indexCreator = new MongoPersistentEntityIndexCreator((MongoMappingContext) mappingContext, mongoDbFactory);
			eventPublisher = new MongoMappingEventPublisher(indexCreator);
			if (mappingContext instanceof ApplicationEventPublisherAware) {
				((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
			}
		}

	}

	/**
	 * Configures the {@link WriteResultChecking} to be used with the template. Setting {@literal null} will reset the
	 * default of {@value #DEFAULT_WRITE_RESULT_CHECKING}.
	 * 
	 * @param resultChecking
	 */
	public void setWriteResultChecking(WriteResultChecking resultChecking) {
		this.writeResultChecking = resultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : resultChecking;
	}

	/**
	 * Configures the {@link WriteConcern} to be used with the template.
	 * 
	 * @param writeConcern
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/**
	 * TODO: document properly
	 * 
	 * @param slaveOk
	 */
	public void setSlaveOk(boolean slaveOk) {
		this.slaveOk = slaveOk;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		String[] beans = applicationContext.getBeanNamesForType(MongoPersistentEntityIndexCreator.class);
		if ((null == beans || beans.length == 0) && applicationContext instanceof ConfigurableApplicationContext) {
			((ConfigurableApplicationContext) applicationContext).addApplicationListener(indexCreator);
		}
		eventPublisher = applicationContext;
		if (mappingContext instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
		}
	}

	/**
	 * Returns the default {@link org.springframework.data.mongodb.core.core.convert.MongoConverter}.
	 * 
	 * @return
	 */
	public MongoConverter getConverter() {
		return this.mongoConverter;
	}

	public String getCollectionName(Class<?> entityClass) {
		return this.determineCollectionName(entityClass);
	}

	public CommandResult executeCommand(String jsonCommand) {
		return executeCommand((DBObject) JSON.parse(jsonCommand));
	}

	public CommandResult executeCommand(final DBObject command) {

		CommandResult result = execute(new DbCallback<CommandResult>() {
			public CommandResult doInDB(DB db) throws MongoException, DataAccessException {
				return db.command(command);
			}
		});

		String error = result.getErrorMessage();
		if (error != null) {
			// TODO: DATADOC-204 allow configuration of logging level / throw
			// throw new
			// InvalidDataAccessApiUsageException("Command execution of " +
			// command.toString() + " failed: " + error);
			LOGGER.warn("Command execution of " + command.toString() + " failed: " + error);
		}
		return result;
	}

	public <T> T execute(DbCallback<T> action) {

		Assert.notNull(action);

		try {
			DB db = this.getDb();
			return action.doInDB(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	public <T> T execute(Class<?> entityClass, CollectionCallback<T> callback) {
		return execute(determineCollectionName(entityClass), callback);
	}

	public <T> T execute(String collectionName, CollectionCallback<T> callback) {

		Assert.notNull(callback);

		try {
			DBCollection collection = getAndPrepareCollection(getDb(), collectionName);
			return callback.doInCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

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

	public <T> DBCollection createCollection(Class<T> entityClass) {
		return createCollection(determineCollectionName(entityClass));
	}

	public <T> DBCollection createCollection(Class<T> entityClass, CollectionOptions collectionOptions) {
		return createCollection(determineCollectionName(entityClass), collectionOptions);
	}

	public DBCollection createCollection(final String collectionName) {
		return doCreateCollection(collectionName, new BasicDBObject());
	}

	public DBCollection createCollection(final String collectionName, final CollectionOptions collectionOptions) {
		return doCreateCollection(collectionName, convertToDbObject(collectionOptions));
	}

	public DBCollection getCollection(final String collectionName) {
		return execute(new DbCallback<DBCollection>() {
			public DBCollection doInDB(DB db) throws MongoException, DataAccessException {
				return db.getCollection(collectionName);
			}
		});
	}

	public <T> boolean collectionExists(Class<T> entityClass) {
		return collectionExists(determineCollectionName(entityClass));
	}

	public boolean collectionExists(final String collectionName) {
		return execute(new DbCallback<Boolean>() {
			public Boolean doInDB(DB db) throws MongoException, DataAccessException {
				return db.collectionExists(collectionName);
			}
		});
	}

	public <T> void dropCollection(Class<T> entityClass) {
		dropCollection(determineCollectionName(entityClass));
	}

	public void dropCollection(String collectionName) {
		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.drop();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Dropped collection [" + collection.getFullName() + "]");
				}
				return null;
			}
		});
	}

	// Indexing methods

	public void ensureIndex(IndexDefinition indexDefinition, Class<?> entityClass) {
		ensureIndex(indexDefinition, determineCollectionName(entityClass));
	}

	public void ensureIndex(final IndexDefinition indexDefinition, String collectionName) {
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

	public <T> T findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {
		return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass);
	}

	// Find methods that take a Query to express the query and that return a List of objects.

	public <T> List<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> List<T> find(final Query query, Class<T> entityClass, String collectionName) {
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
		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass, cursorPreparer);
	}

	public <T> List<T> find(Query query, Class<T> entityClass, CursorPreparer preparer, String collectionName) {
		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass, preparer);
	}

	public <T> T findById(Object id, Class<T> entityClass) {
		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
		return findById(id, entityClass, persistentEntity.getCollection());
	}

	public <T> T findById(Object id, Class<T> entityClass, String collectionName) {
		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
		MongoPersistentProperty idProperty = persistentEntity.getIdProperty();
		String idKey = idProperty == null ? ID : idProperty.getName();
		return doFindOne(collectionName, new BasicDBObject(idKey, id), null, entityClass);
	}

	// Find methods that take a Query to express the query and that return a single object that is also removed from the
	// collection in the database.

	public <T> T findAndRemove(Query query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> T findAndRemove(Query query, Class<T> entityClass, String collectionName) {
		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(), query.getSortObject(),
				entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#insert(java.lang.Object)
	 */
	public void insert(Object objectToSave) {
		ensureNotIterable(objectToSave);
		insert(objectToSave, determineEntityCollectionName(objectToSave));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#insert(java.lang.Object, java.lang.String)
	 */
	public void insert(Object objectToSave, String collectionName) {
		ensureNotIterable(objectToSave);
		doInsert(collectionName, objectToSave, this.mongoConverter);
	}

	protected void ensureNotIterable(Object o) {
		if (null != o) {
			if (o.getClass().isArray() || ITERABLE_CLASSES.contains(o.getClass().getName())) {
				throw new IllegalArgumentException("Cannot use a collection here.");
			}
		}
	}

	/**
	 * Prepare the collection before any processing is done using it. This allows a convenient way to apply settings like
	 * slaveOk() etc. Can be overridden in sub-classes.
	 * 
	 * @param collection
	 */
	protected void prepareCollection(DBCollection collection) {
		if (this.slaveOk) {
			collection.slaveOk();
		}
	}

	/**
	 * Prepare the WriteConcern before any processing is done using it. This allows a convenient way to apply custom
	 * settings in sub-classes.
	 * 
	 * @param writeConcern any WriteConcern already configured or null
	 * @return The prepared WriteConcern or null
	 */
	protected WriteConcern prepareWriteConcern(WriteConcern writeConcern) {
		return writeConcern;
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

	public void insert(Collection<? extends Object> batchToSave, Class<?> entityClass) {
		doInsertBatch(determineCollectionName(entityClass), batchToSave, this.mongoConverter);
	}

	public void insert(Collection<? extends Object> batchToSave, String collectionName) {
		doInsertBatch(collectionName, batchToSave, this.mongoConverter);
	}

	public void insertAll(Collection<? extends Object> objectsToSave) {
		doInsertAll(objectsToSave, this.mongoConverter);
	}

	protected <T> void doInsertAll(Collection<? extends T> listToSave, MongoWriter<T> writer) {
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
			doInsertBatch(entry.getKey(), entry.getValue(), this.mongoConverter);
		}
	}

	protected <T> void doInsertBatch(String collectionName, Collection<? extends T> batchToSave, MongoWriter<T> writer) {

		Assert.notNull(writer);

		List<DBObject> dbObjectList = new ArrayList<DBObject>();
		for (T o : batchToSave) {
			BasicDBObject dbDoc = new BasicDBObject();

			maybeEmitEvent(new BeforeConvertEvent<T>(o));
			writer.write(o, dbDoc);

			maybeEmitEvent(new BeforeSaveEvent<T>(o, dbDoc));
			dbObjectList.add(dbDoc);
		}
		List<ObjectId> ids = insertDBObjectList(collectionName, dbObjectList);
		int i = 0;
		for (T obj : batchToSave) {
			if (i < ids.size()) {
				populateIdIfNecessary(obj, ids.get(i));
				maybeEmitEvent(new AfterSaveEvent<T>(obj, dbObjectList.get(i)));
			}
			i++;
		}
	}

	public void save(Object objectToSave) {
		save(objectToSave, determineEntityCollectionName(objectToSave));
	}

	public void save(Object objectToSave, String collectionName) {
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
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insert DBObject containing fields: " + dbDoc.keySet() + " in collection: " + collectionName);
		}
		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				WriteConcern writeConcernToUse = prepareWriteConcern(writeConcern);
				if (writeConcernToUse == null) {
					collection.insert(dbDoc);
				} else {
					collection.insert(dbDoc, writeConcernToUse);
				}
				return dbDoc.get(ID);
			}
		});
	}

	protected List<ObjectId> insertDBObjectList(String collectionName, final List<DBObject> dbDocList) {
		if (dbDocList.isEmpty()) {
			return Collections.emptyList();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insert list of DBObjects containing " + dbDocList.size() + " items");
		}
		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				WriteConcern writeConcernToUse = prepareWriteConcern(writeConcern);
				if (writeConcernToUse == null) {
					collection.insert(dbDocList);
				} else {
					collection.insert(dbDocList.toArray((DBObject[]) new BasicDBObject[dbDocList.size()]), writeConcernToUse);
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
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("save DBObject containing fields: " + dbDoc.keySet());
		}
		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				WriteConcern writeConcernToUse = prepareWriteConcern(writeConcern);
				if (writeConcernToUse == null) {
					collection.save(dbDoc);
				} else {
					collection.save(dbDoc, writeConcernToUse);
				}
				return dbDoc.get(ID);
			}
		});
	}

	public WriteResult updateFirst(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	public WriteResult updateFirst(final Query query, final Update update, final String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	public WriteResult updateMulti(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	public WriteResult updateMulti(final Query query, final Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	protected WriteResult doUpdate(final String collectionName, final Query query, final Update update,
			final Class<?> entityClass, final boolean upsert, final boolean multi) {

		return execute(collectionName, new CollectionCallback<WriteResult>() {
			public WriteResult doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				
				MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);
				
				DBObject queryObj = mapper.getMappedObject(query.getQueryObject(), entity);
				DBObject updateObj = update.getUpdateObject();

				for (String key : updateObj.keySet()) {
					updateObj.put(key, mongoConverter.convertToMongoType(updateObj.get(key)));
				}

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("calling update using query: " + queryObj + " and update: " + updateObj + " in collection: "
							+ collectionName);
				}

				WriteResult wr;
				WriteConcern writeConcernToUse = prepareWriteConcern(writeConcern);
				if (writeConcernToUse == null) {
					if (multi) {
						wr = collection.updateMulti(queryObj, updateObj);
					} else {
						wr = collection.update(queryObj, updateObj);
					}
				} else {
					wr = collection.update(queryObj, updateObj, upsert, multi, writeConcernToUse);
				}
				handleAnyWriteResultErrors(wr, queryObj, "update with '" + updateObj + "'");
				return wr;
			}
		});
	}

	public void remove(Object object) {

		if (object == null) {
			return;
		}

		remove(new Query(where(getIdPropertyName(object)).is(getIdValue(object))), object.getClass());
	}

	public <T> void remove(Query query, Class<T> entityClass) {
		Assert.notNull(query);
		doRemove(determineCollectionName(entityClass), query, entityClass);
	}

	protected <T> void doRemove(String collectionName, final Query query, Class<T> entityClass) {
		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to remove can't be null");
		}
		final DBObject queryObject = query.getQueryObject();
		final MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);
		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject dboq = mapper.getMappedObject(queryObject, entity);
				WriteResult wr = null;
				WriteConcern writeConcernToUse = prepareWriteConcern(writeConcern);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("remove using query: " + queryObject + " in collection: " + collection.getName());
				}
				if (writeConcernToUse == null) {
					wr = collection.remove(dboq);
				} else {
					wr = collection.remove(dboq, writeConcernToUse);
				}
				handleAnyWriteResultErrors(wr, dboq, "remove");
				return null;
			}
		});
	}

	public void remove(final Query query, String collectionName) {
		doRemove(collectionName, query, null);
	}

	public <T> List<T> findAll(Class<T> entityClass) {
		return executeFindMultiInternal(new FindCallback(null), null, new ReadDbObjectCallback<T>(mongoConverter,
				entityClass), determineCollectionName(entityClass));
	}

	public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(null), null, new ReadDbObjectCallback<T>(mongoConverter,
				entityClass), collectionName);
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
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Created collection [" + coll.getFullName() + "]");
				}
				return coll;
			}
		});
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter
	 * <p/>
	 * The query document is specified as a standard DBObject and so is the fields specification.
	 * 
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param fields the document that specifies the fields to be returned
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> T doFindOne(String collectionName, DBObject query, DBObject fields, Class<T> entityClass) {
		MongoReader<? super T> readerToUse = this.mongoConverter;
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		DBObject mappedQuery = mapper.getMappedObject(query, entity);

		return executeFindOneInternal(new FindOneCallback(mappedQuery, fields), new ReadDbObjectCallback<T>(readerToUse,
				entityClass), collectionName);
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
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param fields the document that specifies the fields to be returned
	 * @param entityClass the parameterized type of the returned list.
	 * @param preparer allows for customization of the DBCursor used when iterating over the result set, (apply limits,
	 *          skips and so on).
	 * @return the List of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> entityClass,
			CursorPreparer preparer) {
		return doFind(collectionName, query, fields, entityClass, preparer, new ReadDbObjectCallback<T>(mongoConverter,
				entityClass));
	}

	protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> entityClass,
			CursorPreparer preparer, DbObjectCallback<T> objectCallback) {
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: " + query + " fields: " + fields + " for class: " + entityClass
					+ " in collection: " + collectionName);
		}
		return executeFindMultiInternal(new FindCallback(mapper.getMappedObject(query, entity), fields), preparer,
				objectCallback, collectionName);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List using the template's converter.
	 * <p/>
	 * The query document is specified as a standard DBObject and so is the fields specification.
	 * 
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param fields the document that specifies the fields to be returned
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> entityClass) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: " + query + " fields: " + fields + " for class: " + entityClass
					+ " in collection: " + collectionName);
		}
		MongoReader<? super T> readerToUse = this.mongoConverter;
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		return executeFindMultiInternal(new FindCallback(mapper.getMappedObject(query, entity), fields), null,
				new ReadDbObjectCallback<T>(readerToUse, entityClass), collectionName);
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
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> T doFindAndRemove(String collectionName, DBObject query, DBObject fields, DBObject sort,
			Class<T> entityClass) {
		MongoReader<? super T> readerToUse = this.mongoConverter;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAndRemove using query: " + query + " fields: " + fields + " sort: " + sort + " for class: "
					+ entityClass + " in collection: " + collectionName);
		}
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		return executeFindOneInternal(new FindAndRemoveCallback(mapper.getMappedObject(query, entity), fields, sort),
				new ReadDbObjectCallback<T>(readerToUse, entityClass), collectionName);
	}

	protected Object getIdValue(Object object) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(object.getClass());
		MongoPersistentProperty idProp = entity.getIdProperty();

		if (idProp == null) {
			throw new MappingException("No id property found for object of type " + entity.getType().getName());
		}

		ConversionService service = mongoConverter.getConversionService();

		try {
			return BeanWrapper.create(object, service).getProperty(idProp, Object.class, true);
		} catch (IllegalAccessException e) {
			throw new MappingException(e.getMessage(), e);
		} catch (InvocationTargetException e) {
			throw new MappingException(e.getMessage(), e);
		}
	}

	protected String getIdPropertyName(Object object) {
		Assert.notNull(object);

		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(object.getClass());
		MongoPersistentProperty idProperty = persistentEntity.getIdProperty();

		return idProperty == null ? ID : idProperty.getName();
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
			BeanWrapper.create(savedObject, mongoConverter.getConversionService()).setProperty(idProp, id);
			return;
		} catch (IllegalAccessException e) {
			throw new MappingException(e.getMessage(), e);
		} catch (InvocationTargetException e) {
			throw new MappingException(e.getMessage(), e);
		}
	}

	private DBCollection getAndPrepareCollection(DB db, String collectionName) {
		try {
			DBCollection collection = db.getCollection(collectionName);
			prepareCollection(collection);
			return collection;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	/**
	 * Internal method using callbacks to do queries against the datastore that requires reading a single object from a
	 * collection of objects. It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ConnectionCallback} for a {@link DBObject}.</li>
	 * <li>Apply the given {@link DbObjectCallback} to each of the {@link DBObject}s to obtain the result.</li>
	 * <ol>
	 * 
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link DBObject} with
	 * @param objectCallback the {@link DbObjectCallback} to transform {@link DBObject}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> T executeFindOneInternal(CollectionCallback<DBObject> collectionCallback,
			DbObjectCallback<T> objectCallback, String collectionName) {

		try {
			T result = objectCallback.doWith(collectionCallback.doInCollection(getAndPrepareCollection(getDb(),
					collectionName)));
			return result;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	/**
	 * Internal method using callback to do queries against the datastore that requires reading a collection of objects.
	 * It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ConnectionCallback} for a {@link DBCursor}.</li>
	 * <li>Prepare that {@link DBCursor} with the given {@link CursorPreparer} (will be skipped if {@link CursorPreparer}
	 * is {@literal null}</li>
	 * <li>Iterate over the {@link DBCursor} and applies the given {@link DbObjectCallback} to each of the
	 * {@link DBObject}s collecting the actual result {@link List}.</li>
	 * <ol>
	 * 
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link DBCursor} with
	 * @param preparer the {@link CursorPreparer} to potentially modify the {@link DBCursor} before ireating over it
	 * @param objectCallback the {@link DbObjectCallback} to transform {@link DBObject}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> List<T> executeFindMultiInternal(CollectionCallback<DBCursor> collectionCallback,
			CursorPreparer preparer, DbObjectCallback<T> objectCallback, String collectionName) {

		try {
			DBCursor cursor = collectionCallback.doInCollection(getAndPrepareCollection(getDb(), collectionName));

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

	private MongoPersistentEntity<?> getPersistentEntity(Class<?> type) {
		return type == null ? null : mappingContext.getPersistentEntity(type);
	}

	private MongoPersistentProperty getIdPropertyFor(Class<?> type) {
		return mappingContext.getPersistentEntity(type).getIdProperty();
	}

	private <T> String determineEntityCollectionName(T obj) {
		if (null != obj) {
			return determineCollectionName(obj.getClass());
		}

		return null;
	}

	private String determineCollectionName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity collection can't be determined for " + entityClass);
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ entityClass.getName());
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

	private static final MongoConverter getDefaultMongoConverter(MongoDbFactory factory) {
		// ToDo: maybe add some additional configurations to this very basic one
		MappingMongoConverter converter = new MappingMongoConverter(factory, new MongoMappingContext());
		converter.afterPropertiesSet();
		return converter;
	}

	// Callback implementations

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
			Assert.notNull(reader);
			Assert.notNull(type);
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




}
