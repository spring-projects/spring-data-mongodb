/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under t
import org.springframework.util.ResourceUtils;

import org.springframework.data.convert.EntityReader;
he Apache License, Version 2.0 (the "License");
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

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
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.GeoResult;
import org.springframework.data.mongodb.core.geo.GeoResults;
import org.springframework.data.mongodb.core.geo.Metric;
import org.springframework.data.mongodb.core.index.MongoMappingEventPublisher;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jca.cci.core.ConnectionCallback;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

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

	private WriteConcernResolver writeConcernResolver = new DefaultWriteConcernResolver();

	/*
	 * WriteResultChecking to be used for write operations if it has been
	 * specified. Otherwise we should not do any checking.
	 */
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;

	/**
	 * Set the ReadPreference when operating on a collection. See {@link #prepareCollection(DBCollection)}
	 */
	private ReadPreference readPreference = null;

	private final MongoConverter mongoConverter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoDbFactory mongoDbFactory;
	private final MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	private final QueryMapper mapper;

	private ApplicationEventPublisher eventPublisher;
	private ResourceLoader resourceLoader;
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
		this.mapper = new QueryMapper(this.mongoConverter);

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
	 * Configures the {@link WriteConcernResolver} to be used with the template.
	 * 
	 * @param writeConcernResolver
	 */
	public void setWriteConcernResolver(WriteConcernResolver writeConcernResolver) {
		this.writeConcernResolver = writeConcernResolver;
	}

	/**
	 * Used by @{link {@link #prepareCollection(DBCollection)} to set the {@link ReadPreference} before any operations are
	 * performed.
	 * 
	 * @param readPreference
	 */
	public void setReadPreference(ReadPreference readPreference) {
		this.readPreference = readPreference;
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
		resourceLoader = applicationContext;
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

		logCommandExecutionError(command, result);
		return result;
	}

	public CommandResult executeCommand(final DBObject command, final int options) {

		CommandResult result = execute(new DbCallback<CommandResult>() {
			public CommandResult doInDB(DB db) throws MongoException, DataAccessException {
				return db.command(command, options);
			}
		});

		logCommandExecutionError(command, result);
		return result;
	}

	protected void logCommandExecutionError(final DBObject command, CommandResult result) {
		String error = result.getErrorMessage();
		if (error != null) {
			// TODO: DATADOC-204 allow configuration of logging level / throw
			// throw new
			// InvalidDataAccessApiUsageException("Command execution of " +
			// command.toString() + " failed: " + error);
			LOGGER.warn("Command execution of " + command.toString() + " failed: " + error);
		}
	}

	public void executeQuery(Query query, String collectionName, DocumentCallbackHandler dch) {
		executeQuery(query, collectionName, dch, new QueryCursorPreparer(query));
	}

	/**
	 * Execute a MongoDB query and iterate over the query results on a per-document basis with a
	 * {@link DocumentCallbackHandler} using the provided CursorPreparer.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification, must not be {@literal null}.
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param dch the handler that will extract results, one document at a time
	 * @param preparer allows for customization of the {@link DBCursor} used when iterating over the result set, (apply
	 *          limits, skips and so on).
	 */
	protected void executeQuery(Query query, String collectionName, DocumentCallbackHandler dch, CursorPreparer preparer) {

		Assert.notNull(query);

		DBObject queryObject = query.getQueryObject();
		DBObject fieldsObject = query.getFieldsObject();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: " + queryObject + " fields: " + fieldsObject + " in collection: "
					+ collectionName);
		}

		this.executeQueryInternal(new FindCallback(queryObject, fieldsObject), preparer, dch, collectionName);
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

	public IndexOperations indexOps(String collectionName) {
		return new DefaultIndexOperations(this, collectionName);
	}

	public IndexOperations indexOps(Class<?> entityClass) {
		return new DefaultIndexOperations(this, determineCollectionName(entityClass));
	}

	// Find methods that take a Query to express the query and that return a single object.

	public <T> T findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {
		if (query.getSortObject() == null) {
			return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass);
		} else {
			query.limit(1);
			List<T> results = find(query, entityClass, collectionName);
			return (results.isEmpty() ? null : results.get(0));
		}
	}

	// Find methods that take a Query to express the query and that return a List of objects.

	public <T> List<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> List<T> find(final Query query, Class<T> entityClass, String collectionName) {
		CursorPreparer cursorPreparer = query == null ? null : new QueryCursorPreparer(query);
		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass, cursorPreparer);
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

	public <T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass) {
		return geoNear(near, entityClass, determineCollectionName(entityClass));
	}

	@SuppressWarnings("unchecked")
	public <T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass, String collectionName) {

		if (near == null) {
			throw new InvalidDataAccessApiUsageException("NearQuery must not be null!");
		}

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException("Entity class must not be null!");
		}

		String collection = StringUtils.hasText(collectionName) ? collectionName : determineCollectionName(entityClass);
		BasicDBObject command = new BasicDBObject("geoNear", collection);
		command.putAll(near.toDBObject());

		CommandResult commandResult = executeCommand(command);
		List<Object> results = (List<Object>) commandResult.get("results");
		results = results == null ? Collections.emptyList() : results;

		DbObjectCallback<GeoResult<T>> callback = new GeoNearResultDbObjectCallback<T>(new ReadDbObjectCallback<T>(
				mongoConverter, entityClass), near.getMetric());
		List<GeoResult<T>> result = new ArrayList<GeoResult<T>>(results.size());

		for (Object element : results) {
			result.add(callback.doWith((DBObject) element));
		}

		DBObject stats = (DBObject) commandResult.get("stats");
		double averageDistance = stats == null ? 0 : (Double) stats.get("avgDistance");
		return new GeoResults<T>(result, new Distance(averageDistance, near.getMetric()));
	}

	public <T> T findAndModify(Query query, Update update, Class<T> entityClass) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, determineCollectionName(entityClass));
	}

	public <T> T findAndModify(Query query, Update update, Class<T> entityClass, String collectionName) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, collectionName);
	}

	public <T> T findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass) {
		return findAndModify(query, update, options, entityClass, determineCollectionName(entityClass));
	}

	public <T> T findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass,
			String collectionName) {
		return doFindAndModify(collectionName, query.getQueryObject(), query.getFieldsObject(), query.getSortObject(),
				entityClass, update, options);
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

	public long count(Query query, Class<?> entityClass) {
		Assert.notNull(entityClass);
		return count(query, entityClass, determineCollectionName(entityClass));
	}

	public long count(final Query query, String collectionName) {
		return count(query, null, collectionName);
	}

	private long count(Query query, Class<?> entityClass, String collectionName) {

		Assert.hasText(collectionName);
		final DBObject dbObject = query == null ? null : mapper.getMappedObject(query.getQueryObject(),
				entityClass == null ? null : mappingContext.getPersistentEntity(entityClass));

		return execute(collectionName, new CollectionCallback<Long>() {
			public Long doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				return collection.count(dbObject);
			}
		});
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
		if (this.readPreference != null) {
			collection.setReadPreference(readPreference);
		}
	}

	/**
	 * Prepare the WriteConcern before any processing is done using it. This allows a convenient way to apply custom
	 * settings in sub-classes.
	 * 
	 * @param writeConcern any WriteConcern already configured or null
	 * @return The prepared WriteConcern or null
	 */
	protected WriteConcern prepareWriteConcern(MongoAction mongoAction) {
		return writeConcernResolver.resolve(mongoAction);
	}

	protected <T> void doInsert(String collectionName, T objectToSave, MongoWriter<T> writer) {

		assertUpdateableIdIfNotSet(objectToSave);

		BasicDBObject dbDoc = new BasicDBObject();

		maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave));
		writer.write(objectToSave, dbDoc);

		maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc));
		Object id = insertDBObject(collectionName, dbDoc, objectToSave.getClass());

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

		assertUpdateableIdIfNotSet(objectToSave);

		BasicDBObject dbDoc = new BasicDBObject();

		maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave));
		writer.write(objectToSave, dbDoc);

		maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc));
		Object id = saveDBObject(collectionName, dbDoc, objectToSave.getClass());

		populateIdIfNecessary(objectToSave, id);
		maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc));
	}

	protected Object insertDBObject(final String collectionName, final DBObject dbDoc, final Class<?> entityClass) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insert DBObject containing fields: " + dbDoc.keySet() + " in collection: " + collectionName);
		}
		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT, collectionName,
						entityClass, dbDoc, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				if (writeConcernToUse == null) {
					collection.insert(dbDoc);
				} else {
					collection.insert(dbDoc, writeConcernToUse);
				}
				return dbDoc.get(ID);
			}
		});
	}

	protected List<ObjectId> insertDBObjectList(final String collectionName, final List<DBObject> dbDocList) {
		if (dbDocList.isEmpty()) {
			return Collections.emptyList();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insert list of DBObjects containing " + dbDocList.size() + " items");
		}
		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT_LIST, collectionName, null,
						null, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
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

	protected Object saveDBObject(final String collectionName, final DBObject dbDoc, final Class<?> entityClass) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("save DBObject containing fields: " + dbDoc.keySet());
		}
		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.SAVE, collectionName, entityClass,
						dbDoc, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				if (writeConcernToUse == null) {
					collection.save(dbDoc);
				} else {
					collection.save(dbDoc, writeConcernToUse);
				}
				return dbDoc.get(ID);
			}
		});
	}

	public WriteResult upsert(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, true, false);
	}

	public WriteResult upsert(Query query, Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
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

				DBObject queryObj = query == null ? new BasicDBObject()
						: mapper.getMappedObject(query.getQueryObject(), entity);
				DBObject updateObj = update.getUpdateObject();

				for (String key : updateObj.keySet()) {
					updateObj.put(key, mongoConverter.convertToMongoType(updateObj.get(key)));
				}

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("calling update using query: " + queryObj + " and update: " + updateObj + " in collection: "
							+ collectionName);
				}

				WriteResult wr;
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName,
						entityClass, updateObj, queryObj);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				if (writeConcernToUse == null) {
					wr = collection.update(queryObj, updateObj, upsert, multi);
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

		remove(getIdQueryFor(object), object.getClass());
	}

	public void remove(Object object, String collection) {

		Assert.hasText(collection);

		if (object == null) {
			return;
		}

		remove(getIdQueryFor(object), collection);
	}

	/**
	 * Returns a {@link Query} for the given entity by its id.
	 * 
	 * @param object must not be {@literal null}.
	 * @return
	 */
	private Query getIdQueryFor(Object object) {

		Assert.notNull(object);

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(object.getClass());
		MongoPersistentProperty idProp = entity.getIdProperty();

		if (idProp == null) {
			throw new MappingException("No id property found for object of type " + entity.getType().getName());
		}

		ConversionService service = mongoConverter.getConversionService();
		Object idProperty = null;

		idProperty = BeanWrapper.create(object, service).getProperty(idProp, Object.class, true);
		return new Query(where(idProp.getFieldName()).is(idProperty));
	}

	private void assertUpdateableIdIfNotSet(Object entity) {

		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entity.getClass());
		MongoPersistentProperty idProperty = persistentEntity.getIdProperty();

		if (idProperty == null) {
			return;
		}

		ConversionService service = mongoConverter.getConversionService();
		Object idValue = BeanWrapper.create(entity, service).getProperty(idProperty, Object.class, true);

		if (idValue == null && !MongoSimpleTypes.AUTOGENERATED_ID_TYPES.contains(idProperty.getType())) {
			throw new InvalidDataAccessApiUsageException(String.format(
					"Cannot autogenerate id of type %s for entity of type %s!", idProperty.getType().getName(), entity.getClass()
							.getName()));
		}
	}

	public <T> void remove(Query query, Class<T> entityClass) {
		Assert.notNull(query);
		doRemove(determineCollectionName(entityClass), query, entityClass);
	}

	protected <T> void doRemove(final String collectionName, final Query query, final Class<T> entityClass) {
		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to remove can't be null");
		}
		final DBObject queryObject = query.getQueryObject();
		final MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);
		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject dboq = mapper.getMappedObject(queryObject, entity);
				WriteResult wr = null;
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName,
						entityClass, null, queryObject);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("remove using query: " + dboq + " in collection: " + collection.getName());
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

	public <T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			Class<T> entityClass) {
		return mapReduce(null, inputCollectionName, mapFunction, reduceFunction, new MapReduceOptions().outputTypeInline(),
				entityClass);
	}

	public <T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			MapReduceOptions mapReduceOptions, Class<T> entityClass) {
		return mapReduce(null, inputCollectionName, mapFunction, reduceFunction, mapReduceOptions, entityClass);
	}

	public <T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction,
			String reduceFunction, Class<T> entityClass) {
		return mapReduce(query, inputCollectionName, mapFunction, reduceFunction,
				new MapReduceOptions().outputTypeInline(), entityClass);
	}

	public <T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction,
			String reduceFunction, MapReduceOptions mapReduceOptions, Class<T> entityClass) {
		String mapFunc = replaceWithResourceIfNecessary(mapFunction);
		String reduceFunc = replaceWithResourceIfNecessary(reduceFunction);
		DBCollection inputCollection = getCollection(inputCollectionName);
		MapReduceCommand command = new MapReduceCommand(inputCollection, mapFunc, reduceFunc,
				mapReduceOptions.getOutputCollection(), mapReduceOptions.getOutputType(), null);

		DBObject commandObject = copyQuery(query, copyMapReduceOptions(mapReduceOptions, command));

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing MapReduce on collection [" + command.getInput() + "], mapFunction [" + mapFunc
					+ "], reduceFunction [" + reduceFunc + "]");
		}
		CommandResult commandResult = null;
		try {
			if (command.getOutputType() == MapReduceCommand.OutputType.INLINE) {
				commandResult = executeCommand(commandObject, getDb().getOptions());
			} else {
				commandResult = executeCommand(commandObject);
			}
			commandResult.throwOnError();
		} catch (RuntimeException ex) {
			this.potentiallyConvertRuntimeException(ex);
		}
		String error = commandResult.getErrorMessage();
		if (error != null) {
			throw new InvalidDataAccessApiUsageException("Command execution failed:  Error [" + error + "], Command = "
					+ commandObject);
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("MapReduce command result = [" + commandResult + "]");
		}
		MapReduceOutput mapReduceOutput = new MapReduceOutput(inputCollection, commandObject, commandResult);
		List<T> mappedResults = new ArrayList<T>();
		DbObjectCallback<T> callback = new ReadDbObjectCallback<T>(mongoConverter, entityClass);
		for (DBObject dbObject : mapReduceOutput.results()) {
			mappedResults.add(callback.doWith(dbObject));
		}

		MapReduceResults<T> mapReduceResult = new MapReduceResults<T>(mappedResults, commandResult);
		return mapReduceResult;
	}

	public <T> GroupByResults<T> group(String inputCollectionName, GroupBy groupBy, Class<T> entityClass) {
		return group(null, inputCollectionName, groupBy, entityClass);
	}

	public <T> GroupByResults<T> group(Criteria criteria, String inputCollectionName, GroupBy groupBy,
			Class<T> entityClass) {

		DBObject dbo = groupBy.getGroupByObject();
		dbo.put("ns", inputCollectionName);

		if (criteria == null) {
			dbo.put("cond", null);
		} else {
			dbo.put("cond", criteria.getCriteriaObject());
		}
		// If initial document was a JavaScript string, potentially loaded by Spring's Resource abstraction, load it and
		// convert to DBObject

		if (dbo.containsField("initial")) {
			Object initialObj = dbo.get("initial");
			if (initialObj instanceof String) {
				String initialAsString = replaceWithResourceIfNecessary((String) initialObj);
				dbo.put("initial", JSON.parse(initialAsString));
			}
		}

		if (dbo.containsField("$reduce")) {
			dbo.put("$reduce", replaceWithResourceIfNecessary(dbo.get("$reduce").toString()));
		}
		if (dbo.containsField("$keyf")) {
			dbo.put("$keyf", replaceWithResourceIfNecessary(dbo.get("$keyf").toString()));
		}
		if (dbo.containsField("finalize")) {
			dbo.put("finalize", replaceWithResourceIfNecessary(dbo.get("finalize").toString()));
		}

		DBObject commandObject = new BasicDBObject("group", dbo);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing Group with DBObject [" + commandObject.toString() + "]");
		}
		CommandResult commandResult = null;
		try {
			commandResult = executeCommand(commandObject, getDb().getOptions());
			commandResult.throwOnError();
		} catch (RuntimeException ex) {
			this.potentiallyConvertRuntimeException(ex);
		}
		String error = commandResult.getErrorMessage();
		if (error != null) {
			throw new InvalidDataAccessApiUsageException("Command execution failed:  Error [" + error + "], Command = "
					+ commandObject);
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Group command result = [" + commandResult + "]");
		}

		@SuppressWarnings("unchecked")
		Iterable<DBObject> resultSet = (Iterable<DBObject>) commandResult.get("retval");

		List<T> mappedResults = new ArrayList<T>();
		DbObjectCallback<T> callback = new ReadDbObjectCallback<T>(mongoConverter, entityClass);
		for (DBObject dbObject : resultSet) {
			mappedResults.add(callback.doWith(dbObject));
		}
		GroupByResults<T> groupByResult = new GroupByResults<T>(mappedResults, commandResult);
		return groupByResult;

	}

	protected String replaceWithResourceIfNecessary(String function) {

		String func = function;

		if (this.resourceLoader != null && ResourceUtils.isUrl(function)) {

			Resource functionResource = resourceLoader.getResource(func);

			if (!functionResource.exists()) {
				throw new InvalidDataAccessApiUsageException(String.format("Resource %s not found!", function));
			}

			try {
				return new Scanner(functionResource.getInputStream()).useDelimiter("\\A").next();
			} catch (IOException e) {
				throw new InvalidDataAccessApiUsageException(String.format("Cannot read map-reduce file %s!", function), e);
			}
		}

		return func;
	}

	private DBObject copyQuery(Query query, DBObject copyMapReduceOptions) {
		if (query != null) {
			if (query.getSkip() != 0 || query.getFieldsObject() != null) {
				throw new InvalidDataAccessApiUsageException(
						"Can not use skip or field specification with map reduce operations");
			}
			if (query.getQueryObject() != null) {
				copyMapReduceOptions.put("query", query.getQueryObject());
			}
			if (query.getLimit() > 0) {
				copyMapReduceOptions.put("limit", query.getLimit());
			}
			if (query.getSortObject() != null) {
				copyMapReduceOptions.put("sort", query.getSortObject());
			}
		}
		return copyMapReduceOptions;
	}

	private DBObject copyMapReduceOptions(MapReduceOptions mapReduceOptions, MapReduceCommand command) {
		if (mapReduceOptions.getJavaScriptMode() != null) {
			command.addExtraOption("jsMode", true);
		}
		if (!mapReduceOptions.getExtraOptions().isEmpty()) {
			for (Map.Entry<String, Object> entry : mapReduceOptions.getExtraOptions().entrySet()) {
				command.addExtraOption(entry.getKey(), entry.getValue());
			}
		}
		if (mapReduceOptions.getFinalizeFunction() != null) {
			command.setFinalize(this.replaceWithResourceIfNecessary(mapReduceOptions.getFinalizeFunction()));
		}
		if (mapReduceOptions.getOutputDatabase() != null) {
			command.setOutputDB(mapReduceOptions.getOutputDatabase());
		}
		if (!mapReduceOptions.getScopeVariables().isEmpty()) {
			command.setScope(mapReduceOptions.getScopeVariables());
		}

		DBObject commandObject = command.toDBObject();
		DBObject outObject = (DBObject) commandObject.get("out");

		if (mapReduceOptions.getOutputSharded() != null) {
			outObject.put("sharded", mapReduceOptions.getOutputSharded());
		}
		return commandObject;
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
		EntityReader<? super T, DBObject> readerToUse = this.mongoConverter;
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

	protected <S, T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<S> entityClass,
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
		EntityReader<? super T, DBObject> readerToUse = this.mongoConverter;
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
		EntityReader<? super T, DBObject> readerToUse = this.mongoConverter;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAndRemove using query: " + query + " fields: " + fields + " sort: " + sort + " for class: "
					+ entityClass + " in collection: " + collectionName);
		}
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		return executeFindOneInternal(new FindAndRemoveCallback(mapper.getMappedObject(query, entity), fields, sort),
				new ReadDbObjectCallback<T>(readerToUse, entityClass), collectionName);
	}

	protected <T> T doFindAndModify(String collectionName, DBObject query, DBObject fields, DBObject sort,
			Class<T> entityClass, Update update, FindAndModifyOptions options) {

		EntityReader<? super T, DBObject> readerToUse = this.mongoConverter;

		if (options == null) {
			options = new FindAndModifyOptions();
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		DBObject updateObj = update.getUpdateObject();
		for (String key : updateObj.keySet()) {
			updateObj.put(key, mongoConverter.convertToMongoType(updateObj.get(key)));
		}

		DBObject mappedQuery = mapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAndModify using query: " + mappedQuery + " fields: " + fields + " sort: " + sort
					+ " for class: " + entityClass + " and update: " + updateObj + " in collection: " + collectionName);
		}

		return executeFindOneInternal(new FindAndModifyCallback(mappedQuery, fields, sort, updateObj, options),
				new ReadDbObjectCallback<T>(readerToUse, entityClass), collectionName);
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

		ConversionService conversionService = mongoConverter.getConversionService();
		BeanWrapper<PersistentEntity<Object, ?>, Object> wrapper = BeanWrapper.create(savedObject, conversionService);

		try {

			Object idValue = wrapper.getProperty(idProp);

			if (idValue != null) {
				return;
			}

			wrapper.setProperty(idProp, id);

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

	private void executeQueryInternal(CollectionCallback<DBCursor> collectionCallback, CursorPreparer preparer,
			DocumentCallbackHandler callbackHandler, String collectionName) {

		try {
			DBCursor cursor = collectionCallback.doInCollection(getAndPrepareCollection(getDb(), collectionName));

			if (preparer != null) {
				cursor = preparer.prepare(cursor);
			}

			for (DBObject dbobject : cursor) {
				callbackHandler.processDocument(dbobject);
			}
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

	String determineCollectionName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity collection can't be determined!");
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
	 * Current implementation logs errors. Future version may make this configurable to log warning, errors or throw
	 * exception.
	 */
	protected void handleAnyWriteResultErrors(WriteResult wr, DBObject query, String operation) {

		if (WriteResultChecking.NONE == this.writeResultChecking) {
			return;
		}

		String error = wr.getError();

		if (error != null) {

			String message = String.format("Execution of %s%s failed: %s", operation, query == null ? "" : "' using '"
					+ query.toString() + "' query", error);

			if (WriteResultChecking.EXCEPTION == this.writeResultChecking) {
				throw new DataIntegrityViolationException(message);
			} else {
				LOGGER.error(message);
				return;
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

	private static class FindAndModifyCallback implements CollectionCallback<DBObject> {

		private final DBObject query;
		private final DBObject fields;
		private final DBObject sort;
		private final DBObject update;
		private final FindAndModifyOptions options;

		public FindAndModifyCallback(DBObject query, DBObject fields, DBObject sort, DBObject update,
				FindAndModifyOptions options) {
			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.options = options;
		}

		public DBObject doInCollection(DBCollection collection) throws MongoException, DataAccessException {
			return collection.findAndModify(query, fields, sort, options.isRemove(), update, options.isReturnNew(),
					options.isUpsert());
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

		private final EntityReader<? super T, DBObject> reader;
		private final Class<T> type;

		public ReadDbObjectCallback(EntityReader<? super T, DBObject> reader, Class<T> type) {
			Assert.notNull(reader);
			Assert.notNull(type);
			this.reader = reader;
			this.type = type;
		}

		public T doWith(DBObject object) {
			if (null != object) {
				maybeEmitEvent(new AfterLoadEvent<T>(object, type));
			}
			T source = reader.read(type, object);
			if (null != source) {
				maybeEmitEvent(new AfterConvertEvent<T>(object, source));
			}
			return source;
		}
	}

	private class DefaultWriteConcernResolver implements WriteConcernResolver {

		public WriteConcern resolve(MongoAction action) {
			return action.getDefaultWriteConcern();
		}

	}

	class QueryCursorPreparer implements CursorPreparer {

		private final Query query;

		public QueryCursorPreparer(Query query) {
			this.query = query;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.CursorPreparer#prepare(com.mongodb.DBCursor)
		 */
		public DBCursor prepare(DBCursor cursor) {

			if (query == null) {
				return cursor;
			}

			if (query.getSkip() <= 0 && query.getLimit() <= 0 && query.getSortObject() == null
					&& !StringUtils.hasText(query.getHint())) {
				return cursor;
			}

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
				if (StringUtils.hasText(query.getHint())) {
					cursorToUse = cursorToUse.hint(query.getHint());
				}
			} catch (RuntimeException e) {
				throw potentiallyConvertRuntimeException(e);
			}

			return cursorToUse;
		}
	}

	/**
	 * {@link DbObjectCallback} that assumes a {@link GeoResult} to be created, delegates actual content unmarshalling to
	 * a delegate and creates a {@link GeoResult} from the result.
	 * 
	 * @author Oliver Gierke
	 */
	static class GeoNearResultDbObjectCallback<T> implements DbObjectCallback<GeoResult<T>> {

		private final DbObjectCallback<T> delegate;
		private final Metric metric;

		/**
		 * Creates a new {@link GeoNearResultDbObjectCallback} using the given {@link DbObjectCallback} delegate for
		 * {@link GeoResult} content unmarshalling.
		 * 
		 * @param delegate
		 */
		public GeoNearResultDbObjectCallback(DbObjectCallback<T> delegate, Metric metric) {
			Assert.notNull(delegate);
			this.delegate = delegate;
			this.metric = metric;
		}

		public GeoResult<T> doWith(DBObject object) {

			double distance = ((Double) object.get("dis")).doubleValue();
			DBObject content = (DBObject) object.get("obj");

			T doWith = delegate.doWith(content);

			return new GeoResult<T>(doWith, new Distance(distance, metric));
		}
	}

}
