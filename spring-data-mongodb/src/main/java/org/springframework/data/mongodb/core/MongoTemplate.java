/*
 * Copyright 2010-2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.SerializationUtils.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.annotation.Id;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.DefaultBulkOperations.BulkOperationContext;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.Fields;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.index.MongoMappingEventPublisher;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.data.util.CloseableIterator;
import org.springframework.jca.cci.core.ConnectionCallback;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.CommandResult;
import com.mongodb.Cursor;
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
import com.mongodb.util.JSONParseException;

/**
 * Primary implementation of {@link MongoOperations}.
 *
 * @author Thomas Risberg
 * @author Graeme Rocher
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Amol Nayak
 * @author Patryk Wasik
 * @author Tobias Trelle
 * @author Sebastian Herold
 * @author Thomas Darimont
 * @author Chuong Ngo
 * @author Christoph Strobl
 * @author Doménique Tilleuil
 * @author Niko Schmuck
 * @author Mark Paluch
 * @author Laszlo Csontos
 * @author Borislav Rangelov
 */
@SuppressWarnings("deprecation")
public class MongoTemplate implements MongoOperations, ApplicationContextAware {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoTemplate.class);
	private static final String ID_FIELD = "_id";
	private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
	private static final Collection<String> ITERABLE_CLASSES;

	static {

		Set<String> iterableClasses = new HashSet<String>();
		iterableClasses.add(List.class.getName());
		iterableClasses.add(Collection.class.getName());
		iterableClasses.add(Iterator.class.getName());

		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
	}

	private final MongoConverter mongoConverter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoDbFactory mongoDbFactory;
	private final PersistenceExceptionTranslator exceptionTranslator;
	private final QueryMapper queryMapper;
	private final UpdateMapper updateMapper;

	private WriteConcern writeConcern;
	private WriteConcernResolver writeConcernResolver = DefaultWriteConcernResolver.INSTANCE;
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;
	private ReadPreference readPreference;
	private ApplicationEventPublisher eventPublisher;
	private ResourceLoader resourceLoader;
	private MongoPersistentEntityIndexCreator indexCreator;

	/**
	 * Constructor used for a basic template configuration
	 *
	 * @param mongo must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 */
	public MongoTemplate(Mongo mongo, String databaseName) {
		this(new SimpleMongoDbFactory(mongo, databaseName), null);
	}

	/**
	 * Constructor used for a template configuration with user credentials in the form of
	 * {@link org.springframework.data.authentication.UserCredentials}
	 *
	 * @param mongo must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 * @param userCredentials
	 */
	public MongoTemplate(Mongo mongo, String databaseName, UserCredentials userCredentials) {
		this(new SimpleMongoDbFactory(mongo, databaseName, userCredentials));
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public MongoTemplate(MongoDbFactory mongoDbFactory) {
		this(mongoDbFactory, null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param mongoConverter
	 */
	public MongoTemplate(MongoDbFactory mongoDbFactory, MongoConverter mongoConverter) {

		Assert.notNull(mongoDbFactory, "MongoDbFactory must not be null!");

		this.mongoDbFactory = mongoDbFactory;
		this.exceptionTranslator = mongoDbFactory.getExceptionTranslator();
		this.mongoConverter = mongoConverter == null ? getDefaultMongoConverter(mongoDbFactory) : mongoConverter;
		this.queryMapper = new QueryMapper(this.mongoConverter);
		this.updateMapper = new UpdateMapper(this.mongoConverter);

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
	 * Configures the {@link WriteConcern} to be used with the template. If none is configured the {@link WriteConcern}
	 * configured on the {@link MongoDbFactory} will apply. If you configured a {@link Mongo} instance no
	 * {@link WriteConcern} will be used.
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		prepareIndexCreator(applicationContext);

		eventPublisher = applicationContext;
		if (mappingContext instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
		}
		resourceLoader = applicationContext;
	}

	/**
	 * Inspects the given {@link ApplicationContext} for {@link MongoPersistentEntityIndexCreator} and those in turn if
	 * they were registered for the current {@link MappingContext}. If no creator for the current {@link MappingContext}
	 * can be found we manually add the internally created one as {@link ApplicationListener} to make sure indexes get
	 * created appropriately for entity types persisted through this {@link MongoTemplate} instance.
	 *
	 * @param context must not be {@literal null}.
	 */
	private void prepareIndexCreator(ApplicationContext context) {

		String[] indexCreators = context.getBeanNamesForType(MongoPersistentEntityIndexCreator.class);

		for (String creator : indexCreators) {
			MongoPersistentEntityIndexCreator creatorBean = context.getBean(creator, MongoPersistentEntityIndexCreator.class);
			if (creatorBean.isIndexCreatorFor(mappingContext)) {
				return;
			}
		}

		if (context instanceof ConfigurableApplicationContext) {
			((ConfigurableApplicationContext) context).addApplicationListener(indexCreator);
		}
	}

	/**
	 * Returns the default {@link org.springframework.data.mongodb.core.convert.MongoConverter}.
	 *
	 * @return
	 */
	public MongoConverter getConverter() {
		return this.mongoConverter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeAsStream(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> CloseableIterator<T> stream(final Query query, final Class<T> entityType) {

		return stream(query, entityType, determineCollectionName(entityType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#stream(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> CloseableIterator<T> stream(final Query query, final Class<T> entityType, final String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityType, "Entity type must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		return execute(collectionName, new CollectionCallback<CloseableIterator<T>>() {

			@Override
			public CloseableIterator<T> doInCollection(DBCollection collection) throws MongoException, DataAccessException {

				MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityType);

				DBObject mappedFields = queryMapper.getMappedFields(query.getFieldsObject(), persistentEntity);
				DBObject mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), persistentEntity);

				DBCursor cursor = collection.find(mappedQuery, mappedFields);
				QueryCursorPreparer cursorPreparer = new QueryCursorPreparer(query, entityType);

				ReadDbObjectCallback<T> readCallback = new ReadDbObjectCallback<T>(mongoConverter, entityType, collectionName);

				return new CloseableIterableCursorAdapter<T>(cursorPreparer.prepare(cursor), exceptionTranslator, readCallback);
			}
		});
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeCommand(com.mongodb.DBObject, int)
	 */
	@Deprecated
	public CommandResult executeCommand(final DBObject command, final int options) {
		return executeCommand(command,
				(options & Bytes.QUERYOPTION_SLAVEOK) != 0 ? ReadPreference.secondaryPreferred() : ReadPreference.primary());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeCommand(com.mongodb.DBObject, com.mongodb.ReadPreference)
	 */
	public CommandResult executeCommand(final DBObject command, final ReadPreference readPreference) {

		Assert.notNull(command, "Command must not be null!");

		CommandResult result = execute(new DbCallback<CommandResult>() {
			public CommandResult doInDB(DB db) throws MongoException, DataAccessException {
				return readPreference != null ? db.command(command, readPreference) : db.command(command);
			}
		});

		logCommandExecutionError(command, result);

		return result;
	}

	protected void logCommandExecutionError(final DBObject command, CommandResult result) {

		String error = result.getErrorMessage();

		if (error != null) {
			LOGGER.warn("Command execution of {} failed: {}", command.toString(), error);
		}
	}

	public void executeQuery(Query query, String collectionName, DocumentCallbackHandler dch) {
		executeQuery(query, collectionName, dch, new QueryCursorPreparer(query, null));
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
	protected void executeQuery(Query query, String collectionName, DocumentCallbackHandler dch,
			CursorPreparer preparer) {

		Assert.notNull(query, "Query must not be null!");

		DBObject queryObject = queryMapper.getMappedObject(query.getQueryObject(), null);
		DBObject sortObject = query.getSortObject();
		DBObject fieldsObject = query.getFieldsObject();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing query: {} sort: {} fields: {} in collection: {}", serializeToJsonSafely(queryObject),
					sortObject, fieldsObject, collectionName);
		}

		this.executeQueryInternal(new FindCallback(queryObject, fieldsObject), preparer, dch, collectionName);
	}

	public <T> T execute(DbCallback<T> action) {

		Assert.notNull(action, "DbCallbackmust not be null!");

		try {
			DB db = this.getDb();
			return action.doInDB(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	public <T> T execute(Class<?> entityClass, CollectionCallback<T> callback) {
		return execute(determineCollectionName(entityClass), callback);
	}

	public <T> T execute(String collectionName, CollectionCallback<T> callback) {

		Assert.notNull(callback, "CollectionCallback must not be null!");

		try {
			DBCollection collection = getAndPrepareCollection(getDb(), collectionName);
			return callback.doInCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeInSession(org.springframework.data.mongodb.core.DbCallback)
	 */
	@Deprecated
	public <T> T executeInSession(final DbCallback<T> action) {

		return execute(new DbCallback<T>() {
			public T doInDB(DB db) throws MongoException, DataAccessException {
				try {
					ReflectiveDbInvoker.requestStart(db);
					return action.doInDB(db);
				} finally {
					ReflectiveDbInvoker.requestDone(db);
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
					LOGGER.debug("Dropped collection [{}]", collection.getFullName());
				}
				return null;
			}
		});
	}

	public IndexOperations indexOps(String collectionName) {
		return new DefaultIndexOperations(this, collectionName);
	}

	public IndexOperations indexOps(Class<?> entityClass) {
		return new DefaultIndexOperations(this, determineCollectionName(entityClass), entityClass);
	}

	public BulkOperations bulkOps(BulkMode bulkMode, String collectionName) {
		return bulkOps(bulkMode, null, collectionName);
	}

	public BulkOperations bulkOps(BulkMode bulkMode, Class<?> entityClass) {
		return bulkOps(bulkMode, entityClass, determineCollectionName(entityClass));
	}

	public BulkOperations bulkOps(BulkMode mode, Class<?> entityType, String collectionName) {

		Assert.notNull(mode, "BulkMode must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		DefaultBulkOperations operations = new DefaultBulkOperations(this, collectionName,
				new BulkOperationContext(mode, getPersistentEntity(entityType), queryMapper, updateMapper));

		operations.setExceptionTranslator(exceptionTranslator);
		operations.setDefaultWriteConcern(writeConcern);

		return operations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#scriptOps()
	 */
	@Override
	public ScriptOperations scriptOps() {
		return new DefaultScriptOperations(this);
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
			return results.isEmpty() ? null : results.get(0);
		}
	}

	public boolean exists(Query query, Class<?> entityClass) {
		return exists(query, entityClass, determineCollectionName(entityClass));
	}

	public boolean exists(Query query, String collectionName) {
		return exists(query, null, collectionName);
	}

	public boolean exists(Query query, Class<?> entityClass, String collectionName) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to exist can't be null");
		}

		DBObject mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), getPersistentEntity(entityClass));
		return execute(collectionName, new FindCallback(mappedQuery)).hasNext();
	}

	// Find methods that take a Query to express the query and that return a List of objects.

	public <T> List<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> List<T> find(final Query query, Class<T> entityClass, String collectionName) {

		if (query == null) {
			return findAll(entityClass, collectionName);
		}

		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
				new QueryCursorPreparer(query, entityClass));
	}

	public <T> T findById(Object id, Class<T> entityClass) {
		return findById(id, entityClass, determineCollectionName(entityClass));
	}

	public <T> T findById(Object id, Class<T> entityClass, String collectionName) {
		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
		MongoPersistentProperty idProperty = persistentEntity == null ? null : persistentEntity.getIdProperty();
		String idKey = idProperty == null ? ID_FIELD : idProperty.getName();
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
		DBObject nearDbObject = near.toDBObject();

		BasicDBObject command = new BasicDBObject("geoNear", collection);
		command.putAll(nearDbObject);

		if (nearDbObject.containsField("query")) {
			DBObject query = (DBObject) nearDbObject.get("query");
			command.put("query", queryMapper.getMappedObject(query, getPersistentEntity(entityClass)));
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing geoNear using: {} for class: {} in collection: {}", serializeToJsonSafely(command),
					entityClass, collectionName);
		}

		CommandResult commandResult = executeCommand(command, this.readPreference);
		List<Object> results = (List<Object>) commandResult.get("results");
		results = results == null ? Collections.emptyList() : results;

		DbObjectCallback<GeoResult<T>> callback = new GeoNearResultDbObjectCallback<T>(
				new ReadDbObjectCallback<T>(mongoConverter, entityClass, collectionName), near.getMetric());
		List<GeoResult<T>> result = new ArrayList<GeoResult<T>>(results.size());

		int index = 0;
		int elementsToSkip = near.getSkip() != null ? near.getSkip() : 0;

		for (Object element : results) {

			/*
			 * As MongoDB currently (2.4.4) doesn't support the skipping of elements in near queries
			 * we skip the elements ourselves to avoid at least the document 2 object mapping overhead.
			 *
			 * @see <a href="https://jira.mongodb.org/browse/SERVER-3925">MongoDB Jira: SERVER-3925</a>
			 */
			if (index >= elementsToSkip) {
				result.add(callback.doWith((DBObject) element));
			}
			index++;
		}

		if (elementsToSkip > 0) {
			// as we skipped some elements we have to calculate the averageDistance ourselves:
			return new GeoResults<T>(result, near.getMetric());
		}

		GeoCommandStatistics stats = GeoCommandStatistics.from(commandResult);
		return new GeoResults<T>(result, new Distance(stats.getAverageDistance(), near.getMetric()));
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
		return doFindAndModify(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass, update, options);
	}

	// Find methods that take a Query to express the query and that return a single object that is also removed from the
	// collection in the database.

	public <T> T findAndRemove(Query query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	public <T> T findAndRemove(Query query, Class<T> entityClass, String collectionName) {

		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass);
	}

	public long count(Query query, Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null!");
		return count(query, entityClass, determineCollectionName(entityClass));
	}

	public long count(final Query query, String collectionName) {
		return count(query, null, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public long count(Query query, Class<?> entityClass, String collectionName) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		final DBObject dbObject = query == null ? null
				: queryMapper.getMappedObject(query.getQueryObject(),
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
	 * settings in sub-classes. <br />
	 * In case of using MongoDB Java driver version 3 the returned {@link WriteConcern} will be defaulted to
	 * {@link WriteConcern#ACKNOWLEDGED} when {@link WriteResultChecking} is set to {@link WriteResultChecking#EXCEPTION}.
	 *
	 * @param writeConcern any WriteConcern already configured or null
	 * @return The prepared WriteConcern or null
	 */
	protected WriteConcern prepareWriteConcern(MongoAction mongoAction) {

		WriteConcern wc = writeConcernResolver.resolve(mongoAction);
		return potentiallyForceAcknowledgedWrite(wc);
	}

	private WriteConcern potentiallyForceAcknowledgedWrite(WriteConcern wc) {

		if (ObjectUtils.nullSafeEquals(WriteResultChecking.EXCEPTION, writeResultChecking)
				&& MongoClientVersion.isMongo3Driver()) {
			if (wc == null || wc.getWObject() == null
					|| (wc.getWObject() instanceof Number && ((Number) wc.getWObject()).intValue() < 1)) {
				return WriteConcern.ACKNOWLEDGED;
			}
		}
		return wc;
	}

	protected <T> void doInsert(String collectionName, T objectToSave, MongoWriter<T> writer) {

		initializeVersionProperty(objectToSave);
		maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));
		assertUpdateableIdIfNotSet(objectToSave);

		DBObject dbDoc = toDbObject(objectToSave, writer);

		maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc, collectionName));
		Object id = insertDBObject(collectionName, dbDoc, objectToSave.getClass());

		populateIdIfNecessary(objectToSave, id);
		maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc, collectionName));
	}

	/**
	 * @param objectToSave
	 * @param writer
	 * @return
	 */
	private <T> DBObject toDbObject(T objectToSave, MongoWriter<T> writer) {

		if (!(objectToSave instanceof String)) {
			DBObject dbDoc = new BasicDBObject();
			writer.write(objectToSave, dbDoc);
			return dbDoc;
		} else {
			try {
				return (DBObject) JSON.parse((String) objectToSave);
			} catch (JSONParseException e) {
				throw new MappingException("Could not parse given String to save into a JSON document!", e);
			}
		}
	}

	private void initializeVersionProperty(Object entity) {

		MongoPersistentEntity<?> mongoPersistentEntity = getPersistentEntity(entity.getClass());

		if (mongoPersistentEntity != null && mongoPersistentEntity.hasVersionProperty()) {
			ConvertingPropertyAccessor accessor = new ConvertingPropertyAccessor(
					mongoPersistentEntity.getPropertyAccessor(entity), mongoConverter.getConversionService());
			accessor.setProperty(mongoPersistentEntity.getVersionProperty(), 0);
		}
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

		Map<String, List<T>> elementsByCollection = new HashMap<String, List<T>>();

		for (T element : listToSave) {

			if (element == null) {
				continue;
			}

			MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(element.getClass());

			if (entity == null) {
				throw new InvalidDataAccessApiUsageException("No PersistentEntity information found for " + element.getClass());
			}

			String collection = entity.getCollection();
			List<T> collectionElements = elementsByCollection.get(collection);

			if (null == collectionElements) {
				collectionElements = new ArrayList<T>();
				elementsByCollection.put(collection, collectionElements);
			}

			collectionElements.add(element);
		}

		for (Map.Entry<String, List<T>> entry : elementsByCollection.entrySet()) {
			doInsertBatch(entry.getKey(), entry.getValue(), this.mongoConverter);
		}
	}

	protected <T> void doInsertBatch(String collectionName, Collection<? extends T> batchToSave, MongoWriter<T> writer) {

		Assert.notNull(writer, "MongoWriter must not be null!");

		List<DBObject> dbObjectList = new ArrayList<DBObject>();

		for (T o : batchToSave) {

			initializeVersionProperty(o);
			maybeEmitEvent(new BeforeConvertEvent<T>(o, collectionName));

			BasicDBObject dbDoc = new BasicDBObject();
			writer.write(o, dbDoc);

			maybeEmitEvent(new BeforeSaveEvent<T>(o, dbDoc, collectionName));
			dbObjectList.add(dbDoc);
		}

		List<Object> ids = consolidateIdentifiers(insertDBObjectList(collectionName, dbObjectList), dbObjectList);

		int i = 0;
		for (T obj : batchToSave) {
			if (i < ids.size()) {
				populateIdIfNecessary(obj, ids.get(i));
				maybeEmitEvent(new AfterSaveEvent<T>(obj, dbObjectList.get(i), collectionName));
			}
			i++;
		}
	}

	public void save(Object objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		save(objectToSave, determineEntityCollectionName(objectToSave));
	}

	public void save(Object objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		MongoPersistentEntity<?> mongoPersistentEntity = getPersistentEntity(objectToSave.getClass());

		// No optimistic locking -> simple save
		if (mongoPersistentEntity == null || !mongoPersistentEntity.hasVersionProperty()) {
			doSave(collectionName, objectToSave, this.mongoConverter);
			return;
		}

		doSaveVersioned(objectToSave, mongoPersistentEntity, collectionName);
	}

	private <T> void doSaveVersioned(T objectToSave, MongoPersistentEntity<?> entity, String collectionName) {

		ConvertingPropertyAccessor convertingAccessor = new ConvertingPropertyAccessor(
				entity.getPropertyAccessor(objectToSave), mongoConverter.getConversionService());

		MongoPersistentProperty idProperty = entity.getIdProperty();
		MongoPersistentProperty versionProperty = entity.getVersionProperty();

		Object version = convertingAccessor.getProperty(versionProperty);
		Number versionNumber = convertingAccessor.getProperty(versionProperty, Number.class);

		// Fresh instance -> initialize version property
		if (version == null) {
			doInsert(collectionName, objectToSave, this.mongoConverter);
		} else {

			// Bump version number
			convertingAccessor.setProperty(versionProperty, versionNumber.longValue() + 1);

			maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));
			assertUpdateableIdIfNotSet(objectToSave);

			BasicDBObject dbObject = new BasicDBObject();

			this.mongoConverter.write(objectToSave, dbObject);

			maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbObject, collectionName));
			Update update = Update.fromDBObject(dbObject, ID_FIELD);

			// Create query for entity with the id and old version
			Object id = convertingAccessor.getProperty(idProperty);
			Query query = new Query(Criteria.where(idProperty.getName()).is(id).and(versionProperty.getName()).is(version));

			doUpdate(collectionName, query, update, objectToSave.getClass(), false, false);
			maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbObject, collectionName));
		}
	}

	protected <T> void doSave(String collectionName, T objectToSave, MongoWriter<T> writer) {

		maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));
		assertUpdateableIdIfNotSet(objectToSave);

		DBObject dbDoc = toDbObject(objectToSave, writer);

		maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc, collectionName));
		Object id = saveDBObject(collectionName, dbDoc, objectToSave.getClass());

		populateIdIfNecessary(objectToSave, id);
		maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc, collectionName));
	}

	protected Object insertDBObject(final String collectionName, final DBObject dbDoc, final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting DBObject containing fields: {} in collection: {}", dbDoc.keySet(), collectionName);
		}

		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT, collectionName,
						entityClass, dbDoc, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				WriteResult writeResult = writeConcernToUse == null ? collection.insert(dbDoc)
						: collection.insert(dbDoc, writeConcernToUse);
				handleAnyWriteResultErrors(writeResult, dbDoc, MongoActionOperation.INSERT);
				return dbDoc.get(ID_FIELD);
			}
		});
	}

	// TODO: 2.0 - Change method signature to return List<Object> and return all identifiers (DATAMONGO-1513,
	// DATAMONGO-1519)
	protected List<ObjectId> insertDBObjectList(final String collectionName, final List<DBObject> dbDocList) {
		if (dbDocList.isEmpty()) {
			return Collections.emptyList();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting list of DBObjects containing {} items", dbDocList.size());
		}

		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT_LIST, collectionName, null,
						null, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				WriteResult writeResult = writeConcernToUse == null ? collection.insert(dbDocList)
						: collection.insert(dbDocList.toArray((DBObject[]) new BasicDBObject[dbDocList.size()]), writeConcernToUse);
				handleAnyWriteResultErrors(writeResult, null, MongoActionOperation.INSERT_LIST);
				return null;
			}
		});

		List<ObjectId> ids = new ArrayList<ObjectId>();
		for (DBObject dbo : dbDocList) {
			Object id = dbo.get(ID_FIELD);
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
			LOGGER.debug("Saving DBObject containing fields: {}", dbDoc.keySet());
		}

		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.SAVE, collectionName, entityClass,
						dbDoc, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				WriteResult writeResult = writeConcernToUse == null ? collection.save(dbDoc)
						: collection.save(dbDoc, writeConcernToUse);
				handleAnyWriteResultErrors(writeResult, dbDoc, MongoActionOperation.SAVE);
				return dbDoc.get(ID_FIELD);
			}
		});
	}

	public WriteResult upsert(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, true, false);
	}

	public WriteResult upsert(Query query, Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	public WriteResult upsert(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, true, false);
	}

	public WriteResult updateFirst(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	public WriteResult updateFirst(final Query query, final Update update, final String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	public WriteResult updateFirst(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	public WriteResult updateMulti(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	public WriteResult updateMulti(final Query query, final Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	public WriteResult updateMulti(final Query query, final Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	protected WriteResult doUpdate(final String collectionName, final Query query, final Update update,
			final Class<?> entityClass, final boolean upsert, final boolean multi) {

		return execute(collectionName, new CollectionCallback<WriteResult>() {
			public WriteResult doInCollection(DBCollection collection) throws MongoException, DataAccessException {

				MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);

				increaseVersionForUpdateIfNecessary(entity, update);

				DBObject queryObj = query == null ? new BasicDBObject()
						: queryMapper.getMappedObject(query.getQueryObject(), entity);
				DBObject updateObj = update == null ? new BasicDBObject()
						: updateMapper.getMappedObject(update.getUpdateObject(), entity);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Calling update using query: {} and update: {} in collection: {}",
							serializeToJsonSafely(queryObj), serializeToJsonSafely(updateObj), collectionName);
				}

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName,
						entityClass, updateObj, queryObj);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				WriteResult writeResult = writeConcernToUse == null ? collection.update(queryObj, updateObj, upsert, multi)
						: collection.update(queryObj, updateObj, upsert, multi, writeConcernToUse);

				if (entity != null && entity.hasVersionProperty() && !multi) {
					if (ReflectiveWriteResultInvoker.wasAcknowledged(writeResult) && writeResult.getN() == 0
							&& dbObjectContainsVersionProperty(queryObj, entity)) {
						throw new OptimisticLockingFailureException("Optimistic lock exception on saving entity: "
								+ updateObj.toMap().toString() + " to collection " + collectionName);
					}
				}

				handleAnyWriteResultErrors(writeResult, queryObj, MongoActionOperation.UPDATE);
				return writeResult;
			}
		});
	}

	private void increaseVersionForUpdateIfNecessary(MongoPersistentEntity<?> persistentEntity, Update update) {

		if (persistentEntity != null && persistentEntity.hasVersionProperty()) {
			String versionFieldName = persistentEntity.getVersionProperty().getFieldName();
			if (!update.modifies(versionFieldName)) {
				update.inc(versionFieldName, 1L);
			}
		}
	}

	private boolean dbObjectContainsVersionProperty(DBObject dbObject, MongoPersistentEntity<?> persistentEntity) {

		if (persistentEntity == null || !persistentEntity.hasVersionProperty()) {
			return false;
		}

		return dbObject.containsField(persistentEntity.getVersionProperty().getFieldName());
	}

	public WriteResult remove(Object object) {

		if (object == null) {
			return null;
		}

		return remove(getIdQueryFor(object), object.getClass());
	}

	public WriteResult remove(Object object, String collection) {

		Assert.hasText(collection, "Collection name must not be null or empty!");

		if (object == null) {
			return null;
		}

		return doRemove(collection, getIdQueryFor(object), object.getClass());
	}

	/**
	 * Returns {@link Entry} containing the field name of the id property as {@link Entry#getKey()} and the {@link Id}s
	 * property value as its {@link Entry#getValue()}.
	 *
	 * @param object
	 * @return
	 */
	private Entry<String, Object> extractIdPropertyAndValue(Object object) {

		Assert.notNull(object, "Id cannot be extracted from 'null'.");

		Class<?> objectType = object.getClass();

		if (object instanceof DBObject) {
			return Collections.singletonMap(ID_FIELD, ((DBObject) object).get(ID_FIELD)).entrySet().iterator().next();
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(objectType);
		MongoPersistentProperty idProp = entity == null ? null : entity.getIdProperty();

		if (idProp == null || entity == null) {
			throw new MappingException("No id property found for object of type " + objectType);
		}

		Object idValue = entity.getPropertyAccessor(object).getProperty(idProp);
		return Collections.singletonMap(idProp.getFieldName(), idValue).entrySet().iterator().next();
	}

	/**
	 * Returns a {@link Query} for the given entity by its id.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	private Query getIdQueryFor(Object object) {

		Entry<String, Object> id = extractIdPropertyAndValue(object);
		return new Query(where(id.getKey()).is(id.getValue()));
	}

	/**
	 * Returns a {@link Query} for the given entities by their ids.
	 *
	 * @param objects must not be {@literal null} or {@literal empty}.
	 * @return
	 */
	private Query getIdInQueryFor(Collection<?> objects) {

		Assert.notEmpty(objects, "Cannot create Query for empty collection.");

		Iterator<?> it = objects.iterator();
		Entry<String, Object> firstEntry = extractIdPropertyAndValue(it.next());

		ArrayList<Object> ids = new ArrayList<Object>(objects.size());
		ids.add(firstEntry.getValue());

		while (it.hasNext()) {
			ids.add(extractIdPropertyAndValue(it.next()).getValue());
		}

		return new Query(where(firstEntry.getKey()).in(ids));
	}

	private void assertUpdateableIdIfNotSet(Object entity) {

		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entity.getClass());
		MongoPersistentProperty idProperty = persistentEntity == null ? null : persistentEntity.getIdProperty();

		if (idProperty == null || persistentEntity == null) {
			return;
		}

		Object idValue = persistentEntity.getPropertyAccessor(entity).getProperty(idProperty);

		if (idValue == null && !MongoSimpleTypes.AUTOGENERATED_ID_TYPES.contains(idProperty.getType())) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Cannot autogenerate id of type %s for entity of type %s!", idProperty.getType().getName(),
							entity.getClass().getName()));
		}
	}

	public WriteResult remove(Query query, String collectionName) {
		return remove(query, null, collectionName);
	}

	public WriteResult remove(Query query, Class<?> entityClass) {
		return remove(query, entityClass, determineCollectionName(entityClass));
	}

	public WriteResult remove(Query query, Class<?> entityClass, String collectionName) {
		return doRemove(collectionName, query, entityClass);
	}

	protected <T> WriteResult doRemove(final String collectionName, final Query query, final Class<T> entityClass) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to remove can't be null!");
		}

		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		final DBObject queryObject = query.getQueryObject();
		final MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);

		return execute(collectionName, new CollectionCallback<WriteResult>() {
			public WriteResult doInCollection(DBCollection collection) throws MongoException, DataAccessException {

				maybeEmitEvent(new BeforeDeleteEvent<T>(queryObject, entityClass, collectionName));

				DBObject dboq = queryMapper.getMappedObject(queryObject, entity);

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName,
						entityClass, null, queryObject);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Remove using query: {} in collection: {}.",
							new Object[] { serializeToJsonSafely(dboq), collectionName });
				}

				WriteResult wr = writeConcernToUse == null ? collection.remove(dboq)
						: collection.remove(dboq, writeConcernToUse);

				handleAnyWriteResultErrors(wr, dboq, MongoActionOperation.REMOVE);

				maybeEmitEvent(new AfterDeleteEvent<T>(queryObject, entityClass, collectionName));

				return wr;
			}
		});
	}

	public <T> List<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, determineCollectionName(entityClass));
	}

	public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(null), null,
				new ReadDbObjectCallback<T>(mongoConverter, entityClass, collectionName), collectionName);
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
		return mapReduce(query, inputCollectionName, mapFunction, reduceFunction, new MapReduceOptions().outputTypeInline(),
				entityClass);
	}

	public <T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction,
			String reduceFunction, MapReduceOptions mapReduceOptions, Class<T> entityClass) {

		String mapFunc = replaceWithResourceIfNecessary(mapFunction);
		String reduceFunc = replaceWithResourceIfNecessary(reduceFunction);
		DBCollection inputCollection = getCollection(inputCollectionName);

		MapReduceCommand command = new MapReduceCommand(inputCollection, mapFunc, reduceFunc,
				mapReduceOptions.getOutputCollection(), mapReduceOptions.getOutputType(),
				query == null || query.getQueryObject() == null ? null
						: queryMapper.getMappedObject(query.getQueryObject(), null));

		copyMapReduceOptionsToCommand(query, mapReduceOptions, command);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing MapReduce on collection [{}], mapFunction [{}], reduceFunction [{}]", command.getInput(),
					mapFunc, reduceFunc);
		}

		MapReduceOutput mapReduceOutput = inputCollection.mapReduce(command);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("MapReduce command result = [{}]", serializeToJsonSafely(mapReduceOutput.results()));
		}

		List<T> mappedResults = new ArrayList<T>();
		DbObjectCallback<T> callback = new ReadDbObjectCallback<T>(mongoConverter, entityClass, inputCollectionName);

		for (DBObject dbObject : mapReduceOutput.results()) {
			mappedResults.add(callback.doWith(dbObject));
		}

		return new MapReduceResults<T>(mappedResults, mapReduceOutput);
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
			dbo.put("cond", queryMapper.getMappedObject(criteria.getCriteriaObject(), null));
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
			LOGGER.debug("Executing Group with DBObject [{}]", serializeToJsonSafely(commandObject));
		}

		CommandResult commandResult = executeCommand(commandObject, getDb().getOptions());
		handleCommandError(commandResult, commandObject);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Group command result = [{}]", commandResult);
		}

		@SuppressWarnings("unchecked")
		Iterable<DBObject> resultSet = (Iterable<DBObject>) commandResult.get("retval");
		List<T> mappedResults = new ArrayList<T>();
		DbObjectCallback<T> callback = new ReadDbObjectCallback<T>(mongoConverter, entityClass, inputCollectionName);

		for (DBObject dbObject : resultSet) {
			mappedResults.add(callback.doWith(dbObject));
		}

		return new GroupByResults<T>(mappedResults, commandResult);
	}

	@Override
	public <O> AggregationResults<O> aggregate(TypedAggregation<?> aggregation, Class<O> outputType) {
		return aggregate(aggregation, determineCollectionName(aggregation.getInputType()), outputType);
	}

	@Override
	public <O> AggregationResults<O> aggregate(TypedAggregation<?> aggregation, String inputCollectionName,
			Class<O> outputType) {

		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");

		AggregationOperationContext context = new TypeBasedAggregationOperationContext(aggregation.getInputType(),
				mappingContext, queryMapper);
		return aggregate(aggregation, inputCollectionName, outputType, context);
	}

	@Override
	public <O> AggregationResults<O> aggregate(Aggregation aggregation, Class<?> inputType, Class<O> outputType) {

		return aggregate(aggregation, determineCollectionName(inputType), outputType,
				new TypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper));
	}

	@Override
	public <O> AggregationResults<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType) {
		return aggregate(aggregation, collectionName, outputType, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	@Override
	public <T> List<T> findAllAndRemove(Query query, String collectionName) {
		return (List<T>) findAllAndRemove(query, Object.class, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> List<T> findAllAndRemove(Query query, Class<T> entityClass) {
		return findAllAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> List<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName) {
		return doFindAndDelete(collectionName, query, entityClass);
	}

	/**
	 * Retrieve and remove all documents matching the given {@code query} by calling {@link #find(Query, Class, String)}
	 * and {@link #remove(Query, Class, String)}, whereas the {@link Query} for {@link #remove(Query, Class, String)} is
	 * constructed out of the find result.
	 *
	 * @param collectionName
	 * @param query
	 * @param entityClass
	 * @return
	 */
	protected <T> List<T> doFindAndDelete(String collectionName, Query query, Class<T> entityClass) {

		List<T> result = find(query, entityClass, collectionName);

		if (!CollectionUtils.isEmpty(result)) {
			remove(getIdInQueryFor(result), entityClass, collectionName);
		}

		return result;
	}

	protected <O> AggregationResults<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType,
			AggregationOperationContext context) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");
		Assert.notNull(outputType, "Output type must not be null!");

		AggregationOperationContext rootContext = context == null ? Aggregation.DEFAULT_CONTEXT : context;
		DBObject command = aggregation.toDbObject(collectionName, rootContext);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing aggregation: {}", serializeToJsonSafely(command));
		}

		CommandResult commandResult = executeCommand(command, this.readPreference);
		handleCommandError(commandResult, command);

		return new AggregationResults<O>(returnPotentiallyMappedResults(outputType, commandResult, collectionName),
				commandResult);
	}

	/**
	 * Returns the potentially mapped results of the given {@commandResult} contained some.
	 *
	 * @param outputType
	 * @param commandResult
	 * @return
	 */
	private <O> List<O> returnPotentiallyMappedResults(Class<O> outputType, CommandResult commandResult,
			String collectionName) {

		@SuppressWarnings("unchecked")
		Iterable<DBObject> resultSet = (Iterable<DBObject>) commandResult.get("result");
		if (resultSet == null) {
			return Collections.emptyList();
		}

		DbObjectCallback<O> callback = new UnwrapAndReadDbObjectCallback<O>(mongoConverter, outputType, collectionName);

		List<O> mappedResults = new ArrayList<O>();
		for (DBObject dbObject : resultSet) {
			mappedResults.add(callback.doWith(dbObject));
		}

		return mappedResults;
	}

	protected String replaceWithResourceIfNecessary(String function) {

		String func = function;

		if (this.resourceLoader != null && ResourceUtils.isUrl(function)) {

			Resource functionResource = resourceLoader.getResource(func);

			if (!functionResource.exists()) {
				throw new InvalidDataAccessApiUsageException(String.format("Resource %s not found!", function));
			}

			Scanner scanner = null;

			try {
				scanner = new Scanner(functionResource.getInputStream());
				return scanner.useDelimiter("\\A").next();
			} catch (IOException e) {
				throw new InvalidDataAccessApiUsageException(String.format("Cannot read map-reduce file %s!", function), e);
			} finally {
				if (scanner != null) {
					scanner.close();
				}
			}
		}

		return func;
	}

	private void copyMapReduceOptionsToCommand(Query query, MapReduceOptions mapReduceOptions,
			MapReduceCommand mapReduceCommand) {

		if (query != null) {
			if (query.getSkip() != 0 || query.getFieldsObject() != null) {
				throw new InvalidDataAccessApiUsageException(
						"Can not use skip or field specification with map reduce operations");
			}
			if (query.getLimit() > 0 && mapReduceOptions.getLimit() == null) {
				mapReduceCommand.setLimit(query.getLimit());
			}
			if (query.getSortObject() != null) {
				mapReduceCommand.setSort(queryMapper.getMappedObject(query.getSortObject(), null));
			}
		}

		if (mapReduceOptions.getLimit() != null && mapReduceOptions.getLimit().intValue() > 0) {
			mapReduceCommand.setLimit(mapReduceOptions.getLimit());
		}

		if (mapReduceOptions.getJavaScriptMode() != null) {
			mapReduceCommand.setJsMode(true);
		}
		if (!mapReduceOptions.getExtraOptions().isEmpty()) {
			for (Map.Entry<String, Object> entry : mapReduceOptions.getExtraOptions().entrySet()) {
				ReflectiveMapReduceInvoker.addExtraOption(mapReduceCommand, entry.getKey(), entry.getValue());
			}
		}
		if (mapReduceOptions.getFinalizeFunction() != null) {
			mapReduceCommand.setFinalize(this.replaceWithResourceIfNecessary(mapReduceOptions.getFinalizeFunction()));
		}
		if (mapReduceOptions.getOutputDatabase() != null) {
			mapReduceCommand.setOutputDB(mapReduceOptions.getOutputDatabase());
		}
		if (!mapReduceOptions.getScopeVariables().isEmpty()) {
			mapReduceCommand.setScope(mapReduceOptions.getScopeVariables());
		}
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
					LOGGER.debug("Created collection [{}]", coll.getFullName());
				}
				return coll;
			}
		});
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The query document is specified as a standard {@link DBObject} and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param query the query document that specifies the criteria used to find a record.
	 * @param fields the document that specifies the fields to be returned.
	 * @param entityClass the parameterized type of the returned list.
	 * @return the {@link List} of converted objects.
	 */
	protected <T> T doFindOne(String collectionName, DBObject query, DBObject fields, Class<T> entityClass) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		DBObject mappedQuery = queryMapper.getMappedObject(query, entity);
		DBObject mappedFields = fields == null ? null : queryMapper.getMappedObject(fields, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findOne using query: {} fields: {} for class: {} in collection: {}", serializeToJsonSafely(query),
					mappedFields, entityClass, collectionName);
		}

		return executeFindOneInternal(new FindOneCallback(mappedQuery, mappedFields),
				new ReadDbObjectCallback<T>(this.mongoConverter, entityClass, collectionName), collectionName);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List using the template's converter. The
	 * query document is specified as a standard DBObject and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param fields the document that specifies the fields to be returned
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> entityClass) {
		return doFind(collectionName, query, fields, entityClass, null,
				new ReadDbObjectCallback<T>(this.mongoConverter, entityClass, collectionName));
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type. The object is
	 * converted from the MongoDB native representation using an instance of {@see MongoConverter}. The query document is
	 * specified as a standard DBObject and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param query the query document that specifies the criteria used to find a record.
	 * @param fields the document that specifies the fields to be returned.
	 * @param entityClass the parameterized type of the returned list.
	 * @param preparer allows for customization of the {@link DBCursor} used when iterating over the result set, (apply
	 *          limits, skips and so on).
	 * @return the {@link List} of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<T> entityClass,
			CursorPreparer preparer) {
		return doFind(collectionName, query, fields, entityClass, preparer,
				new ReadDbObjectCallback<T>(mongoConverter, entityClass, collectionName));
	}

	protected <S, T> List<T> doFind(String collectionName, DBObject query, DBObject fields, Class<S> entityClass,
			CursorPreparer preparer, DbObjectCallback<T> objectCallback) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		DBObject mappedFields = queryMapper.getMappedFields(fields, entity);
		DBObject mappedQuery = queryMapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: {} fields: {} for class: {} in collection: {}",
					serializeToJsonSafely(mappedQuery), mappedFields, entityClass, collectionName);
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields), preparer, objectCallback,
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
			LOGGER.debug("findAndRemove using query: {} fields: {} sort: {} for class: {} in collection: {}",
					serializeToJsonSafely(query), fields, sort, entityClass, collectionName);
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		return executeFindOneInternal(new FindAndRemoveCallback(queryMapper.getMappedObject(query, entity), fields, sort),
				new ReadDbObjectCallback<T>(readerToUse, entityClass, collectionName), collectionName);
	}

	protected <T> T doFindAndModify(String collectionName, DBObject query, DBObject fields, DBObject sort,
			Class<T> entityClass, Update update, FindAndModifyOptions options) {

		EntityReader<? super T, DBObject> readerToUse = this.mongoConverter;

		if (options == null) {
			options = new FindAndModifyOptions();
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		increaseVersionForUpdateIfNecessary(entity, update);

		DBObject mappedQuery = queryMapper.getMappedObject(query, entity);
		DBObject mappedUpdate = updateMapper.getMappedObject(update.getUpdateObject(), entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					"findAndModify using query: {} fields: {} sort: {} for class: {} and update: {} " + "in collection: {}",
					serializeToJsonSafely(mappedQuery), fields, sort, entityClass, serializeToJsonSafely(mappedUpdate),
					collectionName);
		}

		return executeFindOneInternal(new FindAndModifyCallback(mappedQuery, fields, sort, mappedUpdate, options),
				new ReadDbObjectCallback<T>(readerToUse, entityClass, collectionName), collectionName);
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

		if (savedObject instanceof BasicDBObject) {
			DBObject dbObject = (DBObject) savedObject;
			dbObject.put(ID_FIELD, id);
			return;
		}

		MongoPersistentProperty idProp = getIdPropertyFor(savedObject.getClass());

		if (idProp == null) {
			return;
		}

		ConversionService conversionService = mongoConverter.getConversionService();
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(savedObject.getClass());
		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(savedObject);

		if (accessor.getProperty(idProp) != null) {
			return;
		}

		new ConvertingPropertyAccessor(accessor, conversionService).setProperty(idProp, id);
	}

	private DBCollection getAndPrepareCollection(DB db, String collectionName) {
		try {
			DBCollection collection = db.getCollection(collectionName);
			prepareCollection(collection);
			return collection;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
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
			T result = objectCallback
					.doWith(collectionCallback.doInCollection(getAndPrepareCollection(getDb(), collectionName)));
			return result;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
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
	private <T> List<T> executeFindMultiInternal(CollectionCallback<DBCursor> collectionCallback, CursorPreparer preparer,
			DbObjectCallback<T> objectCallback, String collectionName) {

		try {

			DBCursor cursor = null;

			try {

				cursor = collectionCallback.doInCollection(getAndPrepareCollection(getDb(), collectionName));

				if (preparer != null) {
					cursor = preparer.prepare(cursor);
				}

				List<T> result = new ArrayList<T>();

				while (cursor.hasNext()) {
					DBObject object = cursor.next();
					result.add(objectCallback.doWith(object));
				}

				return result;

			} finally {

				if (cursor != null) {
					cursor.close();
				}
			}
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	private void executeQueryInternal(CollectionCallback<DBCursor> collectionCallback, CursorPreparer preparer,
			DocumentCallbackHandler callbackHandler, String collectionName) {

		try {

			DBCursor cursor = null;

			try {
				cursor = collectionCallback.doInCollection(getAndPrepareCollection(getDb(), collectionName));

				if (preparer != null) {
					cursor = preparer.prepare(cursor);
				}

				while (cursor.hasNext()) {
					DBObject dbobject = cursor.next();
					callbackHandler.processDocument(dbobject);
				}

			} finally {
				if (cursor != null) {
					cursor.close();
				}
			}

		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	private MongoPersistentEntity<?> getPersistentEntity(Class<?> type) {
		return type == null ? null : mappingContext.getPersistentEntity(type);
	}

	private MongoPersistentProperty getIdPropertyFor(Class<?> type) {
		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(type);
		return persistentEntity == null ? null : persistentEntity.getIdProperty();
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
			throw new InvalidDataAccessApiUsageException(
					"No Persistent Entity information found for the class " + entityClass.getName());
		}
		return entity.getCollection();
	}

	/**
	 * Handles {@link WriteResult} errors based on the configured {@link WriteResultChecking}.
	 *
	 * @param writeResult
	 * @param query
	 * @param operation
	 */
	protected void handleAnyWriteResultErrors(WriteResult writeResult, DBObject query, MongoActionOperation operation) {

		if (writeResultChecking == WriteResultChecking.NONE) {
			return;
		}

		String error = ReflectiveWriteResultInvoker.getError(writeResult);

		if (error == null) {
			return;
		}

		String message;

		switch (operation) {

			case INSERT:
			case SAVE:
				message = String.format("Insert/Save for %s failed: %s", query, error);
				break;
			case INSERT_LIST:
				message = String.format("Insert list failed: %s", error);
				break;
			default:
				message = String.format("Execution of %s%s failed: %s", operation,
						query == null ? "" : " using query " + query.toString(), error);
		}

		if (writeResultChecking == WriteResultChecking.EXCEPTION) {
			throw new MongoDataIntegrityViolationException(message, writeResult, operation);
		} else {
			LOGGER.error(message);
			return;
		}
	}

	/**
	 * Inspects the given {@link CommandResult} for erros and potentially throws an
	 * {@link InvalidDataAccessApiUsageException} for that error.
	 *
	 * @param result must not be {@literal null}.
	 * @param source must not be {@literal null}.
	 */
	private void handleCommandError(CommandResult result, DBObject source) {

		try {
			result.throwOnError();
		} catch (MongoException ex) {

			String error = result.getErrorMessage();
			error = error == null ? "NO MESSAGE" : error;

			throw new InvalidDataAccessApiUsageException(
					"Command execution failed:  Error [" + error + "], Command = " + source, ex);
		}
	}

	private static final MongoConverter getDefaultMongoConverter(MongoDbFactory factory) {

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, new MongoMappingContext());
		converter.afterPropertiesSet();
		return converter;
	}

	private DBObject getMappedSortObject(Query query, Class<?> type) {

		if (query == null || query.getSortObject() == null) {
			return null;
		}

		return queryMapper.getMappedSort(query.getSortObject(), mappingContext.getPersistentEntity(type));
	}

	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe re-throwing of the return value.
	 *
	 * @param ex the exception to translate
	 * @param exceptionTranslator the {@link PersistenceExceptionTranslator} to be used for translation
	 * @return
	 */
	private static RuntimeException potentiallyConvertRuntimeException(RuntimeException ex,
			PersistenceExceptionTranslator exceptionTranslator) {
		RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	/**
	 * Returns all identifiers for the given documents. Will augment the given identifiers and fill in only the ones that
	 * are {@literal null} currently. This would've been better solved in {@link #insertDBObjectList(String, List)}
	 * directly but would require a signature change of that method.
	 *
	 * @param ids
	 * @param documents
	 * @return TODO: Remove for 2.0 and change method signature of {@link #insertDBObjectList(String, List)}.
	 */
	private static List<Object> consolidateIdentifiers(List<ObjectId> ids, List<DBObject> documents) {

		List<Object> result = new ArrayList<Object>(ids.size());

		for (int i = 0; i < ids.size(); i++) {

			ObjectId objectId = ids.get(i);
			result.add(objectId == null ? documents.get(i).get(ID_FIELD) : objectId);
		}

		return result;
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
					LOGGER.debug("findOne using query: {} in db.collection: {}", serializeToJsonSafely(query),
							collection.getFullName());
				}
				return collection.findOne(query);
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("findOne using query: {} fields: {} in db.collection: {}", serializeToJsonSafely(query), fields,
							collection.getFullName());
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
			this.query = query == null ? new BasicDBObject() : query;
			this.fields = fields;
		}

		public DBCursor doInCollection(DBCollection collection) throws MongoException, DataAccessException {

			if (fields == null || fields.toMap().isEmpty()) {
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
	 * @author Thomas Darimont
	 */

	interface DbObjectCallback<T> {

		T doWith(DBObject object);
	}

	/**
	 * Simple {@link DbObjectCallback} that will transform {@link DBObject} into the given target type using the given
	 * {@link MongoReader}.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 */
	private class ReadDbObjectCallback<T> implements DbObjectCallback<T> {

		private final EntityReader<? super T, DBObject> reader;
		private final Class<T> type;
		private final String collectionName;

		public ReadDbObjectCallback(EntityReader<? super T, DBObject> reader, Class<T> type, String collectionName) {

			Assert.notNull(reader, "EntityReader must not be null!");
			Assert.notNull(type, "Entity type must not be null!");

			this.reader = reader;
			this.type = type;
			this.collectionName = collectionName;
		}

		public T doWith(DBObject object) {
			if (null != object) {
				maybeEmitEvent(new AfterLoadEvent<T>(object, type, collectionName));
			}
			T source = reader.read(type, object);
			if (null != source) {
				maybeEmitEvent(new AfterConvertEvent<T>(object, source, collectionName));
			}
			return source;
		}
	}

	class UnwrapAndReadDbObjectCallback<T> extends ReadDbObjectCallback<T> {

		public UnwrapAndReadDbObjectCallback(EntityReader<? super T, DBObject> reader, Class<T> type,
				String collectionName) {
			super(reader, type, collectionName);
		}

		@Override
		public T doWith(DBObject object) {

			Object idField = object.get(Fields.UNDERSCORE_ID);

			if (!(idField instanceof DBObject)) {
				return super.doWith(object);
			}

			DBObject toMap = new BasicDBObject();
			DBObject nested = (DBObject) idField;
			toMap.putAll(nested);

			for (String key : object.keySet()) {
				if (!Fields.UNDERSCORE_ID.equals(key)) {
					toMap.put(key, object.get(key));
				}
			}

			return super.doWith(toMap);
		}
	}

	class QueryCursorPreparer implements CursorPreparer {

		private final Query query;
		private final Class<?> type;

		public QueryCursorPreparer(Query query, Class<?> type) {

			this.query = query;
			this.type = type;
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
					&& !StringUtils.hasText(query.getHint()) && !query.getMeta().hasValues()) {
				return cursor;
			}

			DBCursor cursorToUse = cursor.copy();

			try {

				if (query.getSkip() > 0) {
					cursorToUse = cursorToUse.skip(query.getSkip());
				}

				if (query.getLimit() > 0) {
					cursorToUse = cursorToUse.limit(query.getLimit());
				}

				if (query.getSortObject() != null) {
					DBObject sortDbo = type != null ? getMappedSortObject(query, type) : query.getSortObject();
					cursorToUse = cursorToUse.sort(sortDbo);
				}

				if (StringUtils.hasText(query.getHint())) {
					cursorToUse = cursorToUse.hint(query.getHint());
				}

				if (query.getMeta().hasValues()) {

					for (Entry<String, Object> entry : query.getMeta().values()) {
						cursorToUse = cursorToUse.addSpecial(entry.getKey(), entry.getValue());
					}

					for (Meta.CursorOption option : query.getMeta().getFlags()) {

						switch (option) {
							case EXHAUST:
								cursorToUse = cursorToUse.addOption(Bytes.QUERYOPTION_EXHAUST);
								break;
							case NO_TIMEOUT:
								cursorToUse = cursorToUse.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
								break;
							case PARTIAL:
								cursorToUse = cursorToUse.addOption(Bytes.QUERYOPTION_PARTIAL);
								break;
							case SLAVE_OK:
								cursorToUse = cursorToUse.addOption(Bytes.QUERYOPTION_SLAVEOK);
								break;
							default:
								throw new IllegalArgumentException(String.format("%s is no supported flag.", option));
						}
					}
				}

			} catch (RuntimeException e) {
				throw potentiallyConvertRuntimeException(e, exceptionTranslator);
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
		 * @param delegate must not be {@literal null}.
		 */
		public GeoNearResultDbObjectCallback(DbObjectCallback<T> delegate, Metric metric) {

			Assert.notNull(delegate, "DocumentCallback must not be null!");

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

	/**
	 * A {@link CloseableIterator} that is backed by a MongoDB {@link Cursor}.
	 *
	 * @since 1.7
	 * @author Thomas Darimont
	 */
	static class CloseableIterableCursorAdapter<T> implements CloseableIterator<T> {

		private volatile Cursor cursor;
		private PersistenceExceptionTranslator exceptionTranslator;
		private DbObjectCallback<T> objectReadCallback;

		/**
		 * Creates a new {@link CloseableIterableCursorAdapter} backed by the given {@link Cursor}.
		 *
		 * @param cursor
		 * @param exceptionTranslator
		 * @param objectReadCallback
		 */
		public CloseableIterableCursorAdapter(Cursor cursor, PersistenceExceptionTranslator exceptionTranslator,
				DbObjectCallback<T> objectReadCallback) {

			this.cursor = cursor;
			this.exceptionTranslator = exceptionTranslator;
			this.objectReadCallback = objectReadCallback;
		}

		@Override
		public boolean hasNext() {

			if (cursor == null) {
				return false;
			}

			try {
				return cursor.hasNext();
			} catch (RuntimeException ex) {
				throw potentiallyConvertRuntimeException(ex, exceptionTranslator);
			}
		}

		@Override
		public T next() {

			if (cursor == null) {
				return null;
			}

			try {
				DBObject item = cursor.next();
				T converted = objectReadCallback.doWith(item);
				return converted;
			} catch (RuntimeException ex) {
				throw potentiallyConvertRuntimeException(ex, exceptionTranslator);
			}
		}

		@Override
		public void close() {

			Cursor c = cursor;
			try {

				if (c != null) {
					c.close();
				}

			} catch (RuntimeException ex) {
				throw potentiallyConvertRuntimeException(ex, exceptionTranslator);
			} finally {
				cursor = null;
				exceptionTranslator = null;
				objectReadCallback = null;
			}
		}
	}
}
