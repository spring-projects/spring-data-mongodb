/*
 * Copyright 2010-2016 the original author or authors.
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.mongodb.*;
import org.bson.Document;
import org.bson.conversions.Bson;
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

import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
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

	private Mongo mongo;

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

		Assert.notNull(mongoDbFactory);

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
	 * Returns the default {@link org.springframework.data.mongodb.core.core.convert.MongoConverter}.
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
			public CloseableIterator<T> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityType);

				Document mappedFields = queryMapper.getMappedFields(query.getFieldsObject(), persistentEntity);
				Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), persistentEntity);

				FindIterable<Document> cursor = collection.find(mappedQuery).projection(mappedFields);
				QueryCursorPreparer cursorPreparer = new QueryCursorPreparer(query, entityType);

				ReadDocumentCallback<T> readCallback = new ReadDocumentCallback<T>(mongoConverter, entityType,
						collectionName);

				return new CloseableIterableCursorAdapter<T>(cursorPreparer.prepare(cursor), exceptionTranslator, readCallback);
			}
		});
	}

	public String getCollectionName(Class<?> entityClass) {
		return this.determineCollectionName(entityClass);
	}

	public Document executeCommand(final String jsonCommand) {

		return execute(new DbCallback<Document>() {
			public Document doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.runCommand(Document.parse(jsonCommand), Document.class);
			}
		});
	}

	public Document executeCommand(final Document command) {

		Document result = execute(new DbCallback<Document>() {
			public Document doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.runCommand(command, Document.class);
			}
		});

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeCommand(com.mongodb.Document, com.mongodb.ReadPreference)
	 */
	public Document executeCommand(final Document command, final ReadPreference readPreference) {

		Assert.notNull(command, "Command must not be null!");

		Document result = execute(new DbCallback<Document>() {
			public Document doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return readPreference != null ? db.runCommand(command, readPreference, Document.class)
						: db.runCommand(command, Document.class);
			}
		});

		return result;
	}

	protected void logCommandExecutionError(final Document command, CommandResult result) {

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

		Assert.notNull(query);

		Document queryObject = queryMapper.getMappedObject(query.getQueryObject(), null);
		Document sortObject = query.getSortObject();
		Document fieldsObject = query.getFieldsObject();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing query: {} sort: {} fields: {} in collection: {}", serializeToJsonSafely(queryObject),
					sortObject, fieldsObject, collectionName);
		}

		this.executeQueryInternal(new FindCallback(queryObject, fieldsObject), preparer, dch, collectionName);
	}

	public <T> T execute(DbCallback<T> action) {

		Assert.notNull(action);

		try {
			MongoDatabase db = this.getDb();
			return action.doInDB(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	public <T> T execute(Class<?> entityClass, CollectionCallback<T> callback) {
		return execute(determineCollectionName(entityClass), callback);
	}

	public <T> T execute(String collectionName, CollectionCallback<T> callback) {

		Assert.notNull(callback);

		try {
			MongoCollection<Document> collection = getAndPrepareCollection(getDb(), collectionName);
			return callback.doInCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	public <T> MongoCollection<Document> createCollection(Class<T> entityClass) {
		return createCollection(determineCollectionName(entityClass));
	}

	public <T> MongoCollection<Document> createCollection(Class<T> entityClass, CollectionOptions collectionOptions) {
		return createCollection(determineCollectionName(entityClass), collectionOptions);
	}

	public MongoCollection<Document> createCollection(final String collectionName) {
		return doCreateCollection(collectionName, new Document());
	}

	public MongoCollection<Document> createCollection(final String collectionName,
			final CollectionOptions collectionOptions) {
		return doCreateCollection(collectionName, convertToDocument(collectionOptions));
	}

	public MongoCollection<Document> getCollection(final String collectionName) {
		return execute(new DbCallback<MongoCollection<Document>>() {
			public MongoCollection<Document> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.getCollection(collectionName, Document.class);
			}
		});
	}

	public <T> boolean collectionExists(Class<T> entityClass) {
		return collectionExists(determineCollectionName(entityClass));
	}

	public boolean collectionExists(final String collectionName) {
		return execute(new DbCallback<Boolean>() {
			public Boolean doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				for (String name : db.listCollectionNames()) {
					if (name.equals(collectionName)) {
						return true;
					}
				}
				return false;
			}
		});
	}

	public <T> void dropCollection(Class<T> entityClass) {
		dropCollection(determineCollectionName(entityClass));
	}

	public void dropCollection(String collectionName) {
		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				collection.drop();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Dropped collection [{}]", collection.getNamespace().getCollectionName());
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

	public BulkOperations bulkOps(BulkMode bulkMode, String collectionName) {
		return bulkOps(bulkMode, null, collectionName);
	}

	public BulkOperations bulkOps(BulkMode bulkMode, Class<?> entityClass) {
		return bulkOps(bulkMode, entityClass, determineCollectionName(entityClass));
	}

	public BulkOperations bulkOps(BulkMode mode, Class<?> entityType, String collectionName) {

		Assert.notNull(mode, "BulkMode must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		DefaultBulkOperations operations = new DefaultBulkOperations(this, mode, collectionName, entityType);

		operations.setExceptionTranslator(exceptionTranslator);
		operations.setWriteConcernResolver(writeConcernResolver);
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

		Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), getPersistentEntity(entityClass));
		return execute(collectionName, new FindCallback(mappedQuery)).iterator().hasNext();
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
		return doFindOne(collectionName, new Document(idKey, id), null, entityClass);
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
		Document nearDbObject = near.toDocument();

		Document command = new Document("geoNear", collection);
		command.putAll(nearDbObject);

		if (nearDbObject.containsKey("query")) {
			Document query = (Document) nearDbObject.get("query");
			command.put("query", queryMapper.getMappedObject(query, getPersistentEntity(entityClass)));
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing geoNear using: {} for class: {} in collection: {}", serializeToJsonSafely(command),
					entityClass, collectionName);
		}

		Document commandResult = executeCommand(command, this.readPreference);
		List<Object> results = (List<Object>) commandResult.get("results");
		results = results == null ? Collections.emptyList() : results;

		DocumentCallback<GeoResult<T>> callback = new GeoNearResultDocumentCallback<T>(
				new ReadDocumentCallback<T>(mongoConverter, entityClass, collectionName), near.getMetric());
		List<GeoResult<T>> result = new ArrayList<GeoResult<T>>(results.size());

		int index = 0;
		int elementsToSkip = near.getSkip() != null ? near.getSkip() : 0;

		for (Object element : results) {

			/*
			 * As MongoDB currently (2.4.4) doesn't support the skipping of elements in near queries
			 * we skip the elements ourselves to avoid at least the document 2 object mapping overhead.
			 *
			 * @see https://jira.mongodb.org/browse/SERVER-3925
			 */
			if (index >= elementsToSkip) {
				result.add(callback.doWith((Document) element));
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
		Assert.notNull(entityClass);
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

		Assert.hasText(collectionName);
		final Document document = query == null ? null
				: queryMapper.getMappedObject(query.getQueryObject(),
						entityClass == null ? null : mappingContext.getPersistentEntity(entityClass));

		return execute(collectionName, new CollectionCallback<Long>() {
			public Long doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				return collection.count(document);
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
	protected MongoCollection<Document> prepareCollection(MongoCollection<Document> collection) {

		if (this.readPreference != null) {
			return collection.withReadPreference(readPreference);
		}
		return collection;
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

		assertUpdateableIdIfNotSet(objectToSave);

		initializeVersionProperty(objectToSave);

		maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));

		Document dbDoc = toDocument(objectToSave, writer);

		maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc, collectionName));
		Object id = insertDocument(collectionName, dbDoc, objectToSave.getClass());

		populateIdIfNecessary(objectToSave, id);
		maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc, collectionName));
	}

	/**
	 * @param objectToSave
	 * @param writer
	 * @return
	 */
	private <T> Document toDocument(T objectToSave, MongoWriter<T> writer) {

		if (objectToSave instanceof Document) {
			return (Document) objectToSave;
		}

		if (!(objectToSave instanceof String)) {
			Document dbDoc = new Document();
			writer.write(objectToSave, dbDoc);

			if (dbDoc.containsKey(ID_FIELD) && dbDoc.get(ID_FIELD) == null) {
				dbDoc.remove(ID_FIELD);
			}
			return dbDoc;
		} else {
			try {
				return Document.parse((String) objectToSave);
			} catch (JSONParseException e) {
				throw new MappingException("Could not parse given String to save into a JSON document!", e);
			} catch (org.bson.json.JsonParseException e) {
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

		Assert.notNull(writer);

		List<Document> documentList = new ArrayList<Document>();
		for (T o : batchToSave) {

			initializeVersionProperty(o);
			Document dbDoc = new Document();

			maybeEmitEvent(new BeforeConvertEvent<T>(o, collectionName));
			writer.write(o, dbDoc);

			maybeEmitEvent(new BeforeSaveEvent<T>(o, dbDoc, collectionName));
			documentList.add(dbDoc);
		}

		List<Object> ids = consolidateIdentifiers(insertDocumentList(collectionName, documentList), documentList);

		int i = 0;
		for (T obj : batchToSave) {
			if (i < ids.size()) {
				populateIdIfNecessary(obj, ids.get(i));
				maybeEmitEvent(new AfterSaveEvent<T>(obj, documentList.get(i), collectionName));
			}
			i++;
		}
	}

	public void save(Object objectToSave) {

		Assert.notNull(objectToSave);
		save(objectToSave, determineEntityCollectionName(objectToSave));
	}

	public void save(Object objectToSave, String collectionName) {

		Assert.notNull(objectToSave);
		Assert.hasText(collectionName);

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

			assertUpdateableIdIfNotSet(objectToSave);

			// Create query for entity with the id and old version
			Object id = convertingAccessor.getProperty(idProperty);
			Query query = new Query(Criteria.where(idProperty.getName()).is(id).and(versionProperty.getName()).is(version));

			// Bump version number
			convertingAccessor.setProperty(versionProperty, versionNumber.longValue() + 1);

			Document document = new Document();

			maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));
			this.mongoConverter.write(objectToSave, document);

			maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, document, collectionName));
			Update update = Update.fromDocument(document, ID_FIELD);

			UpdateResult result = doUpdate(collectionName, query, update, objectToSave.getClass(), false, false);

			if (result.getModifiedCount() == 0) {
				throw new OptimisticLockingFailureException(
						String.format("Cannot save entity %s with version %s to collection %s. Has it been modified meanwhile?", id,
								versionNumber, collectionName));
			}
			maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, document, collectionName));
		}
	}

	protected <T> void doSave(String collectionName, T objectToSave, MongoWriter<T> writer) {

		assertUpdateableIdIfNotSet(objectToSave);

		maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));

		Document dbDoc = toDocument(objectToSave, writer);

		maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc, collectionName));
		Object id = saveDocument(collectionName, dbDoc, objectToSave.getClass());

		populateIdIfNecessary(objectToSave, id);
		maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc, collectionName));
	}

	protected Object insertDocument(final String collectionName, final Document document, final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting Document containing fields: {} in collection: {}", document.keySet(), collectionName);
		}

		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT, collectionName,
						entityClass, document, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				if (writeConcernToUse == null) {
					collection.insertOne(document);
				} else {
					collection.withWriteConcern(writeConcernToUse).insertOne(document);
				}
				return document.get(ID_FIELD);
			}
		});
	}

	// TODO: 2.0 - Change method signature to return List<Object> and return all identifiers (DATAMONGO-1513,
	// DATAMONGO-1519)
	protected List<ObjectId> insertDocumentList(final String collectionName, final List<Document> documents) {
		if (documents.isEmpty()) {
			return Collections.emptyList();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting list of Documents containing {} items", documents.size());
		}

		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT_LIST, collectionName, null,
						null, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				if (writeConcernToUse == null) {
					collection.insertMany(documents);
				} else {
					collection.withWriteConcern(writeConcernToUse).insertMany(documents);
				}

				return null;
			}
		});

		List<ObjectId> ids = new ArrayList<ObjectId>();
		for (Document dbo : documents) {
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

	protected Object saveDocument(final String collectionName, final Document dbDoc, final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Saving Document containing fields: {}", dbDoc.keySet());
		}

		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.SAVE, collectionName, entityClass,
						dbDoc, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				if (!dbDoc.containsKey(ID_FIELD)) {
					if (writeConcernToUse == null) {
						collection.insertOne(dbDoc);
					} else {
						collection.withWriteConcern(writeConcernToUse).insertOne(dbDoc);
					}
				}

				else if (writeConcernToUse == null) {
					collection.replaceOne(Filters.eq(ID_FIELD, dbDoc.get(ID_FIELD)), dbDoc, new UpdateOptions().upsert(true));
				} else {
					collection.withWriteConcern(writeConcernToUse).replaceOne(Filters.eq(ID_FIELD, dbDoc.get(ID_FIELD)), dbDoc,
							new UpdateOptions().upsert(true));
				}
				return dbDoc.get(ID_FIELD);
			}
		});
	}

	public UpdateResult upsert(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, true, false);
	}

	public UpdateResult upsert(Query query, Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	public UpdateResult upsert(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, true, false);
	}

	public UpdateResult updateFirst(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	public UpdateResult updateFirst(final Query query, final Update update, final String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	public UpdateResult updateFirst(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	public UpdateResult updateMulti(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	public UpdateResult updateMulti(final Query query, final Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	public UpdateResult updateMulti(final Query query, final Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	protected UpdateResult doUpdate(final String collectionName, final Query query, final Update update,
			final Class<?> entityClass, final boolean upsert, final boolean multi) {

		return execute(collectionName, new CollectionCallback<UpdateResult>() {
			public UpdateResult doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);

				increaseVersionForUpdateIfNecessary(entity, update);

				Document queryObj = query == null ? new Document()
						: queryMapper.getMappedObject(query.getQueryObject(), entity);
				Document updateObj = update == null ? new Document()
						: updateMapper.getMappedObject(update.getUpdateObject(), entity);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Calling update using query: {} and update: {} in collection: {}",
							serializeToJsonSafely(queryObj), serializeToJsonSafely(updateObj), collectionName);
				}

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName,
						entityClass, updateObj, queryObj);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				UpdateOptions opts = new UpdateOptions();
				opts.upsert(upsert);

				collection = writeConcernToUse != null ? collection.withWriteConcern(writeConcernToUse) : collection;

				if (!UpdateMapper.isUpdateObject(updateObj)) {
					return collection.replaceOne(queryObj, updateObj, opts);
				} else {
					if (multi) {
						return collection.updateMany(queryObj, updateObj, opts);
					} else {
						return collection.updateOne(queryObj, updateObj, opts);
					}
				}
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

	private boolean documentContainsVersionProperty(Document document, MongoPersistentEntity<?> persistentEntity) {

		if (persistentEntity == null || !persistentEntity.hasVersionProperty()) {
			return false;
		}

		return document.containsKey(persistentEntity.getVersionProperty().getFieldName());
	}

	public DeleteResult remove(Object object) {

		if (object == null) {
			return null;
		}

		return remove(getIdQueryFor(object), object.getClass());
	}

	public DeleteResult remove(Object object, String collection) {

		Assert.hasText(collection);

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

		if (object instanceof Document) {
			return Collections.singletonMap(ID_FIELD, ((Document) object).get(ID_FIELD)).entrySet().iterator().next();
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

	public DeleteResult remove(Query query, String collectionName) {
		return remove(query, null, collectionName);
	}

	public DeleteResult remove(Query query, Class<?> entityClass) {
		return remove(query, entityClass, determineCollectionName(entityClass));
	}

	public DeleteResult remove(Query query, Class<?> entityClass, String collectionName) {
		return doRemove(collectionName, query, entityClass);
	}

	protected <T> DeleteResult doRemove(final String collectionName, final Query query, final Class<T> entityClass) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to remove can't be null!");
		}

		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		final Document queryObject = query.getQueryObject();
		final MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);

		return execute(collectionName, new CollectionCallback<DeleteResult>() {
			public DeleteResult doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				maybeEmitEvent(new BeforeDeleteEvent<T>(queryObject, entityClass, collectionName));

				Document mappedQuery = queryMapper.getMappedObject(queryObject, entity);

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName,
						entityClass, null, queryObject);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				DeleteResult dr = null;
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Remove using query: {} in collection: {}.",
							new Object[] { serializeToJsonSafely(mappedQuery), collectionName });
				}

				if (writeConcernToUse == null) {
					dr = collection.deleteMany(mappedQuery);
				} else {
					dr = collection.withWriteConcern(writeConcernToUse).deleteMany(mappedQuery);
				}

				maybeEmitEvent(new AfterDeleteEvent<T>(queryObject, entityClass, collectionName));

				return dr;
			}
		});
	}

	public <T> List<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, determineCollectionName(entityClass));
	}

	public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(null), null,
				new ReadDocumentCallback<T>(mongoConverter, entityClass, collectionName), collectionName);
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
		MongoCollection<Document> inputCollection = getCollection(inputCollectionName);

		// MapReduceOp
		MapReduceIterable<Document> result = inputCollection.mapReduce(mapFunc, reduceFunc);
		if (query != null && result != null) {

			if (query.getLimit() > 0 && mapReduceOptions.getLimit() == null) {
				result = result.limit(query.getLimit());
			}
			if (query.getMeta() != null && query.getMeta().getMaxTimeMsec() != null) {
				result = result.maxTime(query.getMeta().getMaxTimeMsec(), TimeUnit.MILLISECONDS);
			}
			if (query.getSortObject() != null) {
				result = result.sort(query.getSortObject());
			}

			result = result.filter(queryMapper.getMappedObject(query.getQueryObject(), null));
		}

		if (mapReduceOptions != null) {

			if (!CollectionUtils.isEmpty(mapReduceOptions.getScopeVariables())) {
				Document vars = new Document();
				vars.putAll(mapReduceOptions.getScopeVariables());
				result = result.scope(vars);
			}
			if (mapReduceOptions.getLimit() != null && mapReduceOptions.getLimit().intValue() > 0) {
				result = result.limit(mapReduceOptions.getLimit());
			}
			if (StringUtils.hasText(mapReduceOptions.getFinalizeFunction())) {
				result = result.finalizeFunction(mapReduceOptions.getFinalizeFunction());
			}
			if (mapReduceOptions.getJavaScriptMode() != null) {
				result = result.jsMode(mapReduceOptions.getJavaScriptMode());
			}
			if (mapReduceOptions.getOutputSharded() != null) {
				result = result.sharded(mapReduceOptions.getOutputSharded());
			}
		}
		List<T> mappedResults = new ArrayList<T>();
		DocumentCallback<T> callback = new ReadDocumentCallback<T>(mongoConverter, entityClass, inputCollectionName);

		for (Document document : result) {
			mappedResults.add(callback.doWith(document));
		}

		return new MapReduceResults<T>(mappedResults, new Document());
	}

	public <T> GroupByResults<T> group(String inputCollectionName, GroupBy groupBy, Class<T> entityClass) {
		return group(null, inputCollectionName, groupBy, entityClass);
	}

	public <T> GroupByResults<T> group(Criteria criteria, String inputCollectionName, GroupBy groupBy,
			Class<T> entityClass) {

		Document document = groupBy.getGroupByObject();
		document.put("ns", inputCollectionName);

		if (criteria == null) {
			document.put("cond", null);
		} else {
			document.put("cond", queryMapper.getMappedObject(criteria.getCriteriaObject(), null));
		}
		// If initial document was a JavaScript string, potentially loaded by Spring's Resource abstraction, load it and
		// convert to Document

		if (document.containsKey("initial")) {
			Object initialObj = document.get("initial");
			if (initialObj instanceof String) {
				String initialAsString = replaceWithResourceIfNecessary((String) initialObj);
				document.put("initial", Document.parse(initialAsString));
			}
		}

		if (document.containsKey("$reduce")) {
			document.put("$reduce", replaceWithResourceIfNecessary(document.get("$reduce").toString()));
		}
		if (document.containsKey("$keyf")) {
			document.put("$keyf", replaceWithResourceIfNecessary(document.get("$keyf").toString()));
		}
		if (document.containsKey("finalize")) {
			document.put("finalize", replaceWithResourceIfNecessary(document.get("finalize").toString()));
		}

		Document commandObject = new Document("group", document);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing Group with Document [{}]", serializeToJsonSafely(commandObject));
		}

		Document commandResult = executeCommand(commandObject);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Group command result = [{}]", commandResult);
		}

		@SuppressWarnings("unchecked")
		Iterable<Document> resultSet = (Iterable<Document>) commandResult.get("retval");
		List<T> mappedResults = new ArrayList<T>();
		DocumentCallback<T> callback = new ReadDocumentCallback<T>(mongoConverter, entityClass, inputCollectionName);

		for (Document resultDocument : resultSet) {
			mappedResults.add(callback.doWith(resultDocument));
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
		return findAndRemove(query, null, collectionName);
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
		Document command = aggregation.toDocument(collectionName, rootContext);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing aggregation: {}", serializeToJsonSafely(command));
		}

		Document commandResult = executeCommand(command, this.readPreference);

		return new AggregationResults<O>(returnPotentiallyMappedResults(outputType, commandResult, collectionName),
				commandResult);
	}

	/**
	 * Returns the potentially mapped results of the given {@code commandResult}.
	 *
	 * @param outputType
	 * @param commandResult
	 * @return
	 */
	private <O> List<O> returnPotentiallyMappedResults(Class<O> outputType, Document commandResult,
			String collectionName) {

		@SuppressWarnings("unchecked")
		Iterable<Document> resultSet = (Iterable<Document>) commandResult.get("result");
		if (resultSet == null) {
			return Collections.emptyList();
		}

		DocumentCallback<O> callback = new UnwrapAndReadDocumentCallback<O>(mongoConverter, outputType, collectionName);

		List<O> mappedResults = new ArrayList<O>();
		for (Document document : resultSet) {
			mappedResults.add(callback.doWith(document));
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

	public Set<String> getCollectionNames() {
		return execute(new DbCallback<Set<String>>() {
			public Set<String> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				Set<String> result = new LinkedHashSet<String>();
				for (String name : db.listCollectionNames()) {
					result.add(name);
				}
				return result;
			}
		});
	}

	public MongoDatabase getDb() {
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
	protected MongoCollection<Document> doCreateCollection(final String collectionName,
			final Document collectionOptions) {
		return execute(new DbCallback<MongoCollection<Document>>() {
			public MongoCollection<Document> doInDB(MongoDatabase db) throws MongoException, DataAccessException {

				CreateCollectionOptions co = new CreateCollectionOptions();

				if (collectionOptions.containsKey("capped")) {
					co.capped((Boolean) collectionOptions.get("capped"));
				}
				if (collectionOptions.containsKey("size")) {
					co.sizeInBytes(((Number) collectionOptions.get("size")).longValue());
				}
				if (collectionOptions.containsKey("max")) {
					co.maxDocuments(((Number) collectionOptions.get("max")).longValue());
				}

				db.createCollection(collectionName, co);

				MongoCollection<Document> coll = db.getCollection(collectionName, Document.class);
				// TODO: Emit a collection created event
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Created collection [{}]", coll.getNamespace().getCollectionName());
				}
				return coll;
			}
		});
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The query document is specified as a standard {@link Document} and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param query the query document that specifies the criteria used to find a record.
	 * @param fields the document that specifies the fields to be returned.
	 * @param entityClass the parameterized type of the returned list.
	 * @return the {@link List} of converted objects.
	 */
	protected <T> T doFindOne(String collectionName, Document query, Document fields, Class<T> entityClass) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);
		Document mappedFields = fields == null ? null : queryMapper.getMappedObject(fields, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findOne using query: {} fields: {} for class: {} in collection: {}", serializeToJsonSafely(query),
					mappedFields, entityClass, collectionName);
		}

		return executeFindOneInternal(new FindOneCallback(mappedQuery, mappedFields),
				new ReadDocumentCallback<T>(this.mongoConverter, entityClass, collectionName), collectionName);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List using the template's converter. The
	 * query document is specified as a standard Document and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param fields the document that specifies the fields to be returned
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass) {
		return doFind(collectionName, query, fields, entityClass, null,
				new ReadDocumentCallback<T>(this.mongoConverter, entityClass, collectionName));
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type. The object is
	 * converted from the MongoDB native representation using an instance of {@see MongoConverter}. The query document is
	 * specified as a standard Document and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param query the query document that specifies the criteria used to find a record.
	 * @param fields the document that specifies the fields to be returned.
	 * @param entityClass the parameterized type of the returned list.
	 * @param preparer allows for customization of the {@link DBCursor} used when iterating over the result set, (apply
	 *          limits, skips and so on).
	 * @return the {@link List} of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass,
			CursorPreparer preparer) {
		return doFind(collectionName, query, fields, entityClass, preparer,
				new ReadDocumentCallback<T>(mongoConverter, entityClass, collectionName));
	}

	protected <S, T> List<T> doFind(String collectionName, Document query, Document fields, Class<S> entityClass,
			CursorPreparer preparer, DocumentCallback<T> objectCallback) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		Document mappedFields = queryMapper.getMappedFields(fields, entity);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: {} fields: {} for class: {} in collection: {}",
					serializeToJsonSafely(mappedQuery), mappedFields, entityClass, collectionName);
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields), preparer, objectCallback,
				collectionName);
	}

	protected Document convertToDocument(CollectionOptions collectionOptions) {
		Document document = new Document();
		if (collectionOptions != null) {
			if (collectionOptions.getCapped() != null) {
				document.put("capped", collectionOptions.getCapped().booleanValue());
			}
			if (collectionOptions.getSize() != null) {
				document.put("size", collectionOptions.getSize().intValue());
			}
			if (collectionOptions.getMaxDocuments() != null) {
				document.put("max", collectionOptions.getMaxDocuments().intValue());
			}
		}
		return document;
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The first document that matches the query is returned and also removed from the collection in the database.
	 * <p/>
	 * The query document is specified as a standard Document and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> T doFindAndRemove(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass) {

		EntityReader<? super T, Bson> readerToUse = this.mongoConverter;

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAndRemove using query: {} fields: {} sort: {} for class: {} in collection: {}",
					serializeToJsonSafely(query), fields, sort, entityClass, collectionName);
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		return executeFindOneInternal(new FindAndRemoveCallback(queryMapper.getMappedObject(query, entity), fields, sort),
				new ReadDocumentCallback<T>(readerToUse, entityClass, collectionName), collectionName);
	}

	protected <T> T doFindAndModify(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass, Update update, FindAndModifyOptions options) {

		EntityReader<? super T, Bson> readerToUse = this.mongoConverter;

		if (options == null) {
			options = new FindAndModifyOptions();
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		increaseVersionForUpdateIfNecessary(entity, update);

		Document mappedQuery = queryMapper.getMappedObject(query, entity);
		Document mappedUpdate = updateMapper.getMappedObject(update.getUpdateObject(), entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					"findAndModify using query: {} fields: {} sort: {} for class: {} and update: {} " + "in collection: {}",
					serializeToJsonSafely(mappedQuery), fields, sort, entityClass, serializeToJsonSafely(mappedUpdate),
					collectionName);
		}

		return executeFindOneInternal(new FindAndModifyCallback(mappedQuery, fields, sort, mappedUpdate, options),
				new ReadDocumentCallback<T>(readerToUse, entityClass, collectionName), collectionName);
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

		if (savedObject instanceof Document) {
			Document document = (Document) savedObject;
			document.put(ID_FIELD, id);
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

	private MongoCollection<Document> getAndPrepareCollection(MongoDatabase db, String collectionName) {
		try {
			MongoCollection<Document> collection = db.getCollection(collectionName, Document.class);
			collection = prepareCollection(collection);
			return collection;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/**
	 * Internal method using callbacks to do queries against the datastore that requires reading a single object from a
	 * collection of objects. It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ConnectionCallback} for a {@link Document}.</li>
	 * <li>Apply the given {@link DocumentCallback} to each of the {@link Document}s to obtain the result.</li>
	 * <ol>
	 *
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link Document} with
	 * @param objectCallback the {@link DocumentCallback} to transform {@link Document}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> T executeFindOneInternal(CollectionCallback<Document> collectionCallback,
										 DocumentCallback<T> objectCallback, String collectionName) {

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
	 * <li>Iterate over the {@link DBCursor} and applies the given {@link DocumentCallback} to each of the
	 * {@link Document}s collecting the actual result {@link List}.</li>
	 * <ol>
	 *
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link DBCursor} with
	 * @param preparer the {@link CursorPreparer} to potentially modify the {@link DBCursor} before ireating over it
	 * @param objectCallback the {@link DocumentCallback} to transform {@link Document}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> List<T> executeFindMultiInternal(CollectionCallback<FindIterable<Document>> collectionCallback,
												 CursorPreparer preparer, DocumentCallback<T> objectCallback, String collectionName) {

		try {

			MongoCursor<Document> cursor = null;

			try {

				FindIterable<Document> iterable = collectionCallback
						.doInCollection(getAndPrepareCollection(getDb(), collectionName));

				if (preparer != null) {
					iterable = preparer.prepare(iterable);
				}

				cursor = iterable.iterator();

				List<T> result = new ArrayList<T>();

				while (cursor.hasNext()) {
					Document object = cursor.next();
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

	private void executeQueryInternal(CollectionCallback<FindIterable<Document>> collectionCallback,
			CursorPreparer preparer, DocumentCallbackHandler callbackHandler, String collectionName) {

		try {

			MongoCursor<Document> cursor = null;

			try {
				FindIterable<Document> iterable = collectionCallback
						.doInCollection(getAndPrepareCollection(getDb(), collectionName));

				if (preparer != null) {
					iterable = preparer.prepare(iterable);
				}

				cursor = iterable.iterator();

				while (cursor.hasNext()) {
					callbackHandler.processDocument(cursor.next());
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
	protected void handleAnyWriteResultErrors(WriteResult writeResult, Document query, MongoActionOperation operation) {

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
	private void handleCommandError(CommandResult result, Document source) {

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

	private Document getMappedSortObject(Query query, Class<?> type) {

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
	static RuntimeException potentiallyConvertRuntimeException(RuntimeException ex,
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
	 * @return 
	 * TODO: Remove for 2.0 and change method signature of {@link #insertDBObjectList(String, List)}.
	 */
	private static List<Object> consolidateIdentifiers(List<ObjectId> ids, List<Document> documents) {

		List<Object> result = new ArrayList<Object>(ids.size());

		for (int i = 0; i < ids.size(); i++) {

			ObjectId objectId = ids.get(i);
			result.add(objectId == null ? documents.get(i).get(ID_FIELD) : objectId);
		}

		return result;
	}

	// Callback implementations

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 */
	private static class FindOneCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;

		public FindOneCallback(Document query, Document fields) {
			this.query = query;
			this.fields = fields;
		}

		public Document doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
			if (fields == null) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("findOne using query: {} in db.collection: {}", serializeToJsonSafely(query),
							collection.getNamespace().getFullName());
				}
				return collection.find(query).first();
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("findOne using query: {} fields: {} in db.collection: {}", serializeToJsonSafely(query), fields,
							collection.getNamespace().getFullName());
				}
				return collection.find(query).projection(fields).first();
			}
		}
	}

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 */
	private static class FindCallback implements CollectionCallback<FindIterable<Document>> {

		private final Document query;
		private final Document fields;

		public FindCallback(Document query) {
			this(query, null);
		}

		public FindCallback(Document query, Document fields) {
			this.query = query == null ? new Document() : query;
			this.fields = fields;
		}

		public FindIterable<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			if (fields == null || fields.isEmpty()) {
				return collection.find(query);
			} else {
				return collection.find(query).projection(fields);
			}
		}
	}

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Thomas Risberg
	 */
	private static class FindAndRemoveCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;

		public FindAndRemoveCallback(Document query, Document fields, Document sort) {
			this.query = query;
			this.fields = fields;
			this.sort = sort;
		}

		public Document doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

			FindOneAndDeleteOptions opts = new FindOneAndDeleteOptions();
			opts.sort(sort);
			opts.projection(fields);

			return collection.findOneAndDelete(query, opts);
		}
	}

	private static class FindAndModifyCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Document update;
		private final FindAndModifyOptions options;

		public FindAndModifyCallback(Document query, Document fields, Document sort, Document update,
				FindAndModifyOptions options) {
			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.options = options;
		}

		public Document doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

			FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
			opts.sort(sort);
			if (options.isUpsert()) {
				opts.upsert(true);
			}
			opts.projection(fields);
			if (options.returnNew) {
				opts.returnDocument(ReturnDocument.AFTER);
			}
			return collection.findOneAndUpdate(query, update, opts);
		}

	}

	/**
	 * Simple internal callback to allow operations on a {@link Document}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */

	interface DocumentCallback<T> {

		T doWith(Document object);
	}

	/**
	 * Simple {@link DocumentCallback} that will transform {@link Document} into the given target type using the given
	 * {@link MongoReader}.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 */
	private class ReadDocumentCallback<T> implements DocumentCallback<T> {

		private final EntityReader<? super T, Bson> reader;
		private final Class<T> type;
		private final String collectionName;

		public ReadDocumentCallback(EntityReader<? super T, Bson> reader, Class<T> type, String collectionName) {

			Assert.notNull(reader);
			Assert.notNull(type);
			this.reader = reader;
			this.type = type;
			this.collectionName = collectionName;
		}

		public T doWith(Document object) {
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

	class UnwrapAndReadDocumentCallback<T> extends ReadDocumentCallback<T> {

		public UnwrapAndReadDocumentCallback(EntityReader<? super T, Bson> reader, Class<T> type, String collectionName) {
			super(reader, type, collectionName);
		}

		@Override
		public T doWith(Document object) {

			Object idField = object.get(Fields.UNDERSCORE_ID);

			if (!(idField instanceof Document)) {
				return super.doWith(object);
			}

			Document toMap = new Document();
			Document nested = (Document) idField;
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
		public FindIterable<Document> prepare(FindIterable<Document> cursor) {

			if (query == null) {
				return cursor;
			}

			if (query.getSkip() <= 0 && query.getLimit() <= 0 && query.getSortObject() == null
					&& !StringUtils.hasText(query.getHint()) && !query.getMeta().hasValues()) {
				return cursor;
			}

			FindIterable<Document> cursorToUse = cursor;

			try {
				if (query.getSkip() > 0) {
					cursorToUse = cursorToUse.skip(query.getSkip());
				}
				if (query.getLimit() > 0) {
					cursorToUse = cursorToUse.limit(query.getLimit());
				}
				if (query.getSortObject() != null) {
					Document sort = type != null ? getMappedSortObject(query, type) : query.getSortObject();
					cursorToUse = cursorToUse.sort(sort);
				}

				Document meta = new Document();
				if (StringUtils.hasText(query.getHint())) {
					meta.put("$hint", query.getHint());
				}

				if (query.getMeta().hasValues()) {

					for (Entry<String, Object> entry : query.getMeta().values()) {
						meta.put(entry.getKey(), entry.getValue());
					}

					for (Meta.CursorOption option : query.getMeta().getFlags()) {

						switch (option) {

							case NO_TIMEOUT:
								cursorToUse = cursorToUse.noCursorTimeout(true);
								break;
							case PARTIAL:
								cursorToUse = cursorToUse.partial(true);
								break;
							default:
								throw new IllegalArgumentException(String.format("%s is no supported flag.", option));
						}
					}
				}

				cursorToUse = cursorToUse.modifiers(meta);

			} catch (RuntimeException e) {
				throw potentiallyConvertRuntimeException(e, exceptionTranslator);
			}

			return cursorToUse;
		}
	}

	/**
	 * {@link DocumentCallback} that assumes a {@link GeoResult} to be created, delegates actual content unmarshalling to
	 * a delegate and creates a {@link GeoResult} from the result.
	 *
	 * @author Oliver Gierke
	 */
	static class GeoNearResultDocumentCallback<T> implements DocumentCallback<GeoResult<T>> {

		private final DocumentCallback<T> delegate;
		private final Metric metric;

		/**
		 * Creates a new {@link GeoNearResultDocumentCallback} using the given {@link DocumentCallback} delegate for
		 * {@link GeoResult} content unmarshalling.
		 *
		 * @param delegate must not be {@literal null}.
		 */
		public GeoNearResultDocumentCallback(DocumentCallback<T> delegate, Metric metric) {
			Assert.notNull(delegate);
			this.delegate = delegate;
			this.metric = metric;
		}

		public GeoResult<T> doWith(Document object) {

			double distance = ((Double) object.get("dis")).doubleValue();
			Document content = (Document) object.get("obj");

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

		private volatile MongoCursor<Document> cursor;
		private PersistenceExceptionTranslator exceptionTranslator;
		private DocumentCallback<T> objectReadCallback;

		CloseableIterableCursorAdapter(MongoCursor<Document> cursor, PersistenceExceptionTranslator exceptionTranslator,
				DocumentCallback<T> objectReadCallback) {
			this.cursor = cursor;
			this.exceptionTranslator = exceptionTranslator;
			this.objectReadCallback = objectReadCallback;
		}

		/**
		 * Creates a new {@link CloseableIterableCursorAdapter} backed by the given {@link Cursor}.
		 *
		 * @param cursor
		 * @param exceptionTranslator
		 * @param objectReadCallback
		 */
		public CloseableIterableCursorAdapter(FindIterable<Document> cursor,
				PersistenceExceptionTranslator exceptionTranslator, DocumentCallback<T> objectReadCallback) {

			this.cursor = cursor.iterator();
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
				Document item = cursor.next();
				T converted = objectReadCallback.doWith(item);
				return converted;
			} catch (RuntimeException ex) {
				throw potentiallyConvertRuntimeException(ex, exceptionTranslator);
			}
		}

		@Override
		public void close() {

			MongoCursor<Document> c = cursor;
			try {
				c.close();
			} catch (RuntimeException ex) {
				throw potentiallyConvertRuntimeException(ex, exceptionTranslator);
			} finally {
				cursor = null;
				exceptionTranslator = null;
				objectReadCallback = null;
			}
		}
	}

	public Mongo getMongo() {
		return mongo;
	}

	public MongoDbFactory getMongoDbFactory() {
		return mongoDbFactory;
	}
}
