/*
 * Copyright 2010-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.springframework.data.mongodb.core.query.SerializationUtils.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.conversions.Bson;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.SessionSynchronization;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.DefaultBulkOperations.BulkOperationContext;
import org.springframework.data.mongodb.core.EntityOperations.AdaptibleEntity;
import org.springframework.data.mongodb.core.QueryOperations.AggregationDefinition;
import org.springframework.data.mongodb.core.QueryOperations.CountContext;
import org.springframework.data.mongodb.core.QueryOperations.DeleteContext;
import org.springframework.data.mongodb.core.QueryOperations.DistinctQueryContext;
import org.springframework.data.mongodb.core.QueryOperations.QueryContext;
import org.springframework.data.mongodb.core.QueryOperations.UpdateContext;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.JsonSchemaMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.MongoJsonSchemaMapper;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.IndexOperationsProvider;
import org.springframework.data.mongodb.core.index.MongoMappingEventPublisher;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.event.*;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.Meta.CursorOption;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.data.mongodb.core.timeseries.Granularity;
import org.springframework.data.mongodb.core.validation.Validator;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.util.CloseableIterator;
import org.springframework.data.util.Optionals;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

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
 * @author Maninder Singh
 * @author Borislav Rangelov
 * @author duozhilin
 * @author Andreas Zink
 * @author Cimon Lucas
 * @author Michael J. Simons
 * @author Roman Puchkovskiy
 * @author Yadhukrishna S Pai
 * @author Anton Barkan
 * @author Bartłomiej Mazur
 */
public class MongoTemplate implements MongoOperations, ApplicationContextAware, IndexOperationsProvider {

	private static final Log LOGGER = LogFactory.getLog(MongoTemplate.class);
	private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;

	private final MongoConverter mongoConverter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoDatabaseFactory mongoDbFactory;
	private final PersistenceExceptionTranslator exceptionTranslator;
	private final QueryMapper queryMapper;
	private final UpdateMapper updateMapper;
	private final JsonSchemaMapper schemaMapper;
	private final EntityOperations operations;
	private final PropertyOperations propertyOperations;
	private final QueryOperations queryOperations;

	private @Nullable WriteConcern writeConcern;
	private WriteConcernResolver writeConcernResolver = DefaultWriteConcernResolver.INSTANCE;
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;
	private @Nullable ReadPreference readPreference;
	private @Nullable ApplicationEventPublisher eventPublisher;
	private @Nullable EntityCallbacks entityCallbacks;
	private @Nullable ResourceLoader resourceLoader;
	private @Nullable MongoPersistentEntityIndexCreator indexCreator;

	private SessionSynchronization sessionSynchronization = SessionSynchronization.ON_ACTUAL_TRANSACTION;

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 * @since 2.1
	 */
	public MongoTemplate(MongoClient mongoClient, String databaseName) {
		this(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName), (MongoConverter) null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public MongoTemplate(MongoDatabaseFactory mongoDbFactory) {
		this(mongoDbFactory, (MongoConverter) null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param mongoConverter
	 */
	public MongoTemplate(MongoDatabaseFactory mongoDbFactory, @Nullable MongoConverter mongoConverter) {

		Assert.notNull(mongoDbFactory, "MongoDbFactory must not be null!");

		this.mongoDbFactory = mongoDbFactory;
		this.exceptionTranslator = mongoDbFactory.getExceptionTranslator();
		this.mongoConverter = mongoConverter == null ? getDefaultMongoConverter(mongoDbFactory) : mongoConverter;
		this.queryMapper = new QueryMapper(this.mongoConverter);
		this.updateMapper = new UpdateMapper(this.mongoConverter);
		this.schemaMapper = new MongoJsonSchemaMapper(this.mongoConverter);
		this.operations = new EntityOperations(this.mongoConverter);
		this.propertyOperations = new PropertyOperations(this.mongoConverter.getMappingContext());
		this.queryOperations = new QueryOperations(queryMapper, updateMapper, operations, propertyOperations,
				mongoDbFactory);

		// We always have a mapping context in the converter, whether it's a simple one or not
		mappingContext = this.mongoConverter.getMappingContext();
		// We create indexes based on mapping events
		if (mappingContext instanceof MongoMappingContext) {

			MongoMappingContext mappingContext = (MongoMappingContext) this.mappingContext;

			if (mappingContext.isAutoIndexCreation()) {

				indexCreator = new MongoPersistentEntityIndexCreator(mappingContext, this);
				eventPublisher = new MongoMappingEventPublisher(indexCreator);
				mappingContext.setApplicationEventPublisher(eventPublisher);
			}
		}
	}

	private MongoTemplate(MongoDatabaseFactory dbFactory, MongoTemplate that) {

		this.mongoDbFactory = dbFactory;
		this.exceptionTranslator = that.exceptionTranslator;
		this.sessionSynchronization = that.sessionSynchronization;

		// we need to (re)create the MappingMongoConverter as we need to have it use a DbRefResolver that operates within
		// the sames session. Otherwise loading referenced objects would happen outside of it.
		if (that.mongoConverter instanceof MappingMongoConverter) {
			this.mongoConverter = ((MappingMongoConverter) that.mongoConverter).with(dbFactory);
		} else {
			this.mongoConverter = that.mongoConverter;
		}

		this.queryMapper = that.queryMapper;
		this.updateMapper = that.updateMapper;
		this.schemaMapper = that.schemaMapper;
		this.mappingContext = that.mappingContext;
		this.operations = that.operations;
		this.propertyOperations = that.propertyOperations;
		this.queryOperations = that.queryOperations;
	}

	/**
	 * Configures the {@link WriteResultChecking} to be used with the template. Setting {@literal null} will reset the
	 * default of {@link #DEFAULT_WRITE_RESULT_CHECKING}.
	 *
	 * @param resultChecking
	 */
	public void setWriteResultChecking(@Nullable WriteResultChecking resultChecking) {
		this.writeResultChecking = resultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : resultChecking;
	}

	/**
	 * Configures the {@link WriteConcern} to be used with the template. If none is configured the {@link WriteConcern}
	 * configured on the {@link MongoDatabaseFactory} will apply.
	 *
	 * @param writeConcern
	 */
	public void setWriteConcern(@Nullable WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/**
	 * Configures the {@link WriteConcernResolver} to be used with the template.
	 *
	 * @param writeConcernResolver
	 */
	public void setWriteConcernResolver(@Nullable WriteConcernResolver writeConcernResolver) {
		this.writeConcernResolver = writeConcernResolver == null ? DefaultWriteConcernResolver.INSTANCE
				: writeConcernResolver;
	}

	/**
	 * Used by @{link {@link #prepareCollection(MongoCollection)} to set the {@link ReadPreference} before any operations
	 * are performed.
	 *
	 * @param readPreference
	 */
	public void setReadPreference(@Nullable ReadPreference readPreference) {
		this.readPreference = readPreference;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		prepareIndexCreator(applicationContext);

		eventPublisher = applicationContext;

		if (entityCallbacks == null) {
			setEntityCallbacks(EntityCallbacks.create(applicationContext));
		}

		if (mappingContext instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
		}

		resourceLoader = applicationContext;
	}

	/**
	 * Set the {@link EntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.EntityCallback callbacks} like the {@link BeforeSaveCallback}.
	 * <br />
	 * Overrides potentially existing {@link EntityCallbacks}.
	 *
	 * @param entityCallbacks must not be {@literal null}.
	 * @throws IllegalArgumentException if the given instance is {@literal null}.
	 * @since 2.2
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {

		Assert.notNull(entityCallbacks, "EntityCallbacks must not be null!");
		this.entityCallbacks = entityCallbacks;
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

		if (context instanceof ConfigurableApplicationContext && indexCreator != null) {
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

	@Override
	public <T> CloseableIterator<T> stream(Query query, Class<T> entityType) {
		return stream(query, entityType, getCollectionName(entityType));
	}

	@Override
	public <T> CloseableIterator<T> stream(Query query, Class<T> entityType, String collectionName) {
		return doStream(query, entityType, collectionName, entityType);
	}

	@SuppressWarnings("ConstantConditions")
	protected <T> CloseableIterator<T> doStream(Query query, Class<?> entityType, String collectionName,
			Class<T> returnType) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityType, "Entity type must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(returnType, "ReturnType must not be null!");

		return execute(collectionName, (CollectionCallback<CloseableIterator<T>>) collection -> {

			MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityType);

			QueryContext queryContext = queryOperations.createQueryContext(query);
			EntityProjection<T, ?> projection = operations.introspectProjection(returnType,
					entityType);

			Document mappedQuery = queryContext.getMappedQuery(persistentEntity);
			Document mappedFields = queryContext.getMappedFields(persistentEntity, projection);

			FindIterable<Document> cursor = new QueryCursorPreparer(query, entityType).initiateFind(collection,
					col -> col.find(mappedQuery, Document.class).projection(mappedFields));

			return new CloseableIterableCursorAdapter<>(cursor, exceptionTranslator,
					new ProjectingReadCallback<>(mongoConverter, projection, collectionName));
		});
	}

	@Override
	public String getCollectionName(Class<?> entityClass) {
		return this.operations.determineCollectionName(entityClass);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public Document executeCommand(String jsonCommand) {

		Assert.hasText(jsonCommand, "JsonCommand must not be null nor empty!");

		return execute(db -> db.runCommand(Document.parse(jsonCommand), Document.class));
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public Document executeCommand(Document command) {

		Assert.notNull(command, "Command must not be null!");

		return execute(db -> db.runCommand(command, Document.class));
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public Document executeCommand(Document command, @Nullable ReadPreference readPreference) {

		Assert.notNull(command, "Command must not be null!");

		return execute(db -> readPreference != null //
				? db.runCommand(command, readPreference, Document.class) //
				: db.runCommand(command, Document.class));
	}

	@Override
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
	 * @param documentCallbackHandler the handler that will extract results, one document at a time
	 * @param preparer allows for customization of the {@link FindIterable} used when iterating over the result set,
	 *          (apply limits, skips and so on).
	 */
	protected void executeQuery(Query query, String collectionName, DocumentCallbackHandler documentCallbackHandler,
			@Nullable CursorPreparer preparer) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(documentCallbackHandler, "DocumentCallbackHandler must not be null!");

		Document queryObject = queryMapper.getMappedObject(query.getQueryObject(), Optional.empty());
		Document sortObject = query.getSortObject();
		Document fieldsObject = query.getFieldsObject();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Executing query: %s sort: %s fields: %s in collection: %s",
					serializeToJsonSafely(queryObject), sortObject, fieldsObject, collectionName));
		}

		this.executeQueryInternal(new FindCallback(queryObject, fieldsObject, null),
				preparer != null ? preparer : CursorPreparer.NO_OP_PREPARER, documentCallbackHandler, collectionName);
	}

	public <T> T execute(DbCallback<T> action) {

		Assert.notNull(action, "DbCallback must not be null!");

		try {
			MongoDatabase db = prepareDatabase(this.doGetDatabase());
			return action.doInDB(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	public <T> T execute(Class<?> entityClass, CollectionCallback<T> callback) {

		Assert.notNull(entityClass, "EntityClass must not be null!");
		return execute(getCollectionName(entityClass), callback);
	}

	public <T> T execute(String collectionName, CollectionCallback<T> callback) {

		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(callback, "CollectionCallback must not be null!");

		try {
			MongoCollection<Document> collection = getAndPrepareCollection(doGetDatabase(), collectionName);
			return callback.doInCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	@Override
	public SessionScoped withSession(ClientSessionOptions options) {

		Assert.notNull(options, "ClientSessionOptions must not be null!");

		return withSession(() -> mongoDbFactory.getSession(options));
	}

	@Override
	public MongoTemplate withSession(ClientSession session) {

		Assert.notNull(session, "ClientSession must not be null!");

		return new SessionBoundMongoTemplate(session, MongoTemplate.this);
	}

	/**
	 * Define if {@link MongoTemplate} should participate in transactions. Default is set to
	 * {@link SessionSynchronization#ON_ACTUAL_TRANSACTION}.<br />
	 * <strong>NOTE:</strong> MongoDB transactions require at least MongoDB 4.0.
	 *
	 * @since 2.1
	 */
	public void setSessionSynchronization(SessionSynchronization sessionSynchronization) {
		this.sessionSynchronization = sessionSynchronization;
	}

	public <T> MongoCollection<Document> createCollection(Class<T> entityClass) {
		return createCollection(entityClass, operations.forType(entityClass).getCollectionOptions());
	}

	public <T> MongoCollection<Document> createCollection(Class<T> entityClass,
			@Nullable CollectionOptions collectionOptions) {

		Assert.notNull(entityClass, "EntityClass must not be null!");

		CollectionOptions options = collectionOptions != null ? collectionOptions : CollectionOptions.empty();
		options = Optionals
				.firstNonEmpty(() -> Optional.ofNullable(collectionOptions).flatMap(CollectionOptions::getCollation),
						() -> operations.forType(entityClass).getCollation()) //
				.map(options::collation).orElse(options);

		return doCreateCollection(getCollectionName(entityClass), convertToDocument(options, entityClass));
	}

	public MongoCollection<Document> createCollection(String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

		return doCreateCollection(collectionName, new Document());
	}

	public MongoCollection<Document> createCollection(String collectionName,
			@Nullable CollectionOptions collectionOptions) {

		Assert.notNull(collectionName, "CollectionName must not be null!");
		return doCreateCollection(collectionName, convertToDocument(collectionOptions));
	}

	@SuppressWarnings("ConstantConditions")
	public MongoCollection<Document> getCollection(String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

		return execute(db -> db.getCollection(collectionName, Document.class));
	}

	public <T> boolean collectionExists(Class<T> entityClass) {
		return collectionExists(getCollectionName(entityClass));
	}

	@SuppressWarnings("ConstantConditions")
	public boolean collectionExists(String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

		return execute(db -> {

			for (String name : db.listCollectionNames()) {
				if (name.equals(collectionName)) {
					return true;
				}
			}
			return false;
		});
	}

	public <T> void dropCollection(Class<T> entityClass) {
		dropCollection(getCollectionName(entityClass));
	}

	public void dropCollection(String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

		execute(collectionName, (CollectionCallback<Void>) collection -> {
			collection.drop();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Dropped collection [%s]",
						collection.getNamespace() != null ? collection.getNamespace().getCollectionName() : collectionName));
			}
			return null;
		});
	}

	@Override
	public IndexOperations indexOps(String collectionName) {
		return indexOps(collectionName, null);
	}

	public IndexOperations indexOps(String collectionName, @Nullable Class<?> type) {
		return new DefaultIndexOperations(this, collectionName, type);
	}

	public IndexOperations indexOps(Class<?> entityClass) {
		return indexOps(getCollectionName(entityClass), entityClass);
	}

	public BulkOperations bulkOps(BulkMode bulkMode, String collectionName) {
		return bulkOps(bulkMode, null, collectionName);
	}

	public BulkOperations bulkOps(BulkMode bulkMode, Class<?> entityClass) {
		return bulkOps(bulkMode, entityClass, getCollectionName(entityClass));
	}

	public BulkOperations bulkOps(BulkMode mode, @Nullable Class<?> entityType, String collectionName) {

		Assert.notNull(mode, "BulkMode must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		DefaultBulkOperations operations = new DefaultBulkOperations(this, collectionName,
				new BulkOperationContext(mode, Optional.ofNullable(getPersistentEntity(entityType)), queryMapper, updateMapper,
						eventPublisher, entityCallbacks));

		operations.setDefaultWriteConcern(writeConcern);

		return operations;
	}

	@Override
	public ScriptOperations scriptOps() {
		return new DefaultScriptOperations(this);
	}

	// Find methods that take a Query to express the query and that return a single object.

	@Nullable
	@Override
	public <T> T findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, getCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		if (ObjectUtils.isEmpty(query.getSortObject())) {

			return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(),
					new QueryCursorPreparer(query, entityClass), entityClass);
		} else {
			query.limit(1);
			List<T> results = find(query, entityClass, collectionName);
			return results.isEmpty() ? null : results.get(0);
		}
	}

	@Override
	public boolean exists(Query query, Class<?> entityClass) {
		return exists(query, entityClass, getCollectionName(entityClass));
	}

	@Override
	public boolean exists(Query query, String collectionName) {
		return exists(query, null, collectionName);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public boolean exists(Query query, @Nullable Class<?> entityClass, String collectionName) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to exist can't be null");
		}
		Assert.notNull(collectionName, "CollectionName must not be null!");

		QueryContext queryContext = queryOperations.createQueryContext(query);
		Document mappedQuery = queryContext.getMappedQuery(entityClass, this::getPersistentEntity);

		return execute(collectionName,
				new ExistsCallback(mappedQuery, queryContext.getCollation(entityClass).orElse(null)));
	}

	// Find methods that take a Query to express the query and that return a List of objects.

	@Override
	public <T> List<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, getCollectionName(entityClass));
	}

	@Override
	public <T> List<T> find(Query query, Class<T> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
				new QueryCursorPreparer(query, entityClass));
	}

	@Nullable
	@Override
	public <T> T findById(Object id, Class<T> entityClass) {
		return findById(id, entityClass, getCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findById(Object id, Class<T> entityClass, String collectionName) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		String idKey = operations.getIdPropertyName(entityClass);

		return doFindOne(collectionName, new Document(idKey, id), new Document(), entityClass);
	}

	@Override
	public <T> List<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass) {
		return findDistinct(query, field, getCollectionName(entityClass), entityClass, resultClass);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> findDistinct(Query query, String field, String collectionName, Class<?> entityClass,
			Class<T> resultClass) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(resultClass, "ResultClass must not be null!");

		MongoPersistentEntity<?> entity = entityClass != Object.class ? getPersistentEntity(entityClass) : null;
		DistinctQueryContext distinctQueryContext = queryOperations.distinctQueryContext(query, field);

		Document mappedQuery = distinctQueryContext.getMappedQuery(entity);
		String mappedFieldName = distinctQueryContext.getMappedFieldName(entity);
		Class<T> mongoDriverCompatibleType = distinctQueryContext.getDriverCompatibleClass(resultClass);

		MongoIterable<?> result = execute(collectionName, (collection) -> {

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Executing findDistinct using query %s for field: %s in collection: %s",
						serializeToJsonSafely(mappedQuery), field, collectionName));
			}

			QueryCursorPreparer preparer = new QueryCursorPreparer(query, entityClass);
			if (preparer.hasReadPreference()) {
				collection = collection.withReadPreference(preparer.getReadPreference());
			}

			DistinctIterable<T> iterable = collection.distinct(mappedFieldName, mappedQuery, mongoDriverCompatibleType);
			distinctQueryContext.applyCollation(entityClass, iterable::collation);

			return iterable;
		});

		if (resultClass == Object.class || mongoDriverCompatibleType != resultClass) {

			MongoConverter converter = getConverter();
			DefaultDbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoDbFactory);

			result = result.map((source) -> converter.mapValueToTargetType(source,
					distinctQueryContext.getMostSpecificConversionTargetType(resultClass, entityClass), dbRefResolver));
		}

		try {
			return (List<T>) result.into(new ArrayList<>());
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	@Override
	public <T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass) {
		return geoNear(near, entityClass, getCollectionName(entityClass));
	}

	@Override
	public <T> GeoResults<T> geoNear(NearQuery near, Class<T> domainType, String collectionName) {
		return geoNear(near, domainType, collectionName, domainType);
	}

	public <T> GeoResults<T> geoNear(NearQuery near, Class<?> domainType, String collectionName, Class<T> returnType) {

		if (near == null) {
			throw new InvalidDataAccessApiUsageException("NearQuery must not be null!");
		}

		if (domainType == null) {
			throw new InvalidDataAccessApiUsageException("Entity class must not be null!");
		}

		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(returnType, "ReturnType must not be null!");

		String collection = StringUtils.hasText(collectionName) ? collectionName : getCollectionName(domainType);
		String distanceField = operations.nearQueryDistanceFieldName(domainType);

		Aggregation $geoNear = TypedAggregation.newAggregation(domainType, Aggregation.geoNear(near, distanceField))
				.withOptions(AggregationOptions.builder().collation(near.getCollation()).build());

		AggregationResults<Document> results = aggregate($geoNear, collection, Document.class);
		EntityProjection<T, ?> projection = operations.introspectProjection(returnType,
				domainType);

		DocumentCallback<GeoResult<T>> callback = new GeoNearResultDocumentCallback<>(distanceField,
				new ProjectingReadCallback<>(mongoConverter, projection, collection), near.getMetric());

		List<GeoResult<T>> result = new ArrayList<>();

		BigDecimal aggregate = BigDecimal.ZERO;
		for (Document element : results) {

			GeoResult<T> geoResult = callback.doWith(element);
			aggregate = aggregate.add(BigDecimal.valueOf(geoResult.getDistance().getValue()));
			result.add(geoResult);
		}

		Distance avgDistance = new Distance(
				result.size() == 0 ? 0 : aggregate.divide(new BigDecimal(result.size()), RoundingMode.HALF_UP).doubleValue(),
				near.getMetric());

		return new GeoResults<>(result, avgDistance);
	}

	@Nullable
	@Override
	public <T> T findAndModify(Query query, UpdateDefinition update, Class<T> entityClass) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, getCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findAndModify(Query query, UpdateDefinition update, Class<T> entityClass, String collectionName) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, collectionName);
	}

	@Nullable
	@Override
	public <T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass) {
		return findAndModify(query, update, options, entityClass, getCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass,
			String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");
		Assert.notNull(options, "Options must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		FindAndModifyOptions optionsToUse = FindAndModifyOptions.of(options);

		Optionals.ifAllPresent(query.getCollation(), optionsToUse.getCollation(), (l, r) -> {
			throw new IllegalArgumentException(
					"Both Query and FindAndModifyOptions define a collation. Please provide the collation only via one of the two.");
		});

		if (!options.getCollation().isPresent()) {
			operations.forType(entityClass).getCollation(query).ifPresent(optionsToUse::collation);
		}

		return doFindAndModify(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass, update, optionsToUse);
	}

	@Override
	public <S, T> T findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType,
			String collectionName, Class<T> resultType) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(replacement, "Replacement must not be null!");
		Assert.notNull(options, "Options must not be null! Use FindAndReplaceOptions#empty() instead.");
		Assert.notNull(entityType, "EntityType must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(resultType, "ResultType must not be null! Use Object.class instead.");

		Assert.isTrue(query.getLimit() <= 1, "Query must not define a limit other than 1 ore none!");
		Assert.isTrue(query.getSkip() <= 0, "Query must not define skip.");

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityType);
		QueryContext queryContext = queryOperations.createQueryContext(query);

		EntityProjection<T, S> projection = operations.introspectProjection(resultType,
				entityType);
		Document mappedQuery = queryContext.getMappedQuery(entity);
		Document mappedFields = queryContext.getMappedFields(entity, projection);
		Document mappedSort = queryContext.getMappedSort(entity);

		replacement = maybeCallBeforeConvert(replacement, collectionName);
		Document mappedReplacement = operations.forEntity(replacement).toMappedDocument(this.mongoConverter).getDocument();

		maybeEmitEvent(new BeforeSaveEvent<>(replacement, mappedReplacement, collectionName));
		maybeCallBeforeSave(replacement, mappedReplacement, collectionName);

		T saved = doFindAndReplace(collectionName, mappedQuery, mappedFields, mappedSort,
				queryContext.getCollation(entityType).orElse(null), entityType, mappedReplacement, options,
				projection);

		if (saved != null) {
			maybeEmitEvent(new AfterSaveEvent<>(saved, mappedReplacement, collectionName));
			return maybeCallAfterSave(saved, mappedReplacement, collectionName);
		}

		return saved;
	}

	// Find methods that take a Query to express the query and that return a single object that is also removed from the
	// collection in the database.

	@Nullable
	@Override
	public <T> T findAndRemove(Query query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, getCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findAndRemove(Query query, Class<T> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), operations.forType(entityClass).getCollation(query).orElse(null),
				entityClass);
	}

	@Override
	public long count(Query query, Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null!");
		return count(query, entityClass, getCollectionName(entityClass));
	}

	@Override
	public long count(Query query, String collectionName) {
		return count(query, null, collectionName);
	}

	public long count(Query query, @Nullable Class<?> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		CountContext countContext = queryOperations.countQueryContext(query);

		CountOptions options = countContext.getCountOptions(entityClass);
		Document mappedQuery = countContext.getMappedQuery(entityClass, mappingContext::getPersistentEntity);

		return doCount(collectionName, mappedQuery, options);
	}

	@SuppressWarnings("ConstantConditions")
	protected long doCount(String collectionName, Document filter, CountOptions options) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER
					.debug(String.format("Executing count: %s in collection: %s", serializeToJsonSafely(filter), collectionName));
		}

		return execute(collectionName,
				collection -> collection.countDocuments(CountQuery.of(filter).toQueryDocument(), options));
	}

	@Override
	public long estimatedCount(String collectionName) {
		return doEstimatedCount(collectionName, new EstimatedDocumentCountOptions());
	}

	protected long doEstimatedCount(String collectionName, EstimatedDocumentCountOptions options) {
		return execute(collectionName, collection -> collection.estimatedDocumentCount(options));
	}

	@Override
	public <T> T insert(T objectToSave) {

		Assert.notNull(objectToSave, "ObjectToSave must not be null!");

		ensureNotIterable(objectToSave);
		return insert(objectToSave, getCollectionName(ClassUtils.getUserClass(objectToSave)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T insert(T objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "ObjectToSave must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		ensureNotIterable(objectToSave);
		return (T) doInsert(collectionName, objectToSave, this.mongoConverter);
	}

	/**
	 * Ensure the given {@literal source} is not an {@link java.lang.reflect.Array}, {@link Collection} or
	 * {@link Iterator}.
	 *
	 * @param source can be {@literal null}.
	 * @deprecated since 3.2. Call {@link #ensureNotCollectionLike(Object)} instead.
	 */
	protected void ensureNotIterable(@Nullable Object source) {
		ensureNotCollectionLike(source);
	}

	/**
	 * Ensure the given {@literal source} is not an {@link java.lang.reflect.Array}, {@link Collection} or
	 * {@link Iterator}.
	 *
	 * @param source can be {@literal null}.
	 * @since 3.2.
	 */
	protected void ensureNotCollectionLike(@Nullable Object source) {

		if (EntityOperations.isCollectionLike(source)) {
			throw new IllegalArgumentException("Cannot use a collection here.");
		}
	}

	/**
	 * Prepare the collection before any processing is done using it. This allows a convenient way to apply settings like
	 * withCodecRegistry() etc. Can be overridden in sub-classes.
	 *
	 * @param collection
	 */
	protected MongoCollection<Document> prepareCollection(MongoCollection<Document> collection) {

		if (this.readPreference != null) {
			collection = collection.withReadPreference(readPreference);
		}

		return collection;
	}

	/**
	 * Prepare the WriteConcern before any processing is done using it. This allows a convenient way to apply custom
	 * settings in sub-classes. <br />
	 * In case of using MongoDB Java driver version 3 the returned {@link WriteConcern} will be defaulted to
	 * {@link WriteConcern#ACKNOWLEDGED} when {@link WriteResultChecking} is set to {@link WriteResultChecking#EXCEPTION}.
	 *
	 * @param mongoAction any MongoAction already configured or null
	 * @return The prepared WriteConcern or null
	 */
	@Nullable
	protected WriteConcern prepareWriteConcern(MongoAction mongoAction) {

		WriteConcern wc = writeConcernResolver.resolve(mongoAction);
		return potentiallyForceAcknowledgedWrite(wc);
	}

	@Nullable
	private WriteConcern potentiallyForceAcknowledgedWrite(@Nullable WriteConcern wc) {

		if (ObjectUtils.nullSafeEquals(WriteResultChecking.EXCEPTION, writeResultChecking)) {
			if (wc == null || wc.getWObject() == null
					|| (wc.getWObject() instanceof Number && ((Number) wc.getWObject()).intValue() < 1)) {
				return WriteConcern.ACKNOWLEDGED;
			}
		}
		return wc;
	}

	protected <T> T doInsert(String collectionName, T objectToSave, MongoWriter<T> writer) {

		BeforeConvertEvent<T> event = new BeforeConvertEvent<>(objectToSave, collectionName);
		T toConvert = maybeEmitEvent(event).getSource();
		toConvert = maybeCallBeforeConvert(toConvert, collectionName);

		AdaptibleEntity<T> entity = operations.forEntity(toConvert, mongoConverter.getConversionService());
		entity.assertUpdateableIdIfNotSet();

		T initialized = entity.initializeVersionProperty();
		Document dbDoc = entity.toMappedDocument(writer).getDocument();

		maybeEmitEvent(new BeforeSaveEvent<>(initialized, dbDoc, collectionName));
		initialized = maybeCallBeforeSave(initialized, dbDoc, collectionName);
		Object id = insertDocument(collectionName, dbDoc, initialized.getClass());

		T saved = populateIdIfNecessary(initialized, id);
		maybeEmitEvent(new AfterSaveEvent<>(saved, dbDoc, collectionName));
		return maybeCallAfterSave(saved, dbDoc, collectionName);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Collection<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass) {

		Assert.notNull(batchToSave, "BatchToSave must not be null!");

		return (Collection<T>) doInsertBatch(getCollectionName(entityClass), batchToSave, this.mongoConverter);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Collection<T> insert(Collection<? extends T> batchToSave, String collectionName) {

		Assert.notNull(batchToSave, "BatchToSave must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		return (Collection<T>) doInsertBatch(collectionName, batchToSave, this.mongoConverter);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Collection<T> insertAll(Collection<? extends T> objectsToSave) {

		Assert.notNull(objectsToSave, "ObjectsToSave must not be null!");
		return (Collection<T>) doInsertAll(objectsToSave, this.mongoConverter);
	}

	@SuppressWarnings("unchecked")
	protected <T> Collection<T> doInsertAll(Collection<? extends T> listToSave, MongoWriter<T> writer) {

		Map<String, List<T>> elementsByCollection = new HashMap<>();
		List<T> savedObjects = new ArrayList<>(listToSave.size());

		for (T element : listToSave) {

			if (element == null) {
				continue;
			}

			String collection = getCollectionName(ClassUtils.getUserClass(element));
			List<T> collectionElements = elementsByCollection.get(collection);

			if (null == collectionElements) {
				collectionElements = new ArrayList<>();
				elementsByCollection.put(collection, collectionElements);
			}

			collectionElements.add(element);
		}

		for (Map.Entry<String, List<T>> entry : elementsByCollection.entrySet()) {
			savedObjects.addAll((Collection<T>) doInsertBatch(entry.getKey(), entry.getValue(), this.mongoConverter));
		}

		return savedObjects;
	}

	protected <T> Collection<T> doInsertBatch(String collectionName, Collection<? extends T> batchToSave,
			MongoWriter<T> writer) {

		Assert.notNull(writer, "MongoWriter must not be null!");

		List<Document> documentList = new ArrayList<>();
		List<T> initializedBatchToSave = new ArrayList<>(batchToSave.size());
		for (T uninitialized : batchToSave) {

			BeforeConvertEvent<T> event = new BeforeConvertEvent<>(uninitialized, collectionName);
			T toConvert = maybeEmitEvent(event).getSource();
			toConvert = maybeCallBeforeConvert(toConvert, collectionName);

			AdaptibleEntity<T> entity = operations.forEntity(toConvert, mongoConverter.getConversionService());
			entity.assertUpdateableIdIfNotSet();

			T initialized = entity.initializeVersionProperty();
			Document document = entity.toMappedDocument(writer).getDocument();
			maybeEmitEvent(new BeforeSaveEvent<>(initialized, document, collectionName));
			initialized = maybeCallBeforeSave(initialized, document, collectionName);

			documentList.add(document);
			initializedBatchToSave.add(initialized);
		}

		List<Object> ids = insertDocumentList(collectionName, documentList);
		List<T> savedObjects = new ArrayList<>(documentList.size());

		int i = 0;
		for (T obj : initializedBatchToSave) {

			if (i < ids.size()) {
				T saved = populateIdIfNecessary(obj, ids.get(i));
				Document doc = documentList.get(i);
				maybeEmitEvent(new AfterSaveEvent<>(saved, doc, collectionName));
				savedObjects.add(maybeCallAfterSave(saved, doc, collectionName));
			} else {
				savedObjects.add(obj);
			}
			i++;
		}

		return savedObjects;
	}

	@Override
	public <T> T save(T objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		return save(objectToSave, getCollectionName(ClassUtils.getUserClass(objectToSave)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T save(T objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		ensureNotCollectionLike(objectToSave);

		AdaptibleEntity<T> source = operations.forEntity(objectToSave, mongoConverter.getConversionService());

		return source.isVersionedEntity() //
				? doSaveVersioned(source, collectionName) //
				: (T) doSave(collectionName, objectToSave, this.mongoConverter);
	}

	@SuppressWarnings("unchecked")
	private <T> T doSaveVersioned(AdaptibleEntity<T> source, String collectionName) {

		if (source.isNew()) {
			return (T) doInsert(collectionName, source.getBean(), this.mongoConverter);
		}

		// Create query for entity with the id and old version
		Query query = source.getQueryForVersion();

		// Bump version number
		T toSave = source.incrementVersion();

		toSave = maybeEmitEvent(new BeforeConvertEvent<T>(toSave, collectionName)).getSource();
		toSave = maybeCallBeforeConvert(toSave, collectionName);

		if (source.getBean() != toSave) {
			source = operations.forEntity(toSave, mongoConverter.getConversionService());
		}

		source.assertUpdateableIdIfNotSet();

		MappedDocument mapped = source.toMappedDocument(mongoConverter);

		maybeEmitEvent(new BeforeSaveEvent<>(toSave, mapped.getDocument(), collectionName));
		toSave = maybeCallBeforeSave(toSave, mapped.getDocument(), collectionName);
		UpdateDefinition update = mapped.updateWithoutId();

		UpdateResult result = doUpdate(collectionName, query, update, toSave.getClass(), false, false);

		if (result.getModifiedCount() == 0) {

			throw new OptimisticLockingFailureException(
					String.format("Cannot save entity %s with version %s to collection %s. Has it been modified meanwhile?",
							source.getId(), source.getVersion(), collectionName));
		}
		maybeEmitEvent(new AfterSaveEvent<>(toSave, mapped.getDocument(), collectionName));

		return maybeCallAfterSave(toSave, mapped.getDocument(), collectionName);
	}

	protected <T> T doSave(String collectionName, T objectToSave, MongoWriter<T> writer) {

		objectToSave = maybeEmitEvent(new BeforeConvertEvent<>(objectToSave, collectionName)).getSource();
		objectToSave = maybeCallBeforeConvert(objectToSave, collectionName);

		AdaptibleEntity<T> entity = operations.forEntity(objectToSave, mongoConverter.getConversionService());
		entity.assertUpdateableIdIfNotSet();

		MappedDocument mapped = entity.toMappedDocument(writer);
		Document dbDoc = mapped.getDocument();

		maybeEmitEvent(new BeforeSaveEvent<>(objectToSave, dbDoc, collectionName));
		objectToSave = maybeCallBeforeSave(objectToSave, dbDoc, collectionName);
		Object id = saveDocument(collectionName, dbDoc, objectToSave.getClass());

		T saved = populateIdIfNecessary(objectToSave, id);
		maybeEmitEvent(new AfterSaveEvent<>(saved, dbDoc, collectionName));

		return maybeCallAfterSave(saved, dbDoc, collectionName);
	}

	@SuppressWarnings("ConstantConditions")
	protected Object insertDocument(String collectionName, Document document, Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Inserting Document containing fields: %s in collection: %s", document.keySet(),
					collectionName));
		}

		return execute(collectionName, collection -> {
			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT, collectionName, entityClass,
					document, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

			if (writeConcernToUse == null) {
				collection.insertOne(document);
			} else {
				collection.withWriteConcern(writeConcernToUse).insertOne(document);
			}

			return operations.forEntity(document).getId();
		});
	}

	protected List<Object> insertDocumentList(String collectionName, List<Document> documents) {

		if (documents.isEmpty()) {
			return Collections.emptyList();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Inserting list of Documents containing %s items", documents.size()));
		}

		execute(collectionName, collection -> {

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT_LIST, collectionName, null,
					null, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

			if (writeConcernToUse == null) {
				collection.insertMany(documents);
			} else {
				collection.withWriteConcern(writeConcernToUse).insertMany(documents);
			}

			return null;
		});

		return MappedDocument.toIds(documents);
	}

	protected Object saveDocument(String collectionName, Document dbDoc, Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Saving Document containing fields: %s", dbDoc.keySet()));
		}

		return execute(collectionName, collection -> {

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.SAVE, collectionName, entityClass,
					dbDoc, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

			MappedDocument mapped = MappedDocument.of(dbDoc);

			MongoCollection<Document> collectionToUse = writeConcernToUse == null //
					? collection //
					: collection.withWriteConcern(writeConcernToUse);

			if (!mapped.hasId()) {
				collectionToUse.insertOne(dbDoc);
			} else {

				MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
				UpdateContext updateContext = queryOperations.replaceSingleContext(mapped, true);
				Document replacement = updateContext.getMappedUpdate(entity);

				Document filter = updateContext.getMappedQuery(entity);

				if (updateContext.requiresShardKey(filter, entity)) {

					if (entity.getShardKey().isImmutable()) {
						filter = updateContext.applyShardKey(entity, filter, null);
					} else {
						filter = updateContext.applyShardKey(entity, filter,
								collection.find(filter, Document.class).projection(updateContext.getMappedShardKey(entity)).first());
					}
				}

				collectionToUse.replaceOne(filter, replacement, new ReplaceOptions().upsert(true));
			}
			return mapped.getId();
		});
	}

	@Override
	public UpdateResult upsert(Query query, UpdateDefinition update, Class<?> entityClass) {
		return doUpdate(getCollectionName(entityClass), query, update, entityClass, true, false);
	}

	@Override
	public UpdateResult upsert(Query query, UpdateDefinition update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	@Override
	public UpdateResult upsert(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {

		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, true, false);
	}

	@Override
	public UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> entityClass) {
		return doUpdate(getCollectionName(entityClass), query, update, entityClass, false, false);
	}

	@Override
	public UpdateResult updateFirst(Query query, UpdateDefinition update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	@Override
	public UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {

		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	@Override
	public UpdateResult updateMulti(Query query, UpdateDefinition update, Class<?> entityClass) {
		return doUpdate(getCollectionName(entityClass), query, update, entityClass, false, true);
	}

	@Override
	public UpdateResult updateMulti(Query query, UpdateDefinition update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	@Override
	public UpdateResult updateMulti(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {

		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	@SuppressWarnings("ConstantConditions")
	protected UpdateResult doUpdate(String collectionName, Query query, UpdateDefinition update,
			@Nullable Class<?> entityClass, boolean upsert, boolean multi) {

		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		if (query.isSorted() && LOGGER.isWarnEnabled()) {

			LOGGER.warn(String.format("%s does not support sort ('%s'). Please use findAndModify() instead.",
					upsert ? "Upsert" : "UpdateFirst", serializeToJsonSafely(query.getSortObject())));
		}

		MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);

		UpdateContext updateContext = multi ? queryOperations.updateContext(update, query, upsert)
				: queryOperations.updateSingleContext(update, query, upsert);
		updateContext.increaseVersionForUpdateIfNecessary(entity);

		Document queryObj = updateContext.getMappedQuery(entity);
		UpdateOptions opts = updateContext.getUpdateOptions(entityClass);

		if (updateContext.isAggregationUpdate()) {

			List<Document> pipeline = updateContext.getUpdatePipeline(entityClass);
			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName, entityClass,
					update.getUpdateObject(), queryObj);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

			return execute(collectionName, collection -> {

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("Calling update using query: %s and update: %s in collection: %s",
							serializeToJsonSafely(queryObj), serializeToJsonSafely(pipeline), collectionName));
				}

				collection = writeConcernToUse != null ? collection.withWriteConcern(writeConcernToUse) : collection;

				return multi ? collection.updateMany(queryObj, pipeline, opts) : collection.updateOne(queryObj, pipeline, opts);
			});
		}

		Document updateObj = updateContext.getMappedUpdate(entity);
		MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName, entityClass,
				updateObj, queryObj);
		WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

		return execute(collectionName, collection -> {

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Calling update using query: %s and update: %s in collection: %s",
						serializeToJsonSafely(queryObj), serializeToJsonSafely(updateObj), collectionName));
			}

			collection = writeConcernToUse != null ? collection.withWriteConcern(writeConcernToUse) : collection;

			if (!UpdateMapper.isUpdateObject(updateObj)) {

				Document filter = new Document(queryObj);

				if (updateContext.requiresShardKey(filter, entity)) {

					if (entity.getShardKey().isImmutable()) {
						filter = updateContext.applyShardKey(entity, filter, null);
					} else {
						filter = updateContext.applyShardKey(entity, filter,
								collection.find(filter, Document.class).projection(updateContext.getMappedShardKey(entity)).first());
					}
				}

				ReplaceOptions replaceOptions = updateContext.getReplaceOptions(entityClass);
				return collection.replaceOne(filter, updateObj, replaceOptions);
			} else {
				return multi ? collection.updateMany(queryObj, updateObj, opts)
						: collection.updateOne(queryObj, updateObj, opts);
			}
		});
	}

	@Override
	public DeleteResult remove(Object object) {

		Assert.notNull(object, "Object must not be null!");

		return remove(object, getCollectionName(object.getClass()));
	}

	@Override
	public DeleteResult remove(Object object, String collectionName) {

		Assert.notNull(object, "Object must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		Query query = operations.forEntity(object).getRemoveByQuery();

		return doRemove(collectionName, query, object.getClass(), false);
	}

	@Override
	public DeleteResult remove(Query query, String collectionName) {
		return doRemove(collectionName, query, null, true);
	}

	@Override
	public DeleteResult remove(Query query, Class<?> entityClass) {
		return remove(query, entityClass, getCollectionName(entityClass));
	}

	@Override
	public DeleteResult remove(Query query, Class<?> entityClass, String collectionName) {

		Assert.notNull(entityClass, "EntityClass must not be null!");
		return doRemove(collectionName, query, entityClass, true);
	}

	@SuppressWarnings("ConstantConditions")
	protected <T> DeleteResult doRemove(String collectionName, Query query, @Nullable Class<T> entityClass,
			boolean multi) {

		Assert.notNull(query, "Query must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);

		DeleteContext deleteContext = multi ? queryOperations.deleteQueryContext(query)
				: queryOperations.deleteSingleContext(query);
		Document queryObject = deleteContext.getMappedQuery(entity);
		DeleteOptions options = deleteContext.getDeleteOptions(entityClass);

		MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName, entityClass,
				null, queryObject);

		WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

		return execute(collectionName, collection -> {

			maybeEmitEvent(new BeforeDeleteEvent<>(queryObject, entityClass, collectionName));

			Document removeQuery = queryObject;

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Remove using query: %s in collection: %s.", serializeToJsonSafely(removeQuery),
						collectionName));
			}

			if (query.getLimit() > 0 || query.getSkip() > 0) {

				MongoCursor<Document> cursor = new QueryCursorPreparer(query, entityClass)
						.prepare(collection.find(removeQuery).projection(MappedDocument.getIdOnlyProjection())) //
						.iterator();

				Set<Object> ids = new LinkedHashSet<>();
				while (cursor.hasNext()) {
					ids.add(MappedDocument.of(cursor.next()).getId());
				}

				removeQuery = MappedDocument.getIdIn(ids);
			}

			MongoCollection<Document> collectionToUse = writeConcernToUse != null
					? collection.withWriteConcern(writeConcernToUse)
					: collection;

			DeleteResult result = multi ? collectionToUse.deleteMany(removeQuery, options)
					: collectionToUse.deleteOne(removeQuery, options);

			maybeEmitEvent(new AfterDeleteEvent<>(queryObject, entityClass, collectionName));

			return result;
		});
	}

	@Override
	public <T> List<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, getCollectionName(entityClass));
	}

	@Override
	public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(
				new FindCallback(new Document(), new Document(),
						operations.forType(entityClass).getCollation().map(Collation::toMongoCollation).orElse(null)),
				CursorPreparer.NO_OP_PREPARER, new ReadDocumentCallback<>(mongoConverter, entityClass, collectionName),
				collectionName);
	}

	@Override
	public <T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			Class<T> entityClass) {
		return mapReduce(new Query(), inputCollectionName, mapFunction, reduceFunction,
				new MapReduceOptions().outputTypeInline(), entityClass);
	}

	@Override
	public <T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			@Nullable MapReduceOptions mapReduceOptions, Class<T> entityClass) {
		return mapReduce(new Query(), inputCollectionName, mapFunction, reduceFunction, mapReduceOptions, entityClass);
	}

	@Override
	public <T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction,
			String reduceFunction, Class<T> entityClass) {
		return mapReduce(query, inputCollectionName, mapFunction, reduceFunction, new MapReduceOptions().outputTypeInline(),
				entityClass);
	}

	@Override
	public <T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction,
			String reduceFunction, @Nullable MapReduceOptions mapReduceOptions, Class<T> entityClass) {

		return new MapReduceResults<>(
				mapReduce(query, entityClass, inputCollectionName, mapFunction, reduceFunction, mapReduceOptions, entityClass),
				new Document());
	}

	/**
	 * @param query
	 * @param domainType
	 * @param inputCollectionName
	 * @param mapFunction
	 * @param reduceFunction
	 * @param mapReduceOptions
	 * @param resultType
	 * @return
	 * @since 2.1
	 */
	public <T> List<T> mapReduce(Query query, Class<?> domainType, String inputCollectionName, String mapFunction,
			String reduceFunction, @Nullable MapReduceOptions mapReduceOptions, Class<T> resultType) {

		Assert.notNull(domainType, "Domain type must not be null!");
		Assert.notNull(inputCollectionName, "Input collection name must not be null!");
		Assert.notNull(resultType, "Result type must not be null!");
		Assert.notNull(mapFunction, "Map function must not be null!");
		Assert.notNull(reduceFunction, "Reduce function must not be null!");

		String mapFunc = replaceWithResourceIfNecessary(mapFunction);
		String reduceFunc = replaceWithResourceIfNecessary(reduceFunction);
		MongoCollection<Document> inputCollection = getAndPrepareCollection(doGetDatabase(), inputCollectionName);

		// MapReduceOp
		MapReduceIterable<Document> mapReduce = inputCollection.mapReduce(mapFunc, reduceFunc, Document.class);

		if (query.getLimit() > 0 && mapReduceOptions != null && mapReduceOptions.getLimit() == null) {
			mapReduce = mapReduce.limit(query.getLimit());
		}
		if (query.getMeta().getMaxTimeMsec() != null) {
			mapReduce = mapReduce.maxTime(query.getMeta().getMaxTimeMsec(), TimeUnit.MILLISECONDS);
		}

		Document mappedSort = getMappedSortObject(query, domainType);
		if (mappedSort != null && !mappedSort.isEmpty()) {
			mapReduce = mapReduce.sort(getMappedSortObject(query, domainType));
		}

		mapReduce = mapReduce
				.filter(queryMapper.getMappedObject(query.getQueryObject(), mappingContext.getPersistentEntity(domainType)));

		Optional<Collation> collation = query.getCollation();

		if (mapReduceOptions != null) {

			Optionals.ifAllPresent(collation, mapReduceOptions.getCollation(), (l, r) -> {
				throw new IllegalArgumentException(
						"Both Query and MapReduceOptions define a collation. Please provide the collation only via one of the two.");
			});

			if (mapReduceOptions.getCollation().isPresent()) {
				collation = mapReduceOptions.getCollation();
			}

			if (!CollectionUtils.isEmpty(mapReduceOptions.getScopeVariables())) {
				mapReduce = mapReduce.scope(new Document(mapReduceOptions.getScopeVariables()));
			}

			if (mapReduceOptions.getLimit() != null && mapReduceOptions.getLimit() > 0) {
				mapReduce = mapReduce.limit(mapReduceOptions.getLimit());
			}

			if (mapReduceOptions.getFinalizeFunction().filter(StringUtils::hasText).isPresent()) {
				mapReduce = mapReduce.finalizeFunction(mapReduceOptions.getFinalizeFunction().get());
			}

			if (mapReduceOptions.getJavaScriptMode() != null) {
				mapReduce = mapReduce.jsMode(mapReduceOptions.getJavaScriptMode());
			}

			if (mapReduceOptions.getOutputSharded().isPresent()) {
				mapReduce = mapReduce.sharded(mapReduceOptions.getOutputSharded().get());
			}

			if (StringUtils.hasText(mapReduceOptions.getOutputCollection()) && !mapReduceOptions.usesInlineOutput()) {

				mapReduce = mapReduce.collectionName(mapReduceOptions.getOutputCollection())
						.action(mapReduceOptions.getMapReduceAction());

				if (mapReduceOptions.getOutputDatabase().isPresent()) {
					mapReduce = mapReduce.databaseName(mapReduceOptions.getOutputDatabase().get());
				}
			}
		}

		if (!collation.isPresent()) {
			collation = operations.forType(domainType).getCollation();
		}

		mapReduce = collation.map(Collation::toMongoCollation).map(mapReduce::collation).orElse(mapReduce);

		List<T> mappedResults = new ArrayList<>();
		DocumentCallback<T> callback = new ReadDocumentCallback<>(mongoConverter, resultType, inputCollectionName);

		for (Document document : mapReduce) {
			mappedResults.add(callback.doWith(document));
		}

		return mappedResults;
	}

	public <T> GroupByResults<T> group(String inputCollectionName, GroupBy groupBy, Class<T> entityClass) {
		return group(null, inputCollectionName, groupBy, entityClass);
	}

	public <T> GroupByResults<T> group(@Nullable Criteria criteria, String inputCollectionName, GroupBy groupBy,
			Class<T> entityClass) {

		Document document = groupBy.getGroupByObject();
		document.put("ns", inputCollectionName);

		if (criteria == null) {
			document.put("cond", null);
		} else {
			document.put("cond", queryMapper.getMappedObject(criteria.getCriteriaObject(), Optional.empty()));
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
			document.put("$reduce", replaceWithResourceIfNecessary(ObjectUtils.nullSafeToString(document.get("$reduce"))));
		}
		if (document.containsKey("$keyf")) {
			document.put("$keyf", replaceWithResourceIfNecessary(ObjectUtils.nullSafeToString(document.get("$keyf"))));
		}
		if (document.containsKey("finalize")) {
			document.put("finalize", replaceWithResourceIfNecessary(ObjectUtils.nullSafeToString(document.get("finalize"))));
		}

		Document commandObject = new Document("group", document);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Executing Group with Document [%s]", serializeToJsonSafely(commandObject)));
		}

		Document commandResult = executeCommand(commandObject, this.readPreference);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Group command result = [%s]", commandResult));
		}

		@SuppressWarnings("unchecked")
		Iterable<Document> resultSet = (Iterable<Document>) commandResult.get("retval");
		List<T> mappedResults = new ArrayList<>();
		DocumentCallback<T> callback = new ReadDocumentCallback<>(mongoConverter, entityClass, inputCollectionName);

		for (Document resultDocument : resultSet) {
			mappedResults.add(callback.doWith(resultDocument));
		}

		return new GroupByResults<>(mappedResults, commandResult);
	}

	@Override
	public <O> AggregationResults<O> aggregate(TypedAggregation<?> aggregation, Class<O> outputType) {
		return aggregate(aggregation, getCollectionName(aggregation.getInputType()), outputType);
	}

	@Override
	public <O> AggregationResults<O> aggregate(TypedAggregation<?> aggregation, String inputCollectionName,
			Class<O> outputType) {

		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");

		return aggregate(aggregation, inputCollectionName, outputType, null);
	}

	@Override
	public <O> AggregationResults<O> aggregate(Aggregation aggregation, Class<?> inputType, Class<O> outputType) {

		return aggregate(aggregation, getCollectionName(inputType), outputType,
				queryOperations.createAggregation(aggregation, inputType).getAggregationOperationContext());
	}

	@Override
	public <O> AggregationResults<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType) {
		return aggregate(aggregation, collectionName, outputType, null);
	}

	@Override
	public <O> CloseableIterator<O> aggregateStream(TypedAggregation<?> aggregation, String inputCollectionName,
			Class<O> outputType) {

		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");

		AggregationOperationContext context = new TypeBasedAggregationOperationContext(aggregation.getInputType(),
				mappingContext, queryMapper);
		return aggregateStream(aggregation, inputCollectionName, outputType, context);
	}

	@Override
	public <O> CloseableIterator<O> aggregateStream(TypedAggregation<?> aggregation, Class<O> outputType) {
		return aggregateStream(aggregation, getCollectionName(aggregation.getInputType()), outputType);
	}

	@Override
	public <O> CloseableIterator<O> aggregateStream(Aggregation aggregation, Class<?> inputType, Class<O> outputType) {

		return aggregateStream(aggregation, getCollectionName(inputType), outputType,
				new TypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper));
	}

	@Override
	public <O> CloseableIterator<O> aggregateStream(Aggregation aggregation, String collectionName, Class<O> outputType) {
		return aggregateStream(aggregation, collectionName, outputType, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> findAllAndRemove(Query query, String collectionName) {
		return (List<T>) findAllAndRemove(query, Object.class, collectionName);
	}

	@Override
	public <T> List<T> findAllAndRemove(Query query, Class<T> entityClass) {
		return findAllAndRemove(query, entityClass, getCollectionName(entityClass));
	}

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

			Query byIdInQuery = operations.getByIdInQuery(result);

			remove(byIdInQuery, entityClass, collectionName);
		}

		return result;
	}

	protected <O> AggregationResults<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType,
			@Nullable AggregationOperationContext context) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");
		Assert.notNull(outputType, "Output type must not be null!");

		return doAggregate(aggregation, collectionName, outputType,
				queryOperations.createAggregation(aggregation, context));
	}

	private <O> AggregationResults<O> doAggregate(Aggregation aggregation, String collectionName, Class<O> outputType,
			AggregationDefinition context) {
		return doAggregate(aggregation, collectionName, outputType, context.getAggregationOperationContext());
	}

	@SuppressWarnings("ConstantConditions")
	protected <O> AggregationResults<O> doAggregate(Aggregation aggregation, String collectionName, Class<O> outputType,
			AggregationOperationContext context) {

		ReadDocumentCallback<O> callback = new ReadDocumentCallback<>(mongoConverter, outputType, collectionName);

		AggregationOptions options = aggregation.getOptions();
		AggregationUtil aggregationUtil = new AggregationUtil(queryMapper, mappingContext);

		if (options.isExplain()) {

			Document command = aggregationUtil.createCommand(collectionName, aggregation, context);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Executing aggregation: %s", serializeToJsonSafely(command)));
			}

			Document commandResult = executeCommand(command);
			return new AggregationResults<>(commandResult.get("results", new ArrayList<Document>(0)).stream()
					.map(callback::doWith).collect(Collectors.toList()), commandResult);
		}

		List<Document> pipeline = aggregationUtil.createPipeline(aggregation, context);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					String.format("Executing aggregation: %s in collection %s", serializeToJsonSafely(pipeline), collectionName));
		}

		return execute(collectionName, collection -> {

			List<Document> rawResult = new ArrayList<>();

			Class<?> domainType = aggregation instanceof TypedAggregation ? ((TypedAggregation<?>) aggregation).getInputType()
					: null;

			Optional<Collation> collation = Optionals.firstNonEmpty(options::getCollation,
					() -> operations.forType(domainType) //
							.getCollation());

			AggregateIterable<Document> aggregateIterable = collection.aggregate(pipeline, Document.class) //
					.collation(collation.map(Collation::toMongoCollation).orElse(null)) //
					.allowDiskUse(options.isAllowDiskUse());

			if (options.getCursorBatchSize() != null) {
				aggregateIterable = aggregateIterable.batchSize(options.getCursorBatchSize());
			}

			options.getComment().ifPresent(aggregateIterable::comment);
			options.getHint().ifPresent(aggregateIterable::hint);

			if (options.hasExecutionTimeLimit()) {
				aggregateIterable = aggregateIterable.maxTime(options.getMaxTime().toMillis(), TimeUnit.MILLISECONDS);
			}

			if (options.isSkipResults()) {

				// toCollection only allowed for $out and $merge if those are the last stages
				if (aggregation.getPipeline().isOutOrMerge()) {
					aggregateIterable.toCollection();
				} else {
					aggregateIterable.first();
				}
				return new AggregationResults<>(Collections.emptyList(), new Document());
			}

			MongoIterable<O> iterable = aggregateIterable.map(val -> {

				rawResult.add(val);
				return callback.doWith(val);
			});

			return new AggregationResults<>(iterable.into(new ArrayList<>()),
					new Document("results", rawResult).append("ok", 1.0D));
		});
	}

	@SuppressWarnings("ConstantConditions")
	protected <O> CloseableIterator<O> aggregateStream(Aggregation aggregation, String collectionName,
			Class<O> outputType, @Nullable AggregationOperationContext context) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");
		Assert.notNull(outputType, "Output type must not be null!");
		Assert.isTrue(!aggregation.getOptions().isExplain(), "Can't use explain option with streaming!");

		AggregationDefinition aggregationDefinition = queryOperations.createAggregation(aggregation, context);

		AggregationOptions options = aggregation.getOptions();
		List<Document> pipeline = aggregationDefinition.getAggregationPipeline();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					String.format("Streaming aggregation: %s in collection %s", serializeToJsonSafely(pipeline), collectionName));
		}

		ReadDocumentCallback<O> readCallback = new ReadDocumentCallback<>(mongoConverter, outputType, collectionName);

		return execute(collectionName, (CollectionCallback<CloseableIterator<O>>) collection -> {

			AggregateIterable<Document> cursor = collection.aggregate(pipeline, Document.class) //
					.allowDiskUse(options.isAllowDiskUse());

			if (options.getCursorBatchSize() != null) {
				cursor = cursor.batchSize(options.getCursorBatchSize());
			}

			options.getComment().ifPresent(cursor::comment);
			options.getHint().ifPresent(cursor::hint);

			Class<?> domainType = aggregation instanceof TypedAggregation ? ((TypedAggregation) aggregation).getInputType()
					: null;

			Optionals.firstNonEmpty(options::getCollation, //
					() -> operations.forType(domainType).getCollation()) //
					.map(Collation::toMongoCollation) //
					.ifPresent(cursor::collation);

			return new CloseableIterableCursorAdapter<>(cursor, exceptionTranslator, readCallback);
		});
	}

	@Override
	public <T> ExecutableFind<T> query(Class<T> domainType) {
		return new ExecutableFindOperationSupport(this).query(domainType);
	}

	@Override
	public <T> ExecutableUpdate<T> update(Class<T> domainType) {
		return new ExecutableUpdateOperationSupport(this).update(domainType);
	}

	@Override
	public <T> ExecutableRemove<T> remove(Class<T> domainType) {
		return new ExecutableRemoveOperationSupport(this).remove(domainType);
	}

	@Override
	public <T> ExecutableAggregation<T> aggregateAndReturn(Class<T> domainType) {
		return new ExecutableAggregationOperationSupport(this).aggregateAndReturn(domainType);
	}

	@Override
	public <T> ExecutableMapReduce<T> mapReduce(Class<T> domainType) {
		return new ExecutableMapReduceOperationSupport(this).mapReduce(domainType);
	}

	@Override
	public <T> ExecutableInsert<T> insert(Class<T> domainType) {
		return new ExecutableInsertOperationSupport(this).insert(domainType);
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

	@SuppressWarnings("ConstantConditions")
	public Set<String> getCollectionNames() {
		return execute(db -> {
			Set<String> result = new LinkedHashSet<>();
			for (String name : db.listCollectionNames()) {
				result.add(name);
			}
			return result;
		});
	}

	public MongoDatabase getDb() {
		return doGetDatabase();
	}

	protected MongoDatabase doGetDatabase() {
		return MongoDatabaseUtils.getDatabase(mongoDbFactory, sessionSynchronization);
	}

	protected MongoDatabase prepareDatabase(MongoDatabase database) {
		return database;
	}

	protected <E extends MongoMappingEvent<T>, T> E maybeEmitEvent(E event) {

		if (eventPublisher != null) {
			eventPublisher.publishEvent(event);
		}

		return event;
	}

	protected <T> T maybeCallBeforeConvert(T object, String collection) {

		if (entityCallbacks != null) {
			return entityCallbacks.callback(BeforeConvertCallback.class, object, collection);
		}

		return object;
	}

	protected <T> T maybeCallBeforeSave(T object, Document document, String collection) {

		if (entityCallbacks != null) {
			return entityCallbacks.callback(BeforeSaveCallback.class, object, document, collection);
		}

		return object;
	}

	protected <T> T maybeCallAfterSave(T object, Document document, String collection) {

		if (entityCallbacks != null) {
			return entityCallbacks.callback(AfterSaveCallback.class, object, document, collection);
		}

		return object;
	}

	protected <T> T maybeCallAfterConvert(T object, Document document, String collection) {

		if (entityCallbacks != null) {
			return entityCallbacks.callback(AfterConvertCallback.class, object, document, collection);
		}

		return object;
	}

	/**
	 * Create the specified collection using the provided options
	 *
	 * @param collectionName
	 * @param collectionOptions
	 * @return the collection that was created
	 */
	@SuppressWarnings("ConstantConditions")
	protected MongoCollection<Document> doCreateCollection(String collectionName, Document collectionOptions) {
		return execute(db -> {

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

			if (collectionOptions.containsKey("collation")) {
				co.collation(IndexConverters.fromDocument(collectionOptions.get("collation", Document.class)));
			}

			if (collectionOptions.containsKey("validator")) {

				com.mongodb.client.model.ValidationOptions options = new com.mongodb.client.model.ValidationOptions();

				if (collectionOptions.containsKey("validationLevel")) {
					options.validationLevel(ValidationLevel.fromString(collectionOptions.getString("validationLevel")));
				}
				if (collectionOptions.containsKey("validationAction")) {
					options.validationAction(ValidationAction.fromString(collectionOptions.getString("validationAction")));
				}

				options.validator(collectionOptions.get("validator", Document.class));
				co.validationOptions(options);
			}

			if (collectionOptions.containsKey("timeseries")) {

				Document timeSeries = collectionOptions.get("timeseries", Document.class);
				com.mongodb.client.model.TimeSeriesOptions options = new com.mongodb.client.model.TimeSeriesOptions(
						timeSeries.getString("timeField"));
				if (timeSeries.containsKey("metaField")) {
					options.metaField(timeSeries.getString("metaField"));
				}
				if (timeSeries.containsKey("granularity")) {
					options.granularity(TimeSeriesGranularity.valueOf(timeSeries.getString("granularity").toUpperCase()));
				}
				co.timeSeriesOptions(options);
			}

			db.createCollection(collectionName, co);

			MongoCollection<Document> coll = db.getCollection(collectionName, Document.class);

			// TODO: Emit a collection created event
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Created collection [%s]",
						coll.getNamespace() != null ? coll.getNamespace().getCollectionName() : collectionName));
			}
			return coll;
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
		return doFindOne(collectionName, query, fields, CursorPreparer.NO_OP_PREPARER, entityClass);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The query document is specified as a standard {@link Document} and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param query the query document that specifies the criteria used to find a record.
	 * @param fields the document that specifies the fields to be returned.
	 * @param entityClass the parameterized type of the returned list.
	 * @param preparer the preparer used to modify the cursor on execution.
	 * @return the {@link List} of converted objects.
	 * @since 2.2
	 */
	@SuppressWarnings("ConstantConditions")
	protected <T> T doFindOne(String collectionName, Document query, Document fields, CursorPreparer preparer,
			Class<T> entityClass) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		QueryContext queryContext = queryOperations.createQueryContext(new BasicQuery(query, fields));
		Document mappedFields = queryContext.getMappedFields(entity,
				EntityProjection.nonProjecting(entityClass));
		Document mappedQuery = queryContext.getMappedQuery(entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("findOne using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(query), mappedFields, entityClass, collectionName));
		}

		return executeFindOneInternal(new FindOneCallback(mappedQuery, mappedFields, preparer),
				new ReadDocumentCallback<>(this.mongoConverter, entityClass, collectionName), collectionName);
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
				new ReadDocumentCallback<>(this.mongoConverter, entityClass, collectionName));
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
	 * @param preparer allows for customization of the {@link FindIterable} used when iterating over the result set,
	 *          (apply limits, skips and so on).
	 * @return the {@link List} of converted objects.
	 */
	protected <T> List<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass,
			CursorPreparer preparer) {
		return doFind(collectionName, query, fields, entityClass, preparer,
				new ReadDocumentCallback<>(mongoConverter, entityClass, collectionName));
	}

	protected <S, T> List<T> doFind(String collectionName, Document query, Document fields, Class<S> entityClass,
			@Nullable CursorPreparer preparer, DocumentCallback<T> objectCallback) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		QueryContext queryContext = queryOperations.createQueryContext(new BasicQuery(query, fields));
		Document mappedFields = queryContext.getMappedFields(entity,
				EntityProjection.nonProjecting(entityClass));
		Document mappedQuery = queryContext.getMappedQuery(entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("find using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(mappedQuery), mappedFields, entityClass, collectionName));
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields, null),
				preparer != null ? preparer : CursorPreparer.NO_OP_PREPARER, objectCallback, collectionName);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified targetClass while
	 * using sourceClass for mapping the query.
	 *
	 * @since 2.0
	 */
	<S, T> List<T> doFind(String collectionName, Document query, Document fields, Class<S> sourceClass,
			Class<T> targetClass, CursorPreparer preparer) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(sourceClass);
		EntityProjection<T, S> projection = operations.introspectProjection(targetClass,
				sourceClass);

		QueryContext queryContext = queryOperations.createQueryContext(new BasicQuery(query, fields));
		Document mappedFields = queryContext.getMappedFields(entity, projection);
		Document mappedQuery = queryContext.getMappedQuery(entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("find using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(mappedQuery), mappedFields, sourceClass, collectionName));
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields, null), preparer,
				new ProjectingReadCallback<>(mongoConverter, projection, collectionName), collectionName);
	}


	/**
	 * Convert given {@link CollectionOptions} to a document and take the domain type information into account when
	 * creating a mapped schema for validation. <br />
	 * This method calls {@link #convertToDocument(CollectionOptions)} for backwards compatibility and potentially
	 * overwrites the validator with the mapped validator document. In the long run
	 * {@link #convertToDocument(CollectionOptions)} will be removed so that this one becomes the only source of truth.
	 *
	 * @param collectionOptions can be {@literal null}.
	 * @param targetType must not be {@literal null}. Use {@link Object} type instead.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	protected Document convertToDocument(@Nullable CollectionOptions collectionOptions, Class<?> targetType) {

		Document doc = convertToDocument(collectionOptions);

		if (collectionOptions != null) {

			collectionOptions.getValidationOptions().ifPresent(it -> it.getValidator() //
					.ifPresent(val -> doc.put("validator", getMappedValidator(val, targetType))));

			collectionOptions.getTimeSeriesOptions().map(operations.forType(targetType)::mapTimeSeriesOptions)
					.ifPresent(it -> {

						Document timeseries = new Document("timeField", it.getTimeField());
						if (StringUtils.hasText(it.getMetaField())) {
							timeseries.append("metaField", it.getMetaField());
						}
						if (!Granularity.DEFAULT.equals(it.getGranularity())) {
							timeseries.append("granularity", it.getGranularity().name().toLowerCase());
						}
						doc.put("timeseries", timeseries);
					});
		}

		return doc;
	}

	/**
	 * @param collectionOptions can be {@literal null}.
	 * @return never {@literal null}.
	 * @deprecated since 2.1 in favor of {@link #convertToDocument(CollectionOptions, Class)}.
	 */
	@Deprecated
	protected Document convertToDocument(@Nullable CollectionOptions collectionOptions) {

		Document document = new Document();

		if (collectionOptions != null) {

			collectionOptions.getCapped().ifPresent(val -> document.put("capped", val));
			collectionOptions.getSize().ifPresent(val -> document.put("size", val));
			collectionOptions.getMaxDocuments().ifPresent(val -> document.put("max", val));
			collectionOptions.getCollation().ifPresent(val -> document.append("collation", val.toDocument()));

			collectionOptions.getValidationOptions().ifPresent(it -> {

				it.getValidationLevel().ifPresent(val -> document.append("validationLevel", val.getValue()));
				it.getValidationAction().ifPresent(val -> document.append("validationAction", val.getValue()));
				it.getValidator().ifPresent(val -> document.append("validator", getMappedValidator(val, Object.class)));
			});
		}

		return document;
	}

	Document getMappedValidator(Validator validator, Class<?> domainType) {

		Document validationRules = validator.toDocument();

		if (validationRules.containsKey("$jsonSchema")) {
			return schemaMapper.mapSchema(validationRules, domainType);
		}

		return queryMapper.getMappedObject(validationRules, mappingContext.getPersistentEntity(domainType));
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The first document that matches the query is returned and also removed from the collection in the database.
	 * <br />
	 * The query document is specified as a standard Document and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	@SuppressWarnings("ConstantConditions")
	protected <T> T doFindAndRemove(String collectionName, Document query, Document fields, Document sort,
			@Nullable Collation collation, Class<T> entityClass) {

		EntityReader<? super T, Bson> readerToUse = this.mongoConverter;

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("findAndRemove using query: %s fields: %s sort: %s for class: %s in collection: %s",
					serializeToJsonSafely(query), fields, sort, entityClass, collectionName));
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		return executeFindOneInternal(
				new FindAndRemoveCallback(queryMapper.getMappedObject(query, entity), fields, sort, collation),
				new ReadDocumentCallback<>(readerToUse, entityClass, collectionName), collectionName);
	}

	@SuppressWarnings("ConstantConditions")
	protected <T> T doFindAndModify(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass, UpdateDefinition update, @Nullable FindAndModifyOptions options) {

		EntityReader<? super T, Bson> readerToUse = this.mongoConverter;

		if (options == null) {
			options = new FindAndModifyOptions();
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		UpdateContext updateContext = queryOperations.updateSingleContext(update, query, false);
		updateContext.increaseVersionForUpdateIfNecessary(entity);

		Document mappedQuery = updateContext.getMappedQuery(entity);
		Object mappedUpdate = updateContext.isAggregationUpdate() ? updateContext.getUpdatePipeline(entityClass)
				: updateContext.getMappedUpdate(entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format(
					"findAndModify using query: %s fields: %s sort: %s for class: %s and update: %s in collection: %s",
					serializeToJsonSafely(mappedQuery), fields, sort, entityClass, serializeToJsonSafely(mappedUpdate),
					collectionName));
		}

		return executeFindOneInternal(
				new FindAndModifyCallback(mappedQuery, fields, sort, mappedUpdate,
						update.getArrayFilters().stream().map(ArrayFilter::asDocument).collect(Collectors.toList()), options),
				new ReadDocumentCallback<>(readerToUse, entityClass, collectionName), collectionName);
	}

	/**
	 * Customize this part for findAndReplace.
	 *
	 * @param collectionName The name of the collection to perform the operation in.
	 * @param mappedQuery the query to look up documents.
	 * @param mappedFields the fields to project the result to.
	 * @param mappedSort the sort to be applied when executing the query.
	 * @param collation collation settings for the query. Can be {@literal null}.
	 * @param entityType the source domain type.
	 * @param replacement the replacement {@link Document}.
	 * @param options applicable options.
	 * @param resultType the target domain type.
	 * @return {@literal null} if object does not exist, {@link FindAndReplaceOptions#isReturnNew() return new} is
	 *         {@literal false} and {@link FindAndReplaceOptions#isUpsert() upsert} is {@literal false}.
	 */
	@Nullable
	protected <T> T doFindAndReplace(String collectionName, Document mappedQuery, Document mappedFields,
			Document mappedSort, @Nullable com.mongodb.client.model.Collation collation, Class<?> entityType,
			Document replacement, FindAndReplaceOptions options, Class<T> resultType) {

		EntityProjection<T, ?> projection = operations.introspectProjection(resultType,
				entityType);

		return doFindAndReplace(collectionName, mappedQuery, mappedFields, mappedSort, collation, entityType, replacement,
				options, projection);
	}

	/**
	 * Customize this part for findAndReplace.
	 *
	 * @param collectionName The name of the collection to perform the operation in.
	 * @param mappedQuery the query to look up documents.
	 * @param mappedFields the fields to project the result to.
	 * @param mappedSort the sort to be applied when executing the query.
	 * @param collation collation settings for the query. Can be {@literal null}.
	 * @param entityType the source domain type.
	 * @param replacement the replacement {@link Document}.
	 * @param options applicable options.
	 * @param projection the projection descriptor.
	 * @return {@literal null} if object does not exist, {@link FindAndReplaceOptions#isReturnNew() return new} is
	 *         {@literal false} and {@link FindAndReplaceOptions#isUpsert() upsert} is {@literal false}.
	 * @since 3.4
	 */
	@Nullable
	private <T> T doFindAndReplace(String collectionName, Document mappedQuery, Document mappedFields,
			Document mappedSort, @Nullable com.mongodb.client.model.Collation collation, Class<?> entityType,
			Document replacement, FindAndReplaceOptions options, EntityProjection<T, ?> projection) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format(
					"findAndReplace using query: %s fields: %s sort: %s for class: %s and replacement: %s " + "in collection: %s",
					serializeToJsonSafely(mappedQuery), serializeToJsonSafely(mappedFields), serializeToJsonSafely(mappedSort),
					entityType, serializeToJsonSafely(replacement), collectionName));
		}

		return executeFindOneInternal(
				new FindAndReplaceCallback(mappedQuery, mappedFields, mappedSort, replacement, collation, options),
				new ProjectingReadCallback<>(mongoConverter, projection, collectionName), collectionName);
	}

	/**
	 * Populates the id property of the saved object, if it's not set already.
	 *
	 * @param savedObject
	 * @param id
	 */
	protected <T> T populateIdIfNecessary(T savedObject, Object id) {

		return operations.forEntity(savedObject, mongoConverter.getConversionService()) //
				.populateIdIfNecessary(id);
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
	 * <li>Execute the given {@link CollectionCallback} for a {@link Document}.</li>
	 * <li>Apply the given {@link DocumentCallback} to each of the {@link Document}s to obtain the result.</li>
	 * <ol>
	 *
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link Document} with
	 * @param documentCallback the {@link DocumentCallback} to transform {@link Document}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	@Nullable
	private <T> T executeFindOneInternal(CollectionCallback<Document> collectionCallback,
			DocumentCallback<T> documentCallback, String collectionName) {

		try {

			Document document = collectionCallback.doInCollection(getAndPrepareCollection(doGetDatabase(), collectionName));
			return document != null ? documentCallback.doWith(document) : null;
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/**
	 * Internal method using callback to do queries against the datastore that requires reading a collection of objects.
	 * It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link CollectionCallback} for a {@link FindIterable}.</li>
	 * <li>Prepare that {@link FindIterable} with the given {@link CursorPreparer} (will be skipped if
	 * {@link CursorPreparer} is {@literal null}</li>
	 * <li>Iterate over the {@link FindIterable} and applies the given {@link DocumentCallback} to each of the
	 * {@link Document}s collecting the actual result {@link List}.</li>
	 * <ol>
	 *
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link FindIterable} with
	 * @param preparer the {@link CursorPreparer} to potentially modify the {@link FindIterable} before iterating over it
	 * @param documentCallback the {@link DocumentCallback} to transform {@link Document}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> List<T> executeFindMultiInternal(CollectionCallback<FindIterable<Document>> collectionCallback,
			CursorPreparer preparer, DocumentCallback<T> documentCallback, String collectionName) {

		try {

			try (MongoCursor<Document> cursor = preparer
					.initiateFind(getAndPrepareCollection(doGetDatabase(), collectionName), collectionCallback::doInCollection)
					.iterator()) {

				List<T> result = new ArrayList<>();

				while (cursor.hasNext()) {
					Document object = cursor.next();
					result.add(documentCallback.doWith(object));
				}

				return result;
			}
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	private void executeQueryInternal(CollectionCallback<FindIterable<Document>> collectionCallback,
			CursorPreparer preparer, DocumentCallbackHandler callbackHandler, String collectionName) {

		try (MongoCursor<Document> cursor = preparer
				.initiateFind(getAndPrepareCollection(doGetDatabase(), collectionName), collectionCallback::doInCollection)
				.iterator()) {

			while (cursor.hasNext()) {
				callbackHandler.processDocument(cursor.next());
			}
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	public PersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	@Nullable
	private MongoPersistentEntity<?> getPersistentEntity(@Nullable Class<?> type) {
		return type != null ? mappingContext.getPersistentEntity(type) : null;
	}

	private static MongoConverter getDefaultMongoConverter(MongoDatabaseFactory factory) {

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
		MongoCustomConversions conversions = new MongoCustomConversions(Collections.emptyList());

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		converter.setCustomConversions(conversions);
		converter.setCodecRegistryProvider(factory);
		converter.afterPropertiesSet();

		return converter;
	}

	private Document getMappedSortObject(Query query, Class<?> type) {

		if (query == null || ObjectUtils.isEmpty(query.getSortObject())) {
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

	// Callback implementations

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link MongoCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 * @author Christoph Strobl
	 */
	private static class FindOneCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Optional<Document> fields;
		private final CursorPreparer cursorPreparer;

		FindOneCallback(Document query, Document fields, CursorPreparer preparer) {

			this.query = query;
			this.fields = Optional.of(fields).filter(it -> !ObjectUtils.isEmpty(fields));
			this.cursorPreparer = preparer;
		}

		@Override
		public Document doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

			FindIterable<Document> iterable = cursorPreparer.initiateFind(collection, col -> col.find(query, Document.class));

			if (LOGGER.isDebugEnabled()) {

				LOGGER.debug(String.format("findOne using query: %s fields: %s in db.collection: %s",
						serializeToJsonSafely(query),
						serializeToJsonSafely(fields.orElseGet(Document::new)),
						collection.getNamespace() != null ? collection.getNamespace().getFullName() : "n/a"));
			}

			if (fields.isPresent()) {
				iterable = iterable.projection(fields.get());
			}

			return iterable.first();
		}
	}

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link MongoCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 * @author Christoph Strobl
	 */
	private static class FindCallback implements CollectionCallback<FindIterable<Document>> {

		private final Document query;
		private final Document fields;
		private final @Nullable com.mongodb.client.model.Collation collation;

		public FindCallback(Document query, Document fields, @Nullable com.mongodb.client.model.Collation collation) {

			Assert.notNull(query, "Query must not be null!");
			Assert.notNull(fields, "Fields must not be null!");

			this.query = query;
			this.fields = fields;
			this.collation = collation;
		}

		public FindIterable<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			FindIterable<Document> findIterable = collection.find(query, Document.class).projection(fields);

			if (collation != null) {
				findIterable = findIterable.collation(collation);
			}
			return findIterable;
		}
	}

	/**
	 * Optimized {@link CollectionCallback} that takes an already mapped query and a nullable
	 * {@link com.mongodb.client.model.Collation} to execute a count query limited to one element.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	private class ExistsCallback implements CollectionCallback<Boolean> {

		private final Document mappedQuery;
		private final com.mongodb.client.model.Collation collation;

		ExistsCallback(Document mappedQuery, com.mongodb.client.model.Collation collation) {

			this.mappedQuery = mappedQuery;
			this.collation = collation;
		}

		@Override
		public Boolean doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

			return doCount(collection.getNamespace().getCollectionName(), mappedQuery,
					new CountOptions().limit(1).collation(collation)) > 0;
		}
	}

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link MongoCollection}.
	 *
	 * @author Thomas Risberg
	 */
	private static class FindAndRemoveCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Optional<Collation> collation;

		FindAndRemoveCallback(Document query, Document fields, Document sort, @Nullable Collation collation) {

			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.collation = Optional.ofNullable(collation);
		}

		public Document doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

			FindOneAndDeleteOptions opts = new FindOneAndDeleteOptions().sort(sort).projection(fields);
			collation.map(Collation::toMongoCollation).ifPresent(opts::collation);

			return collection.findOneAndDelete(query, opts);
		}
	}

	private static class FindAndModifyCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Object update;
		private final List<Document> arrayFilters;
		private final FindAndModifyOptions options;

		FindAndModifyCallback(Document query, Document fields, Document sort, Object update, List<Document> arrayFilters,
				FindAndModifyOptions options) {

			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.arrayFilters = arrayFilters;
			this.options = options;
		}

		public Document doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

			FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
			opts.sort(sort);
			if (options.isUpsert()) {
				opts.upsert(true);
			}
			opts.projection(fields);
			if (options.isReturnNew()) {
				opts.returnDocument(ReturnDocument.AFTER);
			}

			options.getCollation().map(Collation::toMongoCollation).ifPresent(opts::collation);

			if (!arrayFilters.isEmpty()) {
				opts.arrayFilters(arrayFilters);
			}

			if (update instanceof Document) {
				return collection.findOneAndUpdate(query, (Document) update, opts);
			} else if (update instanceof List) {
				return collection.findOneAndUpdate(query, (List<Document>) update, opts);
			}

			throw new IllegalArgumentException(String.format("Using %s is not supported in findOneAndUpdate", update));
		}
	}

	/**
	 * {@link CollectionCallback} specific for find and remove operation.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	private static class FindAndReplaceCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Document update;
		private final @Nullable com.mongodb.client.model.Collation collation;
		private final FindAndReplaceOptions options;

		FindAndReplaceCallback(Document query, Document fields, Document sort, Document update,
				@Nullable com.mongodb.client.model.Collation collation, FindAndReplaceOptions options) {

			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.options = options;
			this.collation = collation;
		}

		@Override
		public Document doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

			FindOneAndReplaceOptions opts = new FindOneAndReplaceOptions();
			opts.sort(sort);
			opts.collation(collation);
			opts.projection(fields);

			if (options.isUpsert()) {
				opts.upsert(true);
			}

			if (options.isReturnNew()) {
				opts.returnDocument(ReturnDocument.AFTER);
			}

			return collection.findOneAndReplace(query, update, opts);
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
	 * {@link EntityReader}.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 * @author Roman Puchkovskiy
	 */
	private class ReadDocumentCallback<T> implements DocumentCallback<T> {

		private final EntityReader<? super T, Bson> reader;
		private final Class<T> type;
		private final String collectionName;

		ReadDocumentCallback(EntityReader<? super T, Bson> reader, Class<T> type, String collectionName) {

			this.reader = reader;
			this.type = type;
			this.collectionName = collectionName;
		}

		public T doWith(Document document) {

			maybeEmitEvent(new AfterLoadEvent<>(document, type, collectionName));
			T entity = reader.read(type, document);

			if (entity == null) {
				throw new MappingException(String.format("EntityReader %s returned null", reader));
			}

			maybeEmitEvent(new AfterConvertEvent<>(document, entity, collectionName));
			entity = maybeCallAfterConvert(entity, document, collectionName);

			return entity;
		}
	}

	/**
	 * {@link DocumentCallback} transforming {@link Document} into the given {@code targetType} or decorating the
	 * {@code sourceType} with a {@literal projection} in case the {@code targetType} is an {@literal interface}.
	 *
	 * @param <S>
	 * @param <T>
	 * @since 2.0
	 */
	private class ProjectingReadCallback<S, T> implements DocumentCallback<T> {

		private final MongoConverter mongoConverter;
		private final EntityProjection<T, S> projection;
		private final String collectionName;

		ProjectingReadCallback(MongoConverter mongoConverter, EntityProjection<T, S> projection,
				String collectionName) {

			this.mongoConverter = mongoConverter;
			this.projection = projection;
			this.collectionName = collectionName;
		}

		@SuppressWarnings("unchecked")
		public T doWith(Document document) {

			if (document == null) {
				return null;
			}

			maybeEmitEvent(new AfterLoadEvent<>(document, projection.getMappedType().getType(), collectionName));

			Object entity = mongoConverter.project(projection, document);

			if (entity == null) {
				throw new MappingException(String.format("EntityReader %s returned null", mongoConverter));
			}

			maybeEmitEvent(new AfterConvertEvent<>(document, entity, collectionName));
			return (T) maybeCallAfterConvert(entity, document, collectionName);
		}
	}

	class QueryCursorPreparer implements CursorPreparer {

		private final Query query;
		private final @Nullable Class<?> type;

		QueryCursorPreparer(Query query, @Nullable Class<?> type) {

			this.query = query;
			this.type = type;
		}

		public FindIterable<Document> prepare(FindIterable<Document> iterable) {

			FindIterable<Document> cursorToUse = iterable;

			operations.forType(type).getCollation(query) //
					.map(Collation::toMongoCollation) //
					.ifPresent(cursorToUse::collation);

			Meta meta = query.getMeta();
			if (query.getSkip() <= 0 && query.getLimit() <= 0 && ObjectUtils.isEmpty(query.getSortObject())
					&& !StringUtils.hasText(query.getHint()) && !meta.hasValues() && !query.getCollation().isPresent()) {
				return cursorToUse;
			}

			try {
				if (query.getSkip() > 0) {
					cursorToUse = cursorToUse.skip((int) query.getSkip());
				}
				if (query.getLimit() > 0) {
					cursorToUse = cursorToUse.limit(query.getLimit());
				}
				if (!ObjectUtils.isEmpty(query.getSortObject())) {
					Document sort = type != null ? getMappedSortObject(query, type) : query.getSortObject();
					cursorToUse = cursorToUse.sort(sort);
				}

				if (StringUtils.hasText(query.getHint())) {

					String hint = query.getHint();

					if (BsonUtils.isJsonDocument(hint)) {
						cursorToUse = cursorToUse.hint(BsonUtils.parse(hint, mongoDbFactory));
					} else {
						cursorToUse = cursorToUse.hintString(hint);
					}
				}

				if (meta.hasValues()) {

					if (StringUtils.hasText(meta.getComment())) {
						cursorToUse = cursorToUse.comment(meta.getComment());
					}

					if (meta.getMaxTimeMsec() != null) {
						cursorToUse = cursorToUse.maxTime(meta.getMaxTimeMsec(), TimeUnit.MILLISECONDS);
					}

					if (meta.getCursorBatchSize() != null) {
						cursorToUse = cursorToUse.batchSize(meta.getCursorBatchSize());
					}

					if (meta.getAllowDiskUse() != null) {
						cursorToUse = cursorToUse.allowDiskUse(meta.getAllowDiskUse());
					}

					for (Meta.CursorOption option : meta.getFlags()) {

						switch (option) {

							case NO_TIMEOUT:
								cursorToUse = cursorToUse.noCursorTimeout(true);
								break;
							case PARTIAL:
								cursorToUse = cursorToUse.partial(true);
								break;
							case SECONDARY_READS:
							case SLAVE_OK:
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

		@Override
		public ReadPreference getReadPreference() {
			return (query.getMeta().getFlags().contains(CursorOption.SECONDARY_READS)
					|| query.getMeta().getFlags().contains(CursorOption.SLAVE_OK)) ? ReadPreference.primaryPreferred() : null;
		}
	}

	/**
	 * {@link DocumentCallback} that assumes a {@link GeoResult} to be created, delegates actual content unmarshalling to
	 * a delegate and creates a {@link GeoResult} from the result.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 */
	static class GeoNearResultDocumentCallback<T> implements DocumentCallback<GeoResult<T>> {

		private final String distanceField;
		private final DocumentCallback<T> delegate;
		private final Metric metric;

		/**
		 * Creates a new {@link GeoNearResultDocumentCallback} using the given {@link DocumentCallback} delegate for
		 * {@link GeoResult} content unmarshalling.
		 *
		 * @param distanceField the field to read the distance from.
		 * @param delegate must not be {@literal null}.
		 * @param metric the {@link Metric} to apply to the result distance.
		 */
		GeoNearResultDocumentCallback(String distanceField, DocumentCallback<T> delegate, Metric metric) {

			Assert.notNull(delegate, "DocumentCallback must not be null!");

			this.distanceField = distanceField;
			this.delegate = delegate;
			this.metric = metric;
		}

		public GeoResult<T> doWith(Document object) {

			double distance = Double.NaN;
			if (object.containsKey(distanceField)) {
				distance = NumberUtils.convertNumberToTargetClass(object.get(distanceField, Number.class), Double.class);
			}

			T doWith = delegate.doWith(object);

			return new GeoResult<>(doWith, new Distance(distance, metric));
		}
	}

	/**
	 * A {@link CloseableIterator} that is backed by a MongoDB {@link MongoCollection}.
	 *
	 * @author Thomas Darimont
	 * @since 1.7
	 */
	static class CloseableIterableCursorAdapter<T> implements CloseableIterator<T> {

		private volatile @Nullable MongoCursor<Document> cursor;
		private PersistenceExceptionTranslator exceptionTranslator;
		private DocumentCallback<T> objectReadCallback;

		/**
		 * Creates a new {@link CloseableIterableCursorAdapter} backed by the given {@link MongoCollection}.
		 */
		CloseableIterableCursorAdapter(MongoIterable<Document> cursor, PersistenceExceptionTranslator exceptionTranslator,
				DocumentCallback<T> objectReadCallback) {

			this.cursor = cursor.iterator();
			this.exceptionTranslator = exceptionTranslator;
			this.objectReadCallback = objectReadCallback;
		}

		CloseableIterableCursorAdapter(MongoCursor<Document> cursor, PersistenceExceptionTranslator exceptionTranslator,
				DocumentCallback<T> objectReadCallback) {

			this.cursor = cursor;
			this.exceptionTranslator = exceptionTranslator;
			this.objectReadCallback = objectReadCallback;
		}

		@Override
		public boolean hasNext() {

			MongoCursor<Document> cursor = this.cursor;

			if (cursor == null) {
				return false;
			}

			try {
				return cursor.hasNext();
			} catch (RuntimeException ex) {
				throw potentiallyConvertRuntimeException(ex, exceptionTranslator);
			}
		}

		@Nullable
		@Override
		public T next() {

			if (cursor == null) {
				return null;
			}

			try {
				Document item = cursor.next();
				return objectReadCallback.doWith(item);
			} catch (RuntimeException ex) {
				throw potentiallyConvertRuntimeException(ex, exceptionTranslator);
			}
		}

		@Override
		public void close() {

			MongoCursor<Document> c = cursor;

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

	/**
	 * @deprecated since 3.1.4. Use {@link #getMongoDatabaseFactory()} instead.
	 * @return the {@link MongoDatabaseFactory} in use.
	 */
	@Deprecated
	public MongoDatabaseFactory getMongoDbFactory() {
		return getMongoDatabaseFactory();
	}

	/**
	 * @return the {@link MongoDatabaseFactory} in use.
	 * @since 3.1.4
	 */
	public MongoDatabaseFactory getMongoDatabaseFactory() {
		return mongoDbFactory;
	}

	/**
	 * {@link MongoTemplate} extension bound to a specific {@link ClientSession} that is applied when interacting with the
	 * server through the driver API.
	 * <br />
	 * The prepare steps for {@link MongoDatabase} and {@link MongoCollection} proxy the target and invoke the desired
	 * target method matching the actual arguments plus a {@link ClientSession}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class SessionBoundMongoTemplate extends MongoTemplate {

		private final MongoTemplate delegate;
		private final ClientSession session;

		/**
		 * @param session must not be {@literal null}.
		 * @param that must not be {@literal null}.
		 */
		SessionBoundMongoTemplate(ClientSession session, MongoTemplate that) {

			super(that.getMongoDbFactory().withSession(session), that);

			this.delegate = that;
			this.session = session;
		}

		@Override
		public MongoCollection<Document> getCollection(String collectionName) {

			// native MongoDB objects that offer methods with ClientSession must not be proxied.
			return delegate.getCollection(collectionName);
		}

		@Override
		public MongoDatabase getDb() {

			// native MongoDB objects that offer methods with ClientSession must not be proxied.
			return delegate.getDb();
		}
	}
}
