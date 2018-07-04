/*
 * Copyright 2010-2018 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.SerializationUtils.*;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.SessionSynchronization;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.DefaultBulkOperations.BulkOperationContext;
import org.springframework.data.mongodb.core.EntityOperations.AdaptibleEntity;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.Fields;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.*;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.IndexOperationsProvider;
import org.springframework.data.mongodb.core.index.MongoMappingEventPublisher;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
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
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.validation.Validator;
import org.springframework.data.projection.ProjectionInformation;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.util.CloseableIterator;
import org.springframework.data.util.Optionals;
import org.springframework.jca.cci.core.ConnectionCallback;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
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
 */
@SuppressWarnings("deprecation")
public class MongoTemplate implements MongoOperations, ApplicationContextAware, IndexOperationsProvider {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoTemplate.class);
	private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
	private static final Collection<String> ITERABLE_CLASSES;

	static {

		Set<String> iterableClasses = new HashSet<>();
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
	private final JsonSchemaMapper schemaMapper;
	private final SpelAwareProxyProjectionFactory projectionFactory;
	private final EntityOperations operations;

	private @Nullable WriteConcern writeConcern;
	private WriteConcernResolver writeConcernResolver = DefaultWriteConcernResolver.INSTANCE;
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;
	private @Nullable ReadPreference readPreference;
	private @Nullable ApplicationEventPublisher eventPublisher;
	private @Nullable ResourceLoader resourceLoader;
	private @Nullable MongoPersistentEntityIndexCreator indexCreator;

	private SessionSynchronization sessionSynchronization = SessionSynchronization.ON_ACTUAL_TRANSACTION;

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 */
	public MongoTemplate(MongoClient mongoClient, String databaseName) {
		this(new SimpleMongoDbFactory(mongoClient, databaseName), (MongoConverter) null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 * @since 2.1
	 */
	public MongoTemplate(com.mongodb.client.MongoClient mongoClient, String databaseName) {
		this(new SimpleMongoClientDbFactory(mongoClient, databaseName), (MongoConverter) null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public MongoTemplate(MongoDbFactory mongoDbFactory) {
		this(mongoDbFactory, (MongoConverter) null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param mongoConverter
	 */
	public MongoTemplate(MongoDbFactory mongoDbFactory, @Nullable MongoConverter mongoConverter) {

		Assert.notNull(mongoDbFactory, "MongoDbFactory must not be null!");

		this.mongoDbFactory = mongoDbFactory;
		this.exceptionTranslator = mongoDbFactory.getExceptionTranslator();
		this.mongoConverter = mongoConverter == null ? getDefaultMongoConverter(mongoDbFactory) : mongoConverter;
		this.queryMapper = new QueryMapper(this.mongoConverter);
		this.updateMapper = new UpdateMapper(this.mongoConverter);
		this.schemaMapper = new MongoJsonSchemaMapper(this.mongoConverter);
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
		this.operations = new EntityOperations(this.mongoConverter.getMappingContext());

		// We always have a mapping context in the converter, whether it's a simple one or not
		mappingContext = this.mongoConverter.getMappingContext();
		// We create indexes based on mapping events
		if (mappingContext instanceof MongoMappingContext) {
			indexCreator = new MongoPersistentEntityIndexCreator((MongoMappingContext) mappingContext, this);
			eventPublisher = new MongoMappingEventPublisher(indexCreator);
			if (mappingContext instanceof ApplicationEventPublisherAware) {
				((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
			}
		}
	}

	private MongoTemplate(MongoDbFactory dbFactory, MongoTemplate that) {

		this.mongoDbFactory = dbFactory;
		this.exceptionTranslator = that.exceptionTranslator;
		this.sessionSynchronization = that.sessionSynchronization;
		this.mongoConverter = that.mongoConverter instanceof MappingMongoConverter ? getDefaultMongoConverter(dbFactory)
				: that.mongoConverter;
		this.queryMapper = that.queryMapper;
		this.updateMapper = that.updateMapper;
		this.schemaMapper = that.schemaMapper;
		this.projectionFactory = that.projectionFactory;
		this.mappingContext = that.mappingContext;
		this.operations = that.operations;
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
	 * configured on the {@link MongoDbFactory} will apply. If you configured a {@link Mongo} instance no
	 * {@link WriteConcern} will be used.
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

		projectionFactory.setBeanFactory(applicationContext);
		projectionFactory.setBeanClassLoader(applicationContext.getClassLoader());
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeAsStream(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> CloseableIterator<T> stream(final Query query, final Class<T> entityType) {

		return stream(query, entityType, operations.determineCollectionName(entityType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#stream(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> CloseableIterator<T> stream(final Query query, final Class<T> entityType, final String collectionName) {
		return doStream(query, entityType, collectionName, entityType);
	}

	protected <T> CloseableIterator<T> doStream(final Query query, final Class<?> entityType, final String collectionName,
			Class<T> returnType) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityType, "Entity type must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(returnType, "ReturnType must not be null!");

		return execute(collectionName, new CollectionCallback<CloseableIterator<T>>() {

			@Override
			public CloseableIterator<T> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				MongoPersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entityType);

				Document mappedFields = getMappedFieldsObject(query.getFieldsObject(), persistentEntity, returnType);
				Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), persistentEntity);

				FindIterable<Document> cursor = new QueryCursorPreparer(query, entityType)
						.prepare(collection.find(mappedQuery, Document.class).projection(mappedFields));

				return new CloseableIterableCursorAdapter<>(cursor, exceptionTranslator,
						new ProjectingReadCallback<>(mongoConverter, entityType, returnType, collectionName));
			}
		});
	}

	@Override
	public String getCollectionName(Class<?> entityClass) {
		return this.operations.determineCollectionName(entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeCommand(java.lang.String)
	 */
	@Override
	public Document executeCommand(final String jsonCommand) {

		Assert.hasText(jsonCommand, "JsonCommand must not be null nor empty!");

		return execute(new DbCallback<Document>() {
			public Document doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.runCommand(Document.parse(jsonCommand), Document.class);
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeCommand(org.bson.Document)
	 */
	@Override
	public Document executeCommand(final Document command) {

		Assert.notNull(command, "Command must not be null!");

		return execute(db -> db.runCommand(command, Document.class));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeCommand(org.bson.Document, com.mongodb.ReadPreference)
	 */
	@Override
	public Document executeCommand(Document command, @Nullable ReadPreference readPreference) {

		Assert.notNull(command, "Command must not be null!");

		return execute(db -> readPreference != null //
				? db.runCommand(command, readPreference, Document.class) //
				: db.runCommand(command, Document.class));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#executeQuery(org.springframework.data.mongodb.core.query.Query, java.lang.String, org.springframework.data.mongodb.core.DocumentCallbackHandler)
	 */
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
	 * @param preparer allows for customization of the {@link DBCursor} used when iterating over the result set, (apply
	 *          limits, skips and so on).
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
			LOGGER.debug("Executing query: {} sort: {} fields: {} in collection: {}", serializeToJsonSafely(queryObject),
					sortObject, fieldsObject, collectionName);
		}

		this.executeQueryInternal(new FindCallback(queryObject, fieldsObject), preparer, documentCallbackHandler,
				collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#execute(org.springframework.data.mongodb.core.DbCallback)
	 */
	public <T> T execute(DbCallback<T> action) {

		Assert.notNull(action, "DbCallback must not be null!");

		try {
			MongoDatabase db = prepareDatabase(this.doGetDatabase());
			return action.doInDB(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#execute(java.lang.Class, org.springframework.data.mongodb.core.DbCallback)
	 */
	public <T> T execute(Class<?> entityClass, CollectionCallback<T> callback) {

		Assert.notNull(entityClass, "EntityClass must not be null!");
		return execute(operations.determineCollectionName(entityClass), callback);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#execute(java.lang.String, org.springframework.data.mongodb.core.DbCallback)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#withSession(com.mongodb.ClientSessionOptions)
	 */
	@Override
	public SessionScoped withSession(ClientSessionOptions options) {

		Assert.notNull(options, "ClientSessionOptions must not be null!");

		return withSession(() -> mongoDbFactory.getSession(options));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#withSession(com.mongodb.session.ClientSession)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#createCollection(java.lang.Class)
	 */
	public <T> MongoCollection<Document> createCollection(Class<T> entityClass) {
		return createCollection(operations.determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#createCollection(java.lang.Class, org.springframework.data.mongodb.core.CollectionOptions)
	 */
	public <T> MongoCollection<Document> createCollection(Class<T> entityClass,
			@Nullable CollectionOptions collectionOptions) {

		Assert.notNull(entityClass, "EntityClass must not be null!");
		return doCreateCollection(operations.determineCollectionName(entityClass),
				convertToDocument(collectionOptions, entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#createCollection(java.lang.String)
	 */
	public MongoCollection<Document> createCollection(final String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

		return doCreateCollection(collectionName, new Document());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#createCollection(java.lang.String, org.springframework.data.mongodb.core.CollectionOptions)
	 */
	public MongoCollection<Document> createCollection(final String collectionName,
			final @Nullable CollectionOptions collectionOptions) {

		Assert.notNull(collectionName, "CollectionName must not be null!");
		return doCreateCollection(collectionName, convertToDocument(collectionOptions));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#getCollection(java.lang.String)
	 */
	public MongoCollection<Document> getCollection(final String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

		return execute(new DbCallback<MongoCollection<Document>>() {
			public MongoCollection<Document> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				return db.getCollection(collectionName, Document.class);
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#getCollection(java.lang.Class)
	 */
	public <T> boolean collectionExists(Class<T> entityClass) {
		return collectionExists(operations.determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#getCollection(java.lang.String)
	 */
	public boolean collectionExists(final String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#dropCollection(java.lang.Class)
	 */
	public <T> void dropCollection(Class<T> entityClass) {
		dropCollection(operations.determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#dropCollection(java.lang.String)
	 */
	public void dropCollection(String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				collection.drop();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Dropped collection [{}]",
							collection.getNamespace() != null ? collection.getNamespace().getCollectionName() : collectionName);
				}
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#indexOps(java.lang.String)
	 */
	public IndexOperations indexOps(String collectionName) {
		return new DefaultIndexOperations(this, collectionName, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#indexOps(java.lang.Class)
	 */
	public IndexOperations indexOps(Class<?> entityClass) {
		return new DefaultIndexOperations(this, operations.determineCollectionName(entityClass), entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#bulkOps(org.springframework.data.mongodb.core.BulkMode, java.lang.String)
	 */
	public BulkOperations bulkOps(BulkMode bulkMode, String collectionName) {
		return bulkOps(bulkMode, null, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#bulkOps(org.springframework.data.mongodb.core.BulkMode, java.lang.Class)
	 */
	public BulkOperations bulkOps(BulkMode bulkMode, Class<?> entityClass) {
		return bulkOps(bulkMode, entityClass, operations.determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#bulkOps(org.springframework.data.mongodb.core.BulkMode, java.lang.Class, java.lang.String)
	 */
	public BulkOperations bulkOps(BulkMode mode, @Nullable Class<?> entityType, String collectionName) {

		Assert.notNull(mode, "BulkMode must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		DefaultBulkOperations operations = new DefaultBulkOperations(this, collectionName, new BulkOperationContext(mode,
				Optional.ofNullable(getPersistentEntity(entityType)), queryMapper, updateMapper));

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

	@Nullable
	@Override
	public <T> T findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, operations.determineCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		if (ObjectUtils.isEmpty(query.getSortObject()) && !query.getCollation().isPresent()) {
			return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass);
		} else {
			query.limit(1);
			List<T> results = find(query, entityClass, collectionName);
			return results.isEmpty() ? null : results.get(0);
		}
	}

	@Override
	public boolean exists(Query query, Class<?> entityClass) {
		return exists(query, entityClass, operations.determineCollectionName(entityClass));
	}

	@Override
	public boolean exists(Query query, String collectionName) {
		return exists(query, null, collectionName);
	}

	@Override
	public boolean exists(Query query, @Nullable Class<?> entityClass, String collectionName) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to exist can't be null");
		}
		Assert.notNull(collectionName, "CollectionName must not be null!");

		Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), getPersistentEntity(entityClass));

		return execute(collectionName,
				new ExistsCallback(mappedQuery, query.getCollation().map(Collation::toMongoCollation).orElse(null)));
	}

	// Find methods that take a Query to express the query and that return a List of objects.

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findOne(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> List<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, operations.determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findOne(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
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
		return findById(id, entityClass, operations.determineCollectionName(entityClass));
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findDistinct(org.springframework.data.mongodb.core.query.Query, java.lang.String, java.lang.Class, java.lang.Class)
	 */
	@Override
	public <T> List<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass) {
		return findDistinct(query, field, operations.determineCollectionName(entityClass), entityClass, resultClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findDistinct(org.springframework.data.mongodb.core.query.Query, java.lang.String, java.lang.String, java.lang.Class, java.lang.Class)
	 */
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

		Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), entity);
		String mappedFieldName = queryMapper.getMappedFields(new Document(field, 1), entity).keySet().iterator().next();

		Class<T> mongoDriverCompatibleType = getMongoDbFactory().getCodecFor(resultClass) //
				.map(Codec::getEncoderClass) //
				.orElse((Class<T>) BsonValue.class);

		MongoIterable<?> result = execute(collectionName, (collection) -> {

			DistinctIterable<T> iterable = collection.distinct(mappedFieldName, mappedQuery, mongoDriverCompatibleType);

			return query.getCollation().map(Collation::toMongoCollation).map(iterable::collation).orElse(iterable);
		});

		if (resultClass == Object.class || mongoDriverCompatibleType != resultClass) {

			MongoConverter converter = getConverter();
			DefaultDbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoDbFactory);

			result = result.map((source) -> converter.mapValueToTargetType(source,
					getMostSpecificConversionTargetType(resultClass, entityClass, field), dbRefResolver));
		}

		try {
			return (List<T>) result.into(new ArrayList<>());
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/**
	 * @param userType must not be {@literal null}.
	 * @param domainType must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return the most specific conversion target type depending on user preference and domain type property.
	 * @since 2.1
	 */
	private static Class<?> getMostSpecificConversionTargetType(Class<?> userType, Class<?> domainType, String field) {

		Class<?> conversionTargetType = userType;
		try {

			Class<?> propertyType = PropertyPath.from(field, domainType).getLeafProperty().getLeafType();

			// use the more specific type but favor UserType over property one
			if (ClassUtils.isAssignable(userType, propertyType)) {
				conversionTargetType = propertyType;
			}

		} catch (PropertyReferenceException e) {
			// just don't care about it as we default to Object.class anyway.
		}

		return conversionTargetType;
	}

	@Override
	public <T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass) {
		return geoNear(near, entityClass, operations.determineCollectionName(entityClass));
	}

	@Override
	public <T> GeoResults<T> geoNear(NearQuery near, Class<T> domainType, String collectionName) {
		return geoNear(near, domainType, collectionName, domainType);
	}

	@SuppressWarnings("unchecked")
	public <T> GeoResults<T> geoNear(NearQuery near, Class<?> domainType, String collectionName, Class<T> returnType) {

		if (near == null) {
			throw new InvalidDataAccessApiUsageException("NearQuery must not be null!");
		}

		if (domainType == null) {
			throw new InvalidDataAccessApiUsageException("Entity class must not be null!");
		}

		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(returnType, "ReturnType must not be null!");

		String collection = StringUtils.hasText(collectionName) ? collectionName
				: operations.determineCollectionName(domainType);
		Document nearDocument = near.toDocument();

		Document command = new Document("geoNear", collection);
		command.putAll(nearDocument);

		if (nearDocument.containsKey("query")) {
			Document query = (Document) nearDocument.get("query");
			command.put("query", queryMapper.getMappedObject(query, getPersistentEntity(domainType)));
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing geoNear using: {} for class: {} in collection: {}", serializeToJsonSafely(command),
					domainType, collectionName);
		}

		Document commandResult = executeCommand(command, this.readPreference);
		List<Object> results = (List<Object>) commandResult.get("results");
		results = results == null ? Collections.emptyList() : results;

		DocumentCallback<GeoResult<T>> callback = new GeoNearResultDocumentCallback<>(
				new ProjectingReadCallback<>(mongoConverter, domainType, returnType, collectionName), near.getMetric());
		List<GeoResult<T>> result = new ArrayList<>(results.size());

		int index = 0;
		long elementsToSkip = near.getSkip() != null ? near.getSkip() : 0;

		for (Object element : results) {

			/*
			 * As MongoDB currently (2.4.4) doesn't support the skipping of elements in near queries
			 * we skip the elements ourselves to avoid at least the document 2 object mapping overhead.
			 *
			 * @see <a href="https://jira.mongodb.org/browse/SERVER-3925">MongoDB Jira: SERVER-3925</a>
			 */
			if (index >= elementsToSkip) {
				result.add(callback.doWith((Document) element));
			}
			index++;
		}

		if (elementsToSkip > 0) {
			// as we skipped some elements we have to calculate the averageDistance ourselves:
			return new GeoResults<>(result, near.getMetric());
		}

		GeoCommandStatistics stats = GeoCommandStatistics.from(commandResult);
		return new GeoResults<>(result, new Distance(stats.getAverageDistance(), near.getMetric()));
	}

	@Nullable
	@Override
	public <T> T findAndModify(Query query, Update update, Class<T> entityClass) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass,
				operations.determineCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findAndModify(Query query, Update update, Class<T> entityClass, String collectionName) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, collectionName);
	}

	@Nullable
	@Override
	public <T> T findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass) {
		return findAndModify(query, update, options, entityClass, operations.determineCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass,
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

		query.getCollation().ifPresent(optionsToUse::collation);

		return doFindAndModify(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass, update, optionsToUse);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAndReplace(org.springframework.data.mongodb.core.query.Query, java.lang.Object, org.springframework.data.mongodb.core.FindAndReplaceOptions, java.lang.Class, java.lang.String, java.lang.Class)
	 */
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

		Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), entity);
		Document mappedFields = queryMapper.getMappedFields(query.getFieldsObject(), entity);
		Document mappedSort = queryMapper.getMappedSort(query.getSortObject(), entity);

		Document mappedReplacement = operations.forEntity(replacement).toMappedDocument(this.mongoConverter).getDocument();

		return doFindAndReplace(collectionName, mappedQuery, mappedFields, mappedSort,
				query.getCollation().map(Collation::toMongoCollation).orElse(null), entityType, mappedReplacement, options,
				resultType);
	}

	// Find methods that take a Query to express the query and that return a single object that is also removed from the
	// collection in the database.

	@Nullable
	@Override
	public <T> T findAndRemove(Query query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, operations.determineCollectionName(entityClass));
	}

	@Nullable
	@Override
	public <T> T findAndRemove(Query query, Class<T> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), query.getCollation().orElse(null), entityClass);
	}

	@Override
	public long count(Query query, Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null!");
		return count(query, entityClass, operations.determineCollectionName(entityClass));
	}

	@Override
	public long count(final Query query, String collectionName) {
		return count(query, null, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public long count(Query query, @Nullable Class<?> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		CountOptions options = new CountOptions();
		query.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);

		Document document = queryMapper.getMappedObject(query.getQueryObject(),
				Optional.ofNullable(entityClass).map(it -> mappingContext.getPersistentEntity(entityClass)));

		return execute(collectionName, collection -> collection.count(document, options));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> T insert(T objectToSave) {

		Assert.notNull(objectToSave, "ObjectToSave must not be null!");

		ensureNotIterable(objectToSave);
		return insert(objectToSave, operations.determineEntityCollectionName(objectToSave));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#insert(java.lang.Object, java.lang.String)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> T insert(T objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "ObjectToSave must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		ensureNotIterable(objectToSave);
		return (T) doInsert(collectionName, objectToSave, this.mongoConverter);
	}

	protected void ensureNotIterable(@Nullable Object o) {
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

		AdaptibleEntity<T> entity = operations.forEntity(objectToSave, mongoConverter.getConversionService());
		T toSave = entity.initializeVersionProperty();

		BeforeConvertEvent<T> event = new BeforeConvertEvent<>(toSave, collectionName);
		toSave = maybeEmitEvent(event).getSource();

		entity.assertUpdateableIdIfNotSet();

		Document dbDoc = entity.toMappedDocument(writer).getDocument();

		maybeEmitEvent(new BeforeSaveEvent<>(toSave, dbDoc, collectionName));
		Object id = insertDocument(collectionName, dbDoc, toSave.getClass());

		T saved = populateIdIfNecessary(toSave, id);
		maybeEmitEvent(new AfterSaveEvent<>(saved, dbDoc, collectionName));

		return saved;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Collection<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass) {

		Assert.notNull(batchToSave, "BatchToSave must not be null!");

		return (Collection<T>) doInsertBatch(operations.determineCollectionName(entityClass), batchToSave,
				this.mongoConverter);
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

			MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(element.getClass());

			String collection = entity.getCollection();
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

			AdaptibleEntity<T> entity = operations.forEntity(uninitialized, mongoConverter.getConversionService());
			T toSave = entity.initializeVersionProperty();

			BeforeConvertEvent<T> event = new BeforeConvertEvent<>(toSave, collectionName);
			toSave = maybeEmitEvent(event).getSource();

			Document document = entity.toMappedDocument(writer).getDocument();

			maybeEmitEvent(new BeforeSaveEvent<>(toSave, document, collectionName));
			documentList.add(document);
			initializedBatchToSave.add(toSave);
		}

		List<Object> ids = insertDocumentList(collectionName, documentList);
		List<T> savedObjects = new ArrayList<>(documentList.size());

		int i = 0;
		for (T obj : initializedBatchToSave) {

			if (i < ids.size()) {
				T saved = populateIdIfNecessary(obj, ids.get(i));
				maybeEmitEvent(new AfterSaveEvent<>(saved, documentList.get(i), collectionName));
				savedObjects.add(saved);
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
		return save(objectToSave, operations.determineEntityCollectionName(objectToSave));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T save(T objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		AdaptibleEntity<T> source = operations.forEntity(objectToSave, mongoConverter.getConversionService());

		return source.isVersionedEntity() //
				? doSaveVersioned(source, collectionName) //
				: (T) doSave(collectionName, objectToSave, this.mongoConverter);

	}

	@SuppressWarnings("unchecked")
	private <T> T doSaveVersioned(AdaptibleEntity<T> source, String collectionName) {

		Number number = source.getVersion();

		if (number != null) {

			// Create query for entity with the id and old version
			Query query = source.getQueryForVersion();

			// Bump version number
			T toSave = source.incrementVersion();

			toSave = maybeEmitEvent(new BeforeConvertEvent<T>(toSave, collectionName)).getSource();

			source.assertUpdateableIdIfNotSet();

			MappedDocument mapped = source.toMappedDocument(mongoConverter);

			maybeEmitEvent(new BeforeSaveEvent<>(toSave, mapped.getDocument(), collectionName));
			Update update = mapped.updateWithoutId();

			UpdateResult result = doUpdate(collectionName, query, update, toSave.getClass(), false, false);

			if (result.getModifiedCount() == 0) {
				throw new OptimisticLockingFailureException(
						String.format("Cannot save entity %s with version %s to collection %s. Has it been modified meanwhile?",
								source.getId(), number, collectionName));
			}
			maybeEmitEvent(new AfterSaveEvent<>(toSave, mapped.getDocument(), collectionName));

			return toSave;
		}

		return (T) doInsert(collectionName, source.getBean(), this.mongoConverter);
	}

	protected <T> T doSave(String collectionName, T objectToSave, MongoWriter<T> writer) {

		objectToSave = maybeEmitEvent(new BeforeConvertEvent<>(objectToSave, collectionName)).getSource();

		AdaptibleEntity<T> entity = operations.forEntity(objectToSave, mongoConverter.getConversionService());
		entity.assertUpdateableIdIfNotSet();

		MappedDocument mapped = entity.toMappedDocument(writer);
		Document dbDoc = mapped.getDocument();

		maybeEmitEvent(new BeforeSaveEvent<>(objectToSave, dbDoc, collectionName));
		Object id = saveDocument(collectionName, dbDoc, objectToSave.getClass());

		T saved = entity.populateIdIfNecessary(id);
		maybeEmitEvent(new AfterSaveEvent<>(saved, dbDoc, collectionName));

		return saved;
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

				return operations.forEntity(document).getId();
			}
		});
	}

	protected List<Object> insertDocumentList(final String collectionName, final List<Document> documents) {

		if (documents.isEmpty()) {
			return Collections.emptyList();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting list of Documents containing {} items", documents.size());
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

	protected Object saveDocument(final String collectionName, final Document dbDoc, final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Saving Document containing fields: {}", dbDoc.keySet());
		}

		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.SAVE, collectionName, entityClass,
						dbDoc, null);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				MappedDocument mapped = MappedDocument.of(dbDoc);

				if (!mapped.hasId()) {
					if (writeConcernToUse == null) {
						collection.insertOne(dbDoc);
					} else {
						collection.withWriteConcern(writeConcernToUse).insertOne(dbDoc);
					}
				} else if (writeConcernToUse == null) {
					collection.replaceOne(mapped.getIdFilter(), dbDoc, new ReplaceOptions().upsert(true));
				} else {
					collection.withWriteConcern(writeConcernToUse).replaceOne(mapped.getIdFilter(), dbDoc,
							new ReplaceOptions().upsert(true));
				}
				return mapped.getId();
			}
		});
	}

	@Override
	public UpdateResult upsert(Query query, Update update, Class<?> entityClass) {
		return doUpdate(operations.determineCollectionName(entityClass), query, update, entityClass, true, false);
	}

	@Override
	public UpdateResult upsert(Query query, Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	@Override
	public UpdateResult upsert(Query query, Update update, Class<?> entityClass, String collectionName) {

		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, true, false);
	}

	@Override
	public UpdateResult updateFirst(Query query, Update update, Class<?> entityClass) {
		return doUpdate(operations.determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	@Override
	public UpdateResult updateFirst(final Query query, final Update update, final String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	@Override
	public UpdateResult updateFirst(Query query, Update update, Class<?> entityClass, String collectionName) {

		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	@Override
	public UpdateResult updateMulti(Query query, Update update, Class<?> entityClass) {
		return doUpdate(operations.determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	@Override
	public UpdateResult updateMulti(final Query query, final Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	@Override
	public UpdateResult updateMulti(final Query query, final Update update, Class<?> entityClass, String collectionName) {

		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	protected UpdateResult doUpdate(final String collectionName, final Query query, final Update update,
			@Nullable final Class<?> entityClass, final boolean upsert, final boolean multi) {

		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		return execute(collectionName, new CollectionCallback<UpdateResult>() {
			public UpdateResult doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);

				increaseVersionForUpdateIfNecessary(entity, update);

				UpdateOptions opts = new UpdateOptions();
				opts.upsert(upsert);

				Document queryObj = new Document();

				if (query != null) {

					queryObj.putAll(queryMapper.getMappedObject(query.getQueryObject(), entity));
					query.getCollation().map(Collation::toMongoCollation).ifPresent(opts::collation);
				}

				Document updateObj = updateMapper.getMappedObject(update.getUpdateObject(), entity);

				if (multi && update.isIsolated() && !queryObj.containsKey("$isolated")) {
					queryObj.put("$isolated", 1);
				}

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Calling update using query: {} and update: {} in collection: {}",
							serializeToJsonSafely(queryObj), serializeToJsonSafely(updateObj), collectionName);
				}

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName,
						entityClass, updateObj, queryObj);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				collection = writeConcernToUse != null ? collection.withWriteConcern(writeConcernToUse) : collection;

				if (!UpdateMapper.isUpdateObject(updateObj)) {

					ReplaceOptions replaceOptions = new ReplaceOptions();
					replaceOptions.collation(opts.getCollation());
					replaceOptions.upsert(opts.isUpsert());

					return collection.replaceOne(queryObj, updateObj, replaceOptions);
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

	private void increaseVersionForUpdateIfNecessary(@Nullable MongoPersistentEntity<?> persistentEntity, Update update) {

		if (persistentEntity != null && persistentEntity.hasVersionProperty()) {
			String versionFieldName = persistentEntity.getRequiredVersionProperty().getFieldName();
			if (!update.modifies(versionFieldName)) {
				update.inc(versionFieldName, 1L);
			}
		}
	}

	@Override
	public DeleteResult remove(Object object) {

		Assert.notNull(object, "Object must not be null!");

		Query query = operations.forEntity(object).getByIdQuery();

		return remove(query, object.getClass());
	}

	@Override
	public DeleteResult remove(Object object, String collectionName) {

		Assert.notNull(object, "Object must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		Query query = operations.forEntity(object).getByIdQuery();

		return doRemove(collectionName, query, object.getClass(), false);
	}

	@Override
	public DeleteResult remove(Query query, String collectionName) {
		return doRemove(collectionName, query, null, true);
	}

	@Override
	public DeleteResult remove(Query query, Class<?> entityClass) {
		return remove(query, entityClass, operations.determineCollectionName(entityClass));
	}

	@Override
	public DeleteResult remove(Query query, Class<?> entityClass, String collectionName) {

		Assert.notNull(entityClass, "EntityClass must not be null!");
		return doRemove(collectionName, query, entityClass, true);
	}

	protected <T> DeleteResult doRemove(final String collectionName, final Query query,
			@Nullable final Class<T> entityClass, boolean multi) {

		Assert.notNull(query, "Query must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		final MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);
		final Document queryObject = queryMapper.getMappedObject(query.getQueryObject(), entity);

		return execute(collectionName, new CollectionCallback<DeleteResult>() {

			public DeleteResult doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				maybeEmitEvent(new BeforeDeleteEvent<>(queryObject, entityClass, collectionName));

				Document removeQuery = queryObject;

				DeleteOptions options = new DeleteOptions();
				query.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName,
						entityClass, null, queryObject);

				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Remove using query: {} in collection: {}.",
							new Object[] { serializeToJsonSafely(removeQuery), collectionName });
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
						: collection.deleteOne(removeQuery, options);

				maybeEmitEvent(new AfterDeleteEvent<>(queryObject, entityClass, collectionName));

				return result;
			}
		});
	}

	@Override
	public <T> List<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, operations.determineCollectionName(entityClass));
	}

	@Override
	public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(new Document(), new Document()), null,
				new ReadDocumentCallback<>(mongoConverter, entityClass, collectionName), collectionName);
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
		mapReduce = mapReduce.sort(getMappedSortObject(query, domainType));

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
			LOGGER.debug("Executing Group with Document [{}]", serializeToJsonSafely(commandObject));
		}

		Document commandResult = executeCommand(commandObject, this.readPreference);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Group command result = [{}]", commandResult);
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

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.TypedAggregation, java.lang.Class)
	 */
	@Override
	public <O> AggregationResults<O> aggregate(TypedAggregation<?> aggregation, Class<O> outputType) {
		return aggregate(aggregation, operations.determineCollectionName(aggregation.getInputType()), outputType);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.TypedAggregation, java.lang.String, java.lang.Class)
	 */
	@Override
	public <O> AggregationResults<O> aggregate(TypedAggregation<?> aggregation, String inputCollectionName,
			Class<O> outputType) {

		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");

		AggregationOperationContext context = new TypeBasedAggregationOperationContext(aggregation.getInputType(),
				mappingContext, queryMapper);
		return aggregate(aggregation, inputCollectionName, outputType, context);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.Aggregation, java.lang.Class, java.lang.Class)
	 */
	@Override
	public <O> AggregationResults<O> aggregate(Aggregation aggregation, Class<?> inputType, Class<O> outputType) {

		return aggregate(aggregation, operations.determineCollectionName(inputType), outputType,
				new TypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.Aggregation, java.lang.String, java.lang.Class)
	 */
	@Override
	public <O> AggregationResults<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType) {
		return aggregate(aggregation, collectionName, outputType, null);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#aggregateStream(org.springframework.data.mongodb.core.aggregation.TypedAggregation, java.lang.String, java.lang.Class)
	 */
	@Override
	public <O> CloseableIterator<O> aggregateStream(TypedAggregation<?> aggregation, String inputCollectionName,
			Class<O> outputType) {

		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");

		AggregationOperationContext context = new TypeBasedAggregationOperationContext(aggregation.getInputType(),
				mappingContext, queryMapper);
		return aggregateStream(aggregation, inputCollectionName, outputType, context);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#aggregateStream(org.springframework.data.mongodb.core.aggregation.TypedAggregation, java.lang.Class)
	 */
	@Override
	public <O> CloseableIterator<O> aggregateStream(TypedAggregation<?> aggregation, Class<O> outputType) {
		return aggregateStream(aggregation, operations.determineCollectionName(aggregation.getInputType()), outputType);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#aggregateStream(org.springframework.data.mongodb.core.aggregation.Aggregation, java.lang.Class, java.lang.Class)
	 */
	@Override
	public <O> CloseableIterator<O> aggregateStream(Aggregation aggregation, Class<?> inputType, Class<O> outputType) {

		return aggregateStream(aggregation, operations.determineCollectionName(inputType), outputType,
				new TypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#aggregateStream(org.springframework.data.mongodb.core.aggregation.Aggregation, java.lang.String, java.lang.Class)
	 */
	@Override
	public <O> CloseableIterator<O> aggregateStream(Aggregation aggregation, String collectionName, Class<O> outputType) {
		return aggregateStream(aggregation, collectionName, outputType, null);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> findAllAndRemove(Query query, String collectionName) {
		return (List<T>) findAllAndRemove(query, Object.class, collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> List<T> findAllAndRemove(Query query, Class<T> entityClass) {
		return findAllAndRemove(query, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
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

		AggregationOperationContext contextToUse = new AggregationUtil(queryMapper, mappingContext)
				.prepareAggregationContext(aggregation, context);
		return doAggregate(aggregation, collectionName, outputType, contextToUse);
	}

	protected <O> AggregationResults<O> doAggregate(Aggregation aggregation, String collectionName, Class<O> outputType,
			AggregationOperationContext context) {

		DocumentCallback<O> callback = new UnwrapAndReadDocumentCallback<>(mongoConverter, outputType, collectionName);

		AggregationOptions options = aggregation.getOptions();
		AggregationUtil aggregationUtil = new AggregationUtil(queryMapper, mappingContext);

		if (options.isExplain()) {

			Document command = aggregationUtil.createCommand(collectionName, aggregation, context);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Executing aggregation: {}", serializeToJsonSafely(command));
			}

			Document commandResult = executeCommand(command);
			return new AggregationResults<>(commandResult.get("results", new ArrayList<Document>(0)).stream()
					.map(callback::doWith).collect(Collectors.toList()), commandResult);
		}

		List<Document> pipeline = aggregationUtil.createPipeline(aggregation, context);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Executing aggregation: {} in collection {}", serializeToJsonSafely(pipeline), collectionName);
		}

		return execute(collectionName, collection -> {

			List<Document> rawResult = new ArrayList<>();

			AggregateIterable<Document> aggregateIterable = collection.aggregate(pipeline, Document.class) //
					.collation(options.getCollation().map(Collation::toMongoCollation).orElse(null)) //
					.allowDiskUse(options.isAllowDiskUse());

			if (options.getCursorBatchSize() != null) {
				aggregateIterable = aggregateIterable.batchSize(options.getCursorBatchSize());
			}

			MongoIterable<O> iterable = aggregateIterable.map(val -> {

				rawResult.add(val);
				return callback.doWith(val);
			});

			return new AggregationResults<>(iterable.into(new ArrayList<>()),
					new Document("results", rawResult).append("ok", 1.0D));
		});
	}

	protected <O> CloseableIterator<O> aggregateStream(Aggregation aggregation, String collectionName,
			Class<O> outputType, @Nullable AggregationOperationContext context) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");
		Assert.notNull(outputType, "Output type must not be null!");
		Assert.isTrue(!aggregation.getOptions().isExplain(), "Can't use explain option with streaming!");

		AggregationUtil aggregationUtil = new AggregationUtil(queryMapper, mappingContext);
		AggregationOperationContext rootContext = aggregationUtil.prepareAggregationContext(aggregation, context);

		AggregationOptions options = aggregation.getOptions();
		List<Document> pipeline = aggregationUtil.createPipeline(aggregation, rootContext);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Streaming aggregation: {} in collection {}", serializeToJsonSafely(pipeline), collectionName);
		}

		ReadDocumentCallback<O> readCallback = new ReadDocumentCallback<>(mongoConverter, outputType, collectionName);

		return execute(collectionName, (CollectionCallback<CloseableIterator<O>>) collection -> {

			AggregateIterable<Document> cursor = collection.aggregate(pipeline, Document.class) //
					.allowDiskUse(options.isAllowDiskUse()) //
					.useCursor(true);

			if (options.getCursorBatchSize() != null) {
				cursor = cursor.batchSize(options.getCursorBatchSize());
			}

			if (options.getCollation().isPresent()) {
				cursor = cursor.collation(options.getCollation().map(Collation::toMongoCollation).get());
			}

			return new CloseableIterableCursorAdapter<>(cursor, exceptionTranslator, readCallback);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableFindOperation#query(java.lang.Class)
	 */
	@Override
	public <T> ExecutableFind<T> query(Class<T> domainType) {
		return new ExecutableFindOperationSupport(this).query(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation#update(java.lang.Class)
	 */
	@Override
	public <T> ExecutableUpdate<T> update(Class<T> domainType) {
		return new ExecutableUpdateOperationSupport(this).update(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation#remove(java.lang.Class)
	 */
	@Override
	public <T> ExecutableRemove<T> remove(Class<T> domainType) {
		return new ExecutableRemoveOperationSupport(this).remove(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableAggregationOperation#aggregateAndReturn(java.lang.Class)
	 */
	@Override
	public <T> ExecutableAggregation<T> aggregateAndReturn(Class<T> domainType) {
		return new ExecutableAggregationOperationSupport(this).aggregateAndReturn(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableAggregationOperation#aggregateAndReturn(java.lang.Class)
	 */
	@Override
	public <T> ExecutableMapReduce<T> mapReduce(Class<T> domainType) {
		return new ExecutableMapReduceOperationSupport(this).mapReduce(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#insert(java.lang.Class)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableInsertOperation#getCollectionNames()
	 */
	public Set<String> getCollectionNames() {
		return execute(new DbCallback<Set<String>>() {
			public Set<String> doInDB(MongoDatabase db) throws MongoException, DataAccessException {
				Set<String> result = new LinkedHashSet<>();
				for (String name : db.listCollectionNames()) {
					result.add(name);
				}
				return result;
			}
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

		if (null != eventPublisher) {
			eventPublisher.publishEvent(event);
		}

		return event;
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

				db.createCollection(collectionName, co);

				MongoCollection<Document> coll = db.getCollection(collectionName, Document.class);

				// TODO: Emit a collection created event
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Created collection [{}]",
							coll.getNamespace() != null ? coll.getNamespace().getCollectionName() : collectionName);
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
		Document mappedFields = queryMapper.getMappedObject(fields, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findOne using query: {} fields: {} for class: {} in collection: {}", serializeToJsonSafely(query),
					mappedFields, entityClass, collectionName);
		}

		return executeFindOneInternal(new FindOneCallback(mappedQuery, mappedFields),
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
	 * @param preparer allows for customization of the {@link DBCursor} used when iterating over the result set, (apply
	 *          limits, skips and so on).
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

		Document mappedFields = queryMapper.getMappedFields(fields, entity);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: {} fields: {} for class: {} in collection: {}",
					serializeToJsonSafely(mappedQuery), mappedFields, entityClass, collectionName);
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields), preparer, objectCallback,
				collectionName);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified targetClass while
	 * using sourceClass for mapping the query.
	 *
	 * @since 2.0
	 */
	<S, T> List<T> doFind(String collectionName, Document query, Document fields, Class<S> sourceClass,
			Class<T> targetClass, CursorPreparer preparer) {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(sourceClass);

		Document mappedFields = getMappedFieldsObject(fields, entity, targetClass);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: {} fields: {} for class: {} in collection: {}",
					serializeToJsonSafely(mappedQuery), mappedFields, sourceClass, collectionName);
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields), preparer,
				new ProjectingReadCallback<>(mongoConverter, sourceClass, targetClass, collectionName), collectionName);
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
	 * <p/>
	 * The query document is specified as a standard Document and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query document that specifies the criteria used to find a record
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> T doFindAndRemove(String collectionName, Document query, Document fields, Document sort,
			@Nullable Collation collation, Class<T> entityClass) {

		EntityReader<? super T, Bson> readerToUse = this.mongoConverter;

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAndRemove using query: {} fields: {} sort: {} for class: {} in collection: {}",
					serializeToJsonSafely(query), fields, sort, entityClass, collectionName);
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		return executeFindOneInternal(
				new FindAndRemoveCallback(queryMapper.getMappedObject(query, entity), fields, sort, collation),
				new ReadDocumentCallback<>(readerToUse, entityClass, collectionName), collectionName);
	}

	protected <T> T doFindAndModify(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass, Update update, @Nullable FindAndModifyOptions options) {

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

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					"findAndReplace using query: {} fields: {} sort: {} for class: {} and replacement: {} " + "in collection: {}",
					serializeToJsonSafely(mappedQuery), serializeToJsonSafely(mappedFields), serializeToJsonSafely(mappedSort),
					entityType, serializeToJsonSafely(replacement), collectionName);
		}

		maybeEmitEvent(new BeforeSaveEvent<>(replacement, replacement, collectionName));

		return executeFindOneInternal(
				new FindAndReplaceCallback(mappedQuery, mappedFields, mappedSort, replacement, collation, options),
				new ProjectingReadCallback<>(mongoConverter, entityType, resultType, collectionName), collectionName);
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
	@Nullable
	private <T> T executeFindOneInternal(CollectionCallback<Document> collectionCallback,
			DocumentCallback<T> objectCallback, String collectionName) {

		try {
			T result = objectCallback
					.doWith(collectionCallback.doInCollection(getAndPrepareCollection(doGetDatabase(), collectionName)));
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
	 * @param preparer the {@link CursorPreparer} to potentially modify the {@link DBCursor} before iterating over it
	 * @param objectCallback the {@link DocumentCallback} to transform {@link Document}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> List<T> executeFindMultiInternal(CollectionCallback<FindIterable<Document>> collectionCallback,
			@Nullable CursorPreparer preparer, DocumentCallback<T> objectCallback, String collectionName) {

		try {

			MongoCursor<Document> cursor = null;

			try {

				FindIterable<Document> iterable = collectionCallback
						.doInCollection(getAndPrepareCollection(doGetDatabase(), collectionName));

				if (preparer != null) {
					iterable = preparer.prepare(iterable);
				}

				cursor = iterable.iterator();

				List<T> result = new ArrayList<>();

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
			@Nullable CursorPreparer preparer, DocumentCallbackHandler callbackHandler, String collectionName) {

		try {

			MongoCursor<Document> cursor = null;

			try {
				FindIterable<Document> iterable = collectionCallback
						.doInCollection(getAndPrepareCollection(doGetDatabase(), collectionName));

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

	public PersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	@Nullable
	private MongoPersistentEntity<?> getPersistentEntity(@Nullable Class<?> type) {
		return type != null ? mappingContext.getPersistentEntity(type) : null;
	}

	private static MongoConverter getDefaultMongoConverter(MongoDbFactory factory) {

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
		MongoCustomConversions conversions = new MongoCustomConversions(Collections.emptyList());

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		return converter;
	}

	private Document getMappedSortObject(Query query, Class<?> type) {

		if (query == null || ObjectUtils.isEmpty(query.getSortObject())) {
			return null;
		}

		return queryMapper.getMappedSort(query.getSortObject(), mappingContext.getPersistentEntity(type));
	}

	private Document getMappedFieldsObject(Document fields, MongoPersistentEntity<?> entity, Class<?> targetType) {
		return queryMapper.getMappedFields(addFieldsForProjection(fields, entity.getType(), targetType), entity);
	}

	/**
	 * For cases where {@code fields} is {@literal null} or {@literal empty} add fields required for creating the
	 * projection (target) type if the {@code targetType} is a {@literal closed interface projection}.
	 *
	 * @param fields can be {@literal null}.
	 * @param domainType must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return {@link Document} with fields to be included.
	 */
	private Document addFieldsForProjection(Document fields, Class<?> domainType, Class<?> targetType) {

		if (!fields.isEmpty() || !targetType.isInterface() || ClassUtils.isAssignable(domainType, targetType)) {
			return fields;
		}

		ProjectionInformation projectionInformation = projectionFactory.getProjectionInformation(targetType);

		if (projectionInformation.isClosed()) {
			projectionInformation.getInputProperties().forEach(it -> fields.append(it.getName(), 1));
		}

		return fields;
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
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 * @author Christoph Strobl
	 */
	private static class FindOneCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Optional<Document> fields;

		public FindOneCallback(Document query, Document fields) {
			this.query = query;
			this.fields = Optional.of(fields).filter(it -> !ObjectUtils.isEmpty(fields));
		}

		public Document doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

			FindIterable<Document> iterable = collection.find(query, Document.class);

			if (LOGGER.isDebugEnabled()) {

				LOGGER.debug("findOne using query: {} fields: {} in db.collection: {}", serializeToJsonSafely(query),
						serializeToJsonSafely(fields.orElseGet(Document::new)),
						collection.getNamespace() != null ? collection.getNamespace().getFullName() : "n/a");
			}

			if (fields.isPresent()) {
				iterable = iterable.projection(fields.get());
			}

			return iterable.first();
		}
	}

	/**
	 * Simple {@link CollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 * @author Christoph Strobl
	 */
	private static class FindCallback implements CollectionCallback<FindIterable<Document>> {

		private final Document query;
		private final Document fields;

		public FindCallback(Document query, Document fields) {

			Assert.notNull(query, "Query must not be null!");
			Assert.notNull(fields, "Fields must not be null!");

			this.query = query;
			this.fields = fields;
		}

		public FindIterable<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			return collection.find(query, Document.class).projection(fields);
		}
	}

	/**
	 * Optimized {@link CollectionCallback} that takes an already mappend query and a nullable
	 * {@link com.mongodb.client.model.Collation} to execute a count query limited to one element.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	private static class ExistsCallback implements CollectionCallback<Boolean> {

		private final Document mappedQuery;
		private final com.mongodb.client.model.Collation collation;

		@Override
		public Boolean doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
			return collection.count(mappedQuery, new CountOptions().limit(1).collation(collation)) > 0;
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
		private final Optional<Collation> collation;

		public FindAndRemoveCallback(Document query, Document fields, Document sort, @Nullable Collation collation) {

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
			if (options.isReturnNew()) {
				opts.returnDocument(ReturnDocument.AFTER);
			}

			options.getCollation().map(Collation::toMongoCollation).ifPresent(opts::collation);

			return collection.findOneAndUpdate(query, update, opts);
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
				com.mongodb.client.model.Collation collation, FindAndReplaceOptions options) {

			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.options = options;
			this.collation = collation;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.CollectionCallback#doInCollection(com.mongodb.client.MongoCollection)
		 */
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

		@Nullable
		T doWith(@Nullable Document object);
	}

	/**
	 * Simple {@link DocumentCallback} that will transform {@link Document} into the given target type using the given
	 * {@link EntityReader}.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 */
	@RequiredArgsConstructor
	private class ReadDocumentCallback<T> implements DocumentCallback<T> {

		private final @NonNull EntityReader<? super T, Bson> reader;
		private final @NonNull Class<T> type;
		private final String collectionName;

		@Nullable
		public T doWith(@Nullable Document object) {

			if (null != object) {
				maybeEmitEvent(new AfterLoadEvent<>(object, type, collectionName));
			}

			T source = reader.read(type, object);

			if (null != source) {
				maybeEmitEvent(new AfterConvertEvent<>(object, source, collectionName));
			}

			return source;
		}
	}

	/**
	 * {@link DocumentCallback} transforming {@link Document} into the given {@code targetType} or decorating the
	 * {@code sourceType} with a {@literal projection} in case the {@code targetType} is an {@litera interface}.
	 *
	 * @param <S>
	 * @param <T>
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	private class ProjectingReadCallback<S, T> implements DocumentCallback<T> {

		private final @NonNull EntityReader<Object, Bson> reader;
		private final @NonNull Class<S> entityType;
		private final @NonNull Class<T> targetType;
		private final @NonNull String collectionName;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.MongoTemplate.DocumentCallback#doWith(org.bson.Document)
		 */
		@SuppressWarnings("unchecked")
		@Nullable
		public T doWith(@Nullable Document object) {

			if (object == null) {
				return null;
			}

			Class<?> typeToRead = targetType.isInterface() || targetType.isAssignableFrom(entityType) ? entityType
					: targetType;

			if (null != object) {
				maybeEmitEvent(new AfterLoadEvent<>(object, targetType, collectionName));
			}

			Object source = reader.read(typeToRead, object);
			Object result = targetType.isInterface() ? projectionFactory.createProjection(targetType, source) : source;

			if (null != result) {
				maybeEmitEvent(new AfterConvertEvent<>(object, result, collectionName));
			}

			return (T) result;
		}
	}

	class UnwrapAndReadDocumentCallback<T> extends ReadDocumentCallback<T> {

		public UnwrapAndReadDocumentCallback(EntityReader<? super T, Bson> reader, Class<T> type, String collectionName) {
			super(reader, type, collectionName);
		}

		@Override
		public T doWith(@Nullable Document object) {

			if (object == null) {
				return null;
			}

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

		private final @Nullable Query query;
		private final @Nullable Class<?> type;

		public QueryCursorPreparer(@Nullable Query query, @Nullable Class<?> type) {

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

			Meta meta = query.getMeta();
			if (query.getSkip() <= 0 && query.getLimit() <= 0 && ObjectUtils.isEmpty(query.getSortObject())
					&& !StringUtils.hasText(query.getHint()) && !meta.hasValues() && !query.getCollation().isPresent()) {
				return cursor;
			}

			FindIterable<Document> cursorToUse;

			cursorToUse = query.getCollation().map(Collation::toMongoCollation).map(cursor::collation).orElse(cursor);

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
					cursorToUse = cursorToUse.hint(Document.parse(query.getHint()));
				}

				if (meta.hasValues()) {

					if (StringUtils.hasText(meta.getComment())) {
						cursorToUse = cursorToUse.comment(meta.getComment());
					}

					if (meta.getSnapshot()) {
						cursorToUse = cursorToUse.snapshot(meta.getSnapshot());
					}

					if (meta.getMaxScan() != null) {
						cursorToUse = cursorToUse.maxScan(meta.getMaxScan());
					}

					if (meta.getMaxTimeMsec() != null) {
						cursorToUse = cursorToUse.maxTime(meta.getMaxTimeMsec(), TimeUnit.MILLISECONDS);
					}

					if (meta.getCursorBatchSize() != null) {
						cursorToUse = cursorToUse.batchSize(meta.getCursorBatchSize());
					}

					for (Meta.CursorOption option : meta.getFlags()) {

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

			Assert.notNull(delegate, "DocumentCallback must not be null!");

			this.delegate = delegate;
			this.metric = metric;
		}

		@Nullable
		public GeoResult<T> doWith(@Nullable Document object) {

			double distance = ((Double) object.get("dis")).doubleValue();
			Document content = (Document) object.get("obj");

			T doWith = delegate.doWith(content);

			return new GeoResult<>(doWith, new Distance(distance, metric));
		}
	}

	/**
	 * A {@link CloseableIterator} that is backed by a MongoDB {@link Cursor}.
	 *
	 * @author Thomas Darimont
	 * @since 1.7
	 */
	@AllArgsConstructor(access = AccessLevel.PACKAGE)
	static class CloseableIterableCursorAdapter<T> implements CloseableIterator<T> {

		private volatile @Nullable MongoCursor<Document> cursor;
		private PersistenceExceptionTranslator exceptionTranslator;
		private DocumentCallback<T> objectReadCallback;

		/**
		 * Creates a new {@link CloseableIterableCursorAdapter} backed by the given {@link Cursor}.
		 *
		 * @param cursor
		 * @param exceptionTranslator
		 * @param objectReadCallback
		 */
		public CloseableIterableCursorAdapter(MongoIterable<Document> cursor,
				PersistenceExceptionTranslator exceptionTranslator, DocumentCallback<T> objectReadCallback) {

			this.cursor = cursor.iterator();
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

	public MongoDbFactory getMongoDbFactory() {
		return mongoDbFactory;
	}

	/**
	 * {@link MongoTemplate} extension bound to a specific {@link ClientSession} that is applied when interacting with the
	 * server through the driver API.
	 * <p />
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.MongoTemplate#getCollection(java.lang.String)
		 */
		@Override
		public MongoCollection<Document> getCollection(String collectionName) {

			// native MongoDB objects that offer methods with ClientSession must not be proxied.
			return delegate.getCollection(collectionName);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.MongoTemplate#getDb()
		 */
		@Override
		public MongoDatabase getDb() {

			// native MongoDB objects that offer methods with ClientSession must not be proxied.
			return delegate.getDb();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.MongoTemplate#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public long count(Query query, @Nullable Class<?> entityClass, String collectionName) {

			if (!session.hasActiveTransaction()) {
				return super.count(query, entityClass, collectionName);
			}

			CountOptions options = new CountOptions();
			query.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);

			Document document = delegate.queryMapper.getMappedObject(query.getQueryObject(),
					Optional.ofNullable(entityClass).map(it -> delegate.mappingContext.getPersistentEntity(entityClass)));

			return execute(collectionName, collection -> collection.countDocuments(document, options));
		}
	}
}
