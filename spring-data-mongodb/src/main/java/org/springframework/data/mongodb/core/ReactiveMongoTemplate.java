/*
 * Copyright 2016-2018 the original author or authors.
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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Metric;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.EntityOperations.AdaptibleEntity;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.PrefixingDelegatingAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.*;
import org.springframework.data.mongodb.core.index.MongoMappingEventPublisher;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.index.ReactiveMongoPersistentEntityIndexCreator;
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
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.validation.Validator;
import org.springframework.data.projection.ProjectionInformation;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.util.Optionals;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.CursorType;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.*;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.*;

/**
 * Primary implementation of {@link ReactiveMongoOperations}. It simplifies the use of Reactive MongoDB usage and helps
 * to avoid common errors. It executes core MongoDB workflow, leaving application code to provide {@link Document} and
 * extract results. This class executes BSON queries or updates, initiating iteration over {@link FindPublisher} and
 * catching MongoDB exceptions and translating them to the generic, more informative exception hierarchy defined in the
 * org.springframework.dao package. Can be used within a service implementation via direct instantiation with a
 * {@link SimpleReactiveMongoDatabaseFactory} reference, or get prepared in an application context and given to services
 * as bean reference. Note: The {@link SimpleReactiveMongoDatabaseFactory} should always be configured as a bean in the
 * application context, in the first case given to the service directly, in the second case to the prepared template.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class ReactiveMongoTemplate implements ReactiveMongoOperations, ApplicationContextAware {

	public static final DbRefResolver NO_OP_REF_RESOLVER = NoOpDbRefResolver.INSTANCE;

	private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMongoTemplate.class);
	private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
	private static final Collection<Class<?>> ITERABLE_CLASSES;

	static {

		Set<Class<?>> iterableClasses = new HashSet<>();
		iterableClasses.add(List.class);
		iterableClasses.add(Collection.class);
		iterableClasses.add(Iterator.class);
		iterableClasses.add(Publisher.class);

		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
	}

	private final MongoConverter mongoConverter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final ReactiveMongoDatabaseFactory mongoDatabaseFactory;
	private final PersistenceExceptionTranslator exceptionTranslator;
	private final QueryMapper queryMapper;
	private final UpdateMapper updateMapper;
	private final JsonSchemaMapper schemaMapper;
	private final SpelAwareProxyProjectionFactory projectionFactory;
	private final ApplicationListener<MappingContextEvent<?, ?>> indexCreatorListener;
	private final EntityOperations operations;

	private @Nullable WriteConcern writeConcern;
	private WriteConcernResolver writeConcernResolver = DefaultWriteConcernResolver.INSTANCE;
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;
	private @Nullable ReadPreference readPreference;
	private @Nullable ApplicationEventPublisher eventPublisher;
	private @Nullable ReactiveMongoPersistentEntityIndexCreator indexCreator;

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 */
	public ReactiveMongoTemplate(MongoClient mongoClient, String databaseName) {
		this(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName), (MongoConverter) null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDatabaseFactory must not be {@literal null}.
	 */
	public ReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory) {
		this(mongoDatabaseFactory, (MongoConverter) null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDatabaseFactory must not be {@literal null}.
	 * @param mongoConverter can be {@literal null}.
	 */
	public ReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory,
			@Nullable MongoConverter mongoConverter) {
		this(mongoDatabaseFactory, mongoConverter, ReactiveMongoTemplate::handleSubscriptionException);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDatabaseFactory must not be {@literal null}.
	 * @param mongoConverter can be {@literal null}.
	 * @param subscriptionExceptionHandler exception handler called by {@link Flux#doOnError(Consumer)} on reactive type
	 *          materialization via {@link Publisher#subscribe(Subscriber)}. This callback is used during non-blocking
	 *          subscription of e.g. index creation {@link Publisher}s. Must not be {@literal null}.
	 * @since 2.1
	 */
	public ReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory,
			@Nullable MongoConverter mongoConverter, Consumer<Throwable> subscriptionExceptionHandler) {

		Assert.notNull(mongoDatabaseFactory, "ReactiveMongoDatabaseFactory must not be null!");

		this.mongoDatabaseFactory = mongoDatabaseFactory;
		this.exceptionTranslator = mongoDatabaseFactory.getExceptionTranslator();
		this.mongoConverter = mongoConverter == null ? getDefaultMongoConverter() : mongoConverter;
		this.queryMapper = new QueryMapper(this.mongoConverter);
		this.updateMapper = new UpdateMapper(this.mongoConverter);
		this.schemaMapper = new MongoJsonSchemaMapper(this.mongoConverter);
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
		this.indexCreatorListener = new IndexCreatorEventListener(subscriptionExceptionHandler);

		// We always have a mapping context in the converter, whether it's a simple one or not
		this.mappingContext = this.mongoConverter.getMappingContext();
		this.operations = new EntityOperations(this.mappingContext);

		// We create indexes based on mapping events
		if (this.mappingContext instanceof MongoMappingContext) {

			MongoMappingContext mongoMappingContext = (MongoMappingContext) this.mappingContext;
			this.indexCreator = new ReactiveMongoPersistentEntityIndexCreator(mongoMappingContext, this::indexOps);
			this.eventPublisher = new MongoMappingEventPublisher(this.indexCreatorListener);

			mongoMappingContext.setApplicationEventPublisher(this.eventPublisher);
			this.mappingContext.getPersistentEntities()
					.forEach(entity -> onCheckForIndexes(entity, subscriptionExceptionHandler));
		}
	}

	private ReactiveMongoTemplate(ReactiveMongoDatabaseFactory dbFactory, ReactiveMongoTemplate that) {

		this.mongoDatabaseFactory = dbFactory;
		this.exceptionTranslator = that.exceptionTranslator;
		this.mongoConverter = that.mongoConverter;
		this.queryMapper = that.queryMapper;
		this.updateMapper = that.updateMapper;
		this.schemaMapper = that.schemaMapper;
		this.projectionFactory = that.projectionFactory;
		this.indexCreator = that.indexCreator;
		this.indexCreatorListener = that.indexCreatorListener;
		this.mappingContext = that.mappingContext;
		this.operations = that.operations;
	}

	private void onCheckForIndexes(MongoPersistentEntity<?> entity, Consumer<Throwable> subscriptionExceptionHandler) {

		if (indexCreator != null) {
			indexCreator.checkForIndexes(entity).subscribe(v -> {}, subscriptionExceptionHandler);
		}
	}

	private static void handleSubscriptionException(Throwable t) {
		LOGGER.error("Unexpected exception during asynchronous execution", t);
	}

	/**
	 * Configures the {@link WriteResultChecking} to be used with the template. Setting {@literal null} will reset the
	 * default of {@link ReactiveMongoTemplate#DEFAULT_WRITE_RESULT_CHECKING}.
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
	 * @param writeConcern can be {@literal null}.
	 */
	public void setWriteConcern(@Nullable WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/**
	 * Configures the {@link WriteConcernResolver} to be used with the template.
	 *
	 * @param writeConcernResolver can be {@literal null}.
	 */
	public void setWriteConcernResolver(@Nullable WriteConcernResolver writeConcernResolver) {
		this.writeConcernResolver = writeConcernResolver;
	}

	/**
	 * Used by {@link {@link #prepareCollection(MongoCollection)} to set the {@link ReadPreference} before any operations
	 * are performed.
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

		projectionFactory.setBeanFactory(applicationContext);
		projectionFactory.setBeanClassLoader(applicationContext.getClassLoader());
	}

	/**
	 * Inspects the given {@link ApplicationContext} for {@link ReactiveMongoPersistentEntityIndexCreator} and those in
	 * turn if they were registered for the current {@link MappingContext}. If no creator for the current
	 * {@link MappingContext} can be found we manually add the internally created one as {@link ApplicationListener} to
	 * make sure indexes get created appropriately for entity types persisted through this {@link ReactiveMongoTemplate}
	 * instance.
	 *
	 * @param context must not be {@literal null}.
	 */
	private void prepareIndexCreator(ApplicationContext context) {

		String[] indexCreators = context.getBeanNamesForType(ReactiveMongoPersistentEntityIndexCreator.class);

		for (String creator : indexCreators) {
			ReactiveMongoPersistentEntityIndexCreator creatorBean = context.getBean(creator,
					ReactiveMongoPersistentEntityIndexCreator.class);
			if (creatorBean.isIndexCreatorFor(mappingContext)) {
				return;
			}
		}

		if (context instanceof ConfigurableApplicationContext) {
			((ConfigurableApplicationContext) context).addApplicationListener(indexCreatorListener);
		}
	}

	/**
	 * Returns the default {@link MongoConverter}.
	 *
	 * @return
	 */
	public MongoConverter getConverter() {
		return this.mongoConverter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#reactiveIndexOps(java.lang.String)
	 */
	public ReactiveIndexOperations indexOps(String collectionName) {
		return new DefaultReactiveIndexOperations(this, collectionName, this.queryMapper);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#reactiveIndexOps(java.lang.Class)
	 */
	public ReactiveIndexOperations indexOps(Class<?> entityClass) {
		return new DefaultReactiveIndexOperations(this, determineCollectionName(entityClass), this.queryMapper,
				entityClass);
	}

	public String getCollectionName(Class<?> entityClass) {
		return this.determineCollectionName(entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#executeCommand(java.lang.String)
	 */
	public Mono<Document> executeCommand(String jsonCommand) {

		Assert.notNull(jsonCommand, "Command must not be empty!");

		return executeCommand(Document.parse(jsonCommand));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#executeCommand(org.bson.Document)
	 */
	public Mono<Document> executeCommand(final Document command) {
		return executeCommand(command, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#executeCommand(org.bson.Document, com.mongodb.ReadPreference)
	 */
	public Mono<Document> executeCommand(final Document command, @Nullable ReadPreference readPreference) {

		Assert.notNull(command, "Command must not be null!");

		return createFlux(db -> readPreference != null ? db.runCommand(command, readPreference, Document.class)
				: db.runCommand(command, Document.class)).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#execute(java.lang.Class, org.springframework.data.mongodb.core.ReactiveCollectionCallback)
	 */
	@Override
	public <T> Flux<T> execute(Class<?> entityClass, ReactiveCollectionCallback<T> action) {
		return createFlux(determineCollectionName(entityClass), action);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#execute(org.springframework.data.mongodb.core.ReactiveDbCallback)
	 */
	@Override
	public <T> Flux<T> execute(ReactiveDatabaseCallback<T> action) {
		return createFlux(action);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#execute(java.lang.String, org.springframework.data.mongodb.core.ReactiveCollectionCallback)
	 */
	public <T> Flux<T> execute(String collectionName, ReactiveCollectionCallback<T> callback) {

		Assert.notNull(callback, "ReactiveCollectionCallback must not be null!");

		return createFlux(collectionName, callback);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#withSession(org.reactivestreams.Publisher, java.util.function.Consumer)
	 */
	@Override
	public ReactiveSessionScoped withSession(Publisher<ClientSession> sessionProvider) {

		Mono<ClientSession> cachedSession = Mono.from(sessionProvider).cache();

		return new ReactiveSessionScoped() {

			@Override
			public <T> Flux<T> execute(ReactiveSessionCallback<T> action, Consumer<ClientSession> doFinally) {

				return cachedSession.flatMapMany(session -> {

					return ReactiveMongoTemplate.this.withSession(action, session) //
							.doFinally(signalType -> {
								doFinally.accept(session);
							});
				});
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#inTransaction()
	 */
	@Override
	public ReactiveSessionScoped inTransaction() {
		return inTransaction(
				mongoDatabaseFactory.getSession(ClientSessionOptions.builder().causallyConsistent(true).build()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#inTransaction(org.reactivestreams.Publisher)
	 */
	@Override
	public ReactiveSessionScoped inTransaction(Publisher<ClientSession> sessionProvider) {

		Mono<ClientSession> cachedSession = Mono.from(sessionProvider).cache();

		return new ReactiveSessionScoped() {

			@Override
			public <T> Flux<T> execute(ReactiveSessionCallback<T> action, Consumer<ClientSession> doFinally) {

				return cachedSession.flatMapMany(session -> {

					if (!session.hasActiveTransaction()) {
						session.startTransaction();
					}

					return Flux.usingWhen(Mono.just(session), //
							s -> ReactiveMongoTemplate.this.withSession(action, s), //
							ClientSession::commitTransaction, //
							ClientSession::abortTransaction) //
							.doFinally(signalType -> doFinally.accept(session));
				});
			}
		};
	}

	private <T> Flux<T> withSession(ReactiveSessionCallback<T> action, ClientSession session) {

		ReactiveSessionBoundMongoTemplate operations = new ReactiveSessionBoundMongoTemplate(session,
				ReactiveMongoTemplate.this);

		return Flux.from(action.doInSession(operations)) //
				.subscriberContext(ctx -> ReactiveMongoContext.setSession(ctx, Mono.just(session)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#withSession(com.mongodb.session.ClientSession)
	 */
	public ReactiveMongoOperations withSession(ClientSession session) {
		return new ReactiveSessionBoundMongoTemplate(session, ReactiveMongoTemplate.this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#withSession(com.mongodb.ClientSessionOptions)
	 */
	@Override
	public ReactiveSessionScoped withSession(ClientSessionOptions sessionOptions) {
		return withSession(mongoDatabaseFactory.getSession(sessionOptions));
	}

	/**
	 * Create a reusable Flux for a {@link ReactiveDatabaseCallback}. It's up to the developer to choose to obtain a new
	 * {@link Flux} or to reuse the {@link Flux}.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Flux} wrapping the {@link ReactiveDatabaseCallback}.
	 */
	public <T> Flux<T> createFlux(ReactiveDatabaseCallback<T> callback) {

		Assert.notNull(callback, "ReactiveDatabaseCallback must not be null!");

		return Flux.defer(() -> callback.doInDB(prepareDatabase(doGetDatabase()))).onErrorMap(translateException());
	}

	/**
	 * Create a reusable Mono for a {@link ReactiveDatabaseCallback}. It's up to the developer to choose to obtain a new
	 * {@link Flux} or to reuse the {@link Flux}.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Mono} wrapping the {@link ReactiveDatabaseCallback}.
	 */
	public <T> Mono<T> createMono(final ReactiveDatabaseCallback<T> callback) {

		Assert.notNull(callback, "ReactiveDatabaseCallback must not be null!");

		return Mono.defer(() -> Mono.from(callback.doInDB(prepareDatabase(doGetDatabase()))))
				.onErrorMap(translateException());
	}

	/**
	 * Create a reusable {@link Flux} for the {@code collectionName} and {@link ReactiveCollectionCallback}.
	 *
	 * @param collectionName must not be empty or {@literal null}.
	 * @param callback must not be {@literal null}.
	 * @return a reusable {@link Flux} wrapping the {@link ReactiveCollectionCallback}.
	 */
	public <T> Flux<T> createFlux(String collectionName, ReactiveCollectionCallback<T> callback) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(callback, "ReactiveDatabaseCallback must not be null!");

		Mono<MongoCollection<Document>> collectionPublisher = Mono
				.fromCallable(() -> getAndPrepareCollection(doGetDatabase(), collectionName));

		return collectionPublisher.flatMapMany(callback::doInCollection).onErrorMap(translateException());
	}

	/**
	 * Create a reusable {@link Mono} for the {@code collectionName} and {@link ReactiveCollectionCallback}.
	 *
	 * @param collectionName must not be empty or {@literal null}.
	 * @param callback must not be {@literal null}.
	 * @param <T>
	 * @return a reusable {@link Mono} wrapping the {@link ReactiveCollectionCallback}.
	 */
	public <T> Mono<T> createMono(String collectionName, ReactiveCollectionCallback<T> callback) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(callback, "ReactiveCollectionCallback must not be null!");

		Mono<MongoCollection<Document>> collectionPublisher = Mono
				.fromCallable(() -> getAndPrepareCollection(doGetDatabase(), collectionName));

		return collectionPublisher.flatMap(collection -> Mono.from(callback.doInCollection(collection)))
				.onErrorMap(translateException());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.Class)
	 */
	public <T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass) {
		return createCollection(determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.Class, org.springframework.data.mongodb.core.CollectionOptions)
	 */
	public <T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass,
			@Nullable CollectionOptions collectionOptions) {
		return doCreateCollection(determineCollectionName(entityClass),
				convertToCreateCollectionOptions(collectionOptions, entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.String)
	 */
	public Mono<MongoCollection<Document>> createCollection(String collectionName) {
		return doCreateCollection(collectionName, new CreateCollectionOptions());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.String, org.springframework.data.mongodb.core.CollectionOptions)
	 */
	public Mono<MongoCollection<Document>> createCollection(String collectionName,
			@Nullable CollectionOptions collectionOptions) {
		return doCreateCollection(collectionName, convertToCreateCollectionOptions(collectionOptions));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#getCollection(java.lang.String)
	 */
	public MongoCollection<Document> getCollection(final String collectionName) {
		return execute((MongoDatabaseCallback<MongoCollection<Document>>) db -> db.getCollection(collectionName));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#collectionExists(java.lang.Class)
	 */
	public <T> Mono<Boolean> collectionExists(Class<T> entityClass) {
		return collectionExists(determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#collectionExists(java.lang.String)
	 */
	public Mono<Boolean> collectionExists(final String collectionName) {
		return createMono(db -> Flux.from(db.listCollectionNames()) //
				.filter(s -> s.equals(collectionName)) //
				.map(s -> true) //
				.single(false));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#dropCollection(java.lang.Class)
	 */
	public <T> Mono<Void> dropCollection(Class<T> entityClass) {
		return dropCollection(determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#dropCollection(java.lang.String)
	 */
	public Mono<Void> dropCollection(final String collectionName) {

		return createMono(collectionName, MongoCollection::drop).doOnSuccess(success -> {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Dropped collection [" + collectionName + "]");
			}
		}).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#getCollectionNames()
	 */
	public Flux<String> getCollectionNames() {
		return createFlux(MongoDatabase::listCollectionNames);
	}

	public MongoDatabase getMongoDatabase() {
		return doGetDatabase();
	}

	protected MongoDatabase doGetDatabase() {
		return mongoDatabaseFactory.getMongoDatabase();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findOne(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Mono<T> findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findOne(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findOne(Query query, Class<T> entityClass, String collectionName) {

		if (ObjectUtils.isEmpty(query.getSortObject())) {
			return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
					query.getCollation().orElse(null));
		}

		query.limit(1);
		return find(query, entityClass, collectionName).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#exists(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public Mono<Boolean> exists(Query query, Class<?> entityClass) {
		return exists(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#exists(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	public Mono<Boolean> exists(Query query, String collectionName) {
		return exists(query, null, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#exists(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public Mono<Boolean> exists(final Query query, @Nullable Class<?> entityClass, String collectionName) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to exist can't be null");
		}

		return createFlux(collectionName, collection -> {

			Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), getPersistentEntity(entityClass));
			FindPublisher<Document> findPublisher = collection.find(mappedQuery, Document.class)
					.projection(new Document("_id", 1));

			findPublisher = query.getCollation().map(Collation::toMongoCollation).map(findPublisher::collation)
					.orElse(findPublisher);

			return findPublisher.limit(1);
		}).hasElements();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#find(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Flux<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#find(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public <T> Flux<T> find(@Nullable Query query, Class<T> entityClass, String collectionName) {

		if (query == null) {
			return findAll(entityClass, collectionName);
		}

		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
				new QueryFindPublisherPreparer(query, entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findById(java.lang.Object, java.lang.Class)
	 */
	public <T> Mono<T> findById(Object id, Class<T> entityClass) {
		return findById(id, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findById(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findById(Object id, Class<T> entityClass, String collectionName) {

		String idKey = operations.getIdPropertyName(entityClass);

		return doFindOne(collectionName, new Document(idKey, id), null, entityClass, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findDistinct(org.springframework.data.mongodb.core.query.Query, java.lang.String, java.lang.Class, java.lang.Class)
	 */
	public <T> Flux<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass) {
		return findDistinct(query, field, determineCollectionName(entityClass), entityClass, resultClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findDistinct(org.springframework.data.mongodb.core.query.Query, java.lang.String, java.lang.String, java.lang.Class, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	public <T> Flux<T> findDistinct(Query query, String field, String collectionName, Class<?> entityClass,
			Class<T> resultClass) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(resultClass, "ResultClass must not be null!");

		MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);

		Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), entity);
		String mappedFieldName = queryMapper.getMappedFields(new Document(field, 1), entity).keySet().iterator().next();

		Class<T> mongoDriverCompatibleType = mongoDatabaseFactory.getCodecFor(resultClass) //
				.map(Codec::getEncoderClass) //
				.orElse((Class<T>) BsonValue.class);

		Flux<?> result = execute(collectionName, collection -> {

			DistinctPublisher<T> publisher = collection.distinct(mappedFieldName, mappedQuery, mongoDriverCompatibleType);

			return query.getCollation().map(Collation::toMongoCollation).map(publisher::collation).orElse(publisher);
		});

		if (resultClass == Object.class || mongoDriverCompatibleType != resultClass) {

			Class<?> targetType = getMostSpecificConversionTargetType(resultClass, entityClass, field);
			MongoConverter converter = getConverter();

			result = result.map(it -> converter.mapValueToTargetType(it, targetType, NO_OP_REF_RESOLVER));
		}

		return (Flux<T>) result;
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.TypedAggregation, java.lang.String, java.lang.Class)
	 */
	@Override
	public <O> Flux<O> aggregate(TypedAggregation<?> aggregation, String inputCollectionName, Class<O> outputType) {

		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");

		AggregationOperationContext context = new TypeBasedAggregationOperationContext(aggregation.getInputType(),
				mappingContext, queryMapper);
		return aggregate(aggregation, inputCollectionName, outputType, context);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.TypedAggregation, java.lang.Class)
	 */
	@Override
	public <O> Flux<O> aggregate(TypedAggregation<?> aggregation, Class<O> outputType) {
		return aggregate(aggregation, determineCollectionName(aggregation.getInputType()), outputType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.Aggregation, java.lang.Class, java.lang.Class)
	 */
	@Override
	public <O> Flux<O> aggregate(Aggregation aggregation, Class<?> inputType, Class<O> outputType) {

		return aggregate(aggregation, determineCollectionName(inputType), outputType,
				new TypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.Aggregation, java.lang.String, java.lang.Class)
	 */
	@Override
	public <O> Flux<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType) {
		return aggregate(aggregation, collectionName, outputType, null);
	}

	/**
	 * @param aggregation must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param outputType must not be {@literal null}.
	 * @param context can be {@literal null} and will be defaulted to {@link Aggregation#DEFAULT_CONTEXT}.
	 * @return never {@literal null}.
	 */
	protected <O> Flux<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType,
			@Nullable AggregationOperationContext context) {

		Assert.notNull(aggregation, "Aggregation pipeline must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(outputType, "Output type must not be null!");

		AggregationUtil aggregationUtil = new AggregationUtil(queryMapper, mappingContext);
		AggregationOperationContext rootContext = aggregationUtil.prepareAggregationContext(aggregation, context);

		AggregationOptions options = aggregation.getOptions();
		List<Document> pipeline = aggregationUtil.createPipeline(aggregation, rootContext);

		Assert.isTrue(!options.isExplain(), "Cannot use explain option with streaming!");

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Streaming aggregation: {} in collection {}", serializeToJsonSafely(pipeline), collectionName);
		}

		ReadDocumentCallback<O> readCallback = new ReadDocumentCallback<>(mongoConverter, outputType, collectionName);
		return execute(collectionName, collection -> aggregateAndMap(collection, pipeline, options, readCallback));
	}

	private <O> Flux<O> aggregateAndMap(MongoCollection<Document> collection, List<Document> pipeline,
			AggregationOptions options, ReadDocumentCallback<O> readCallback) {

		AggregatePublisher<Document> cursor = collection.aggregate(pipeline, Document.class)
				.allowDiskUse(options.isAllowDiskUse());

		if (options.getCursorBatchSize() != null) {
			cursor = cursor.batchSize(options.getCursorBatchSize());
		}

		if (options.getCollation().isPresent()) {
			cursor = cursor.collation(options.getCollation().map(Collation::toMongoCollation).get());
		}

		return Flux.from(cursor).map(readCallback::doWith);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#geoNear(org.springframework.data.mongodb.core.query.NearQuery, java.lang.Class)
	 */
	@Override
	public <T> Flux<GeoResult<T>> geoNear(NearQuery near, Class<T> entityClass) {
		return geoNear(near, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#geoNear(org.springframework.data.mongodb.core.query.NearQuery, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> Flux<GeoResult<T>> geoNear(NearQuery near, Class<T> entityClass, String collectionName) {
		return geoNear(near, entityClass, collectionName, entityClass);
	}

	@SuppressWarnings("unchecked")
	protected <T> Flux<GeoResult<T>> geoNear(NearQuery near, Class<?> entityClass, String collectionName,
			Class<T> returnType) {

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

		return Flux.defer(() -> {

			if (nearDbObject.containsKey("query")) {
				Document query = (Document) nearDbObject.get("query");
				command.put("query", queryMapper.getMappedObject(query, getPersistentEntity(entityClass)));
			}

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Executing geoNear using: {} for class: {} in collection: {}", serializeToJsonSafely(command),
						entityClass, collectionName);
			}

			GeoNearResultDbObjectCallback<T> callback = new GeoNearResultDbObjectCallback<>(
					new ProjectingReadCallback<>(mongoConverter, entityClass, returnType, collectionName), near.getMetric());

			return executeCommand(command, this.readPreference).flatMapMany(document -> {

				List<Document> results = document.get("results", List.class);

				return results == null ? Flux.empty() : Flux.fromIterable(results);

			}).skip(near.getSkip() != null ? near.getSkip() : 0).map(callback::doWith);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class)
	 */
	public <T> Mono<T> findAndModify(Query query, Update update, Class<T> entityClass) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndModify(Query query, Update update, Class<T> entityClass, String collectionName) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, org.springframework.data.mongodb.core.FindAndModifyOptions, java.lang.Class)
	 */
	public <T> Mono<T> findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass) {
		return findAndModify(query, update, options, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, org.springframework.data.mongodb.core.FindAndModifyOptions, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass,
			String collectionName) {

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
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndReplace(org.springframework.data.mongodb.core.query.Query, java.lang.Object, org.springframework.data.mongodb.core.FindAndReplaceOptions, java.lang.Class, java.lang.String, java.lang.Class)
	 */
	@Override
	public <S, T> Mono<T> findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType,
			String collectionName, Class<T> resultType) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(replacement, "Replacement must not be null!");
		Assert.notNull(options, "Options must not be null! Use FindAndReplaceOptions#empty() instead.");
		Assert.notNull(entityType, "Entity class must not be null!");
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass, String collectionName) {

		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), query.getCollation().orElse(null), entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public Mono<Long> count(Query query, Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null!");

		return count(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	public Mono<Long> count(final Query query, String collectionName) {
		return count(query, null, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public Mono<Long> count(Query query, @Nullable Class<?> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		return createMono(collectionName, collection -> {

			final Document Document = query == null ? null
					: queryMapper.getMappedObject(query.getQueryObject(),
							entityClass == null ? null : mappingContext.getPersistentEntity(entityClass));

			CountOptions options = new CountOptions();
			if (query != null) {
				query.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);
			}

			return collection.count(Document, options);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(reactor.core.publisher.Mono)
	 */
	@Override
	public <T> Mono<T> insert(Mono<? extends T> objectToSave) {

		Assert.notNull(objectToSave, "Mono to insert must not be null!");

		return objectToSave.flatMap(this::insert);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(reactor.core.publisher.Mono, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> batchToSave, Class<?> entityClass) {
		return insertAll(batchToSave, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(reactor.core.publisher.Mono, java.lang.String)
	 */
	@Override
	public <T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> batchToSave, String collectionName) {

		Assert.notNull(batchToSave, "Batch to insert must not be null!");

		return Flux.from(batchToSave).flatMap(collection -> insert(collection, collectionName));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.lang.Object)
	 */
	public <T> Mono<T> insert(T objectToSave) {

		Assert.notNull(objectToSave, "Object to insert must not be null!");

		ensureNotIterable(objectToSave);
		return insert(objectToSave, determineEntityCollectionName(objectToSave));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.lang.Object, java.lang.String)
	 */
	public <T> Mono<T> insert(T objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "Object to insert must not be null!");

		ensureNotIterable(objectToSave);
		return doInsert(collectionName, objectToSave, this.mongoConverter);
	}

	protected <T> Mono<T> doInsert(String collectionName, T objectToSave, MongoWriter<Object> writer) {

		assertUpdateableIdIfNotSet(objectToSave);

		return Mono.defer(() -> {

			AdaptibleEntity<T> entity = operations.forEntity(objectToSave, mongoConverter.getConversionService());
			T toSave = entity.initializeVersionProperty();

			maybeEmitEvent(new BeforeConvertEvent<>(toSave, collectionName));

			Document dbDoc = entity.toMappedDocument(writer).getDocument();

			maybeEmitEvent(new BeforeSaveEvent<>(toSave, dbDoc, collectionName));

			Mono<T> afterInsert = insertDBObject(collectionName, dbDoc, toSave.getClass()).map(id -> {

				T saved = entity.populateIdIfNecessary(id);
				maybeEmitEvent(new AfterSaveEvent<>(saved, dbDoc, collectionName));
				return saved;
			});

			return afterInsert;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.util.Collection, java.lang.Class)
	 */
	public <T> Flux<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass) {
		return doInsertBatch(determineCollectionName(entityClass), batchToSave, this.mongoConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.util.Collection, java.lang.String)
	 */
	public <T> Flux<T> insert(Collection<? extends T> batchToSave, String collectionName) {
		return doInsertBatch(collectionName, batchToSave, this.mongoConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insertAll(java.util.Collection)
	 */
	public <T> Flux<T> insertAll(Collection<? extends T> objectsToSave) {
		return doInsertAll(objectsToSave, this.mongoConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insertAll(reactor.core.publisher.Mono)
	 */
	@Override
	public <T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> objectsToSave) {
		return Flux.from(objectsToSave).flatMap(this::insertAll);
	}

	protected <T> Flux<T> doInsertAll(Collection<? extends T> listToSave, MongoWriter<Object> writer) {

		final Map<String, List<T>> elementsByCollection = new HashMap<>();

		listToSave.forEach(element -> {

			MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(element.getClass());

			String collection = entity.getCollection();
			List<T> collectionElements = elementsByCollection.computeIfAbsent(collection, k -> new ArrayList<>());

			collectionElements.add(element);
		});

		return Flux.fromIterable(elementsByCollection.keySet())
				.flatMap(collectionName -> doInsertBatch(collectionName, elementsByCollection.get(collectionName), writer));
	}

	protected <T> Flux<T> doInsertBatch(final String collectionName, final Collection<? extends T> batchToSave,
			final MongoWriter<Object> writer) {

		Assert.notNull(writer, "MongoWriter must not be null!");

		Mono<List<Tuple2<AdaptibleEntity<T>, Document>>> prepareDocuments = Flux.fromIterable(batchToSave).map(o -> {

			AdaptibleEntity<T> entity = operations.forEntity(o, mongoConverter.getConversionService());
			T toSave = entity.initializeVersionProperty();

			BeforeConvertEvent<T> event = new BeforeConvertEvent<>(toSave, collectionName);
			toSave = maybeEmitEvent(event).getSource();

			Document dbDoc = entity.toMappedDocument(writer).getDocument();

			maybeEmitEvent(new BeforeSaveEvent<>(toSave, dbDoc, collectionName));
			return Tuples.of(entity, dbDoc);
		}).collectList();

		Flux<Tuple2<AdaptibleEntity<T>, Document>> insertDocuments = prepareDocuments.flatMapMany(tuples -> {

			List<Document> dbObjects = tuples.stream().map(Tuple2::getT2).collect(Collectors.toList());

			return insertDocumentList(collectionName, dbObjects).thenMany(Flux.fromIterable(tuples));
		});

		return insertDocuments.map(tuple -> {

			Object id = MappedDocument.of(tuple.getT2()).getId();

			T saved = tuple.getT1().populateIdIfNecessary(id);
			maybeEmitEvent(new AfterSaveEvent<>(saved, tuple.getT2(), collectionName));
			return saved;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#save(reactor.core.publisher.Mono)
	 */
	@Override
	public <T> Mono<T> save(Mono<? extends T> objectToSave) {

		Assert.notNull(objectToSave, "Mono to save must not be null!");

		return objectToSave.flatMap(this::save);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#save(reactor.core.publisher.Mono, java.lang.String)
	 */
	@Override
	public <T> Mono<T> save(Mono<? extends T> objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "Mono to save must not be null!");

		return objectToSave.flatMap(o -> save(o, collectionName));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#save(java.lang.Object)
	 */
	public <T> Mono<T> save(T objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		return save(objectToSave, determineEntityCollectionName(objectToSave));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#save(java.lang.Object, java.lang.String)
	 */
	public <T> Mono<T> save(T objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		MongoPersistentEntity<?> mongoPersistentEntity = getPersistentEntity(objectToSave.getClass());

		// No optimistic locking -> simple save
		if (mongoPersistentEntity == null || !mongoPersistentEntity.hasVersionProperty()) {
			return doSave(collectionName, objectToSave, this.mongoConverter);
		}

		return doSaveVersioned(objectToSave, mongoPersistentEntity, collectionName);
	}

	private <T> Mono<T> doSaveVersioned(T objectToSave, MongoPersistentEntity<?> entity, String collectionName) {

		AdaptibleEntity<T> forEntity = operations.forEntity(objectToSave, mongoConverter.getConversionService());

		return createMono(collectionName, collection -> {

			Number versionNumber = forEntity.getVersion();

			// Fresh instance -> initialize version property
			if (versionNumber == null) {
				return doInsert(collectionName, objectToSave, mongoConverter);
			}

			forEntity.assertUpdateableIdIfNotSet();

			Query query = forEntity.getQueryForVersion();

			T toSave = forEntity.incrementVersion();

			BeforeConvertEvent<T> event = new BeforeConvertEvent<>(toSave, collectionName);
			T afterEvent = ReactiveMongoTemplate.this.maybeEmitEvent(event).getSource();

			MappedDocument mapped = operations.forEntity(toSave).toMappedDocument(mongoConverter);
			Document document = mapped.getDocument();

			ReactiveMongoTemplate.this.maybeEmitEvent(new BeforeSaveEvent<>(afterEvent, document, collectionName));

			return doUpdate(collectionName, query, mapped.updateWithoutId(), afterEvent.getClass(), false, false)
					.map(updateResult -> maybeEmitEvent(new AfterSaveEvent<T>(afterEvent, document, collectionName)).getSource());
		});
	}

	protected <T> Mono<T> doSave(String collectionName, T objectToSave, MongoWriter<Object> writer) {

		assertUpdateableIdIfNotSet(objectToSave);

		return createMono(collectionName, collection -> {

			T toSave = maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName)).getSource();

			AdaptibleEntity<T> entity = operations.forEntity(toSave, mongoConverter.getConversionService());
			Document dbDoc = entity.toMappedDocument(writer).getDocument();
			maybeEmitEvent(new BeforeSaveEvent<T>(toSave, dbDoc, collectionName));

			return saveDocument(collectionName, dbDoc, toSave.getClass()).map(id -> {

				T saved = entity.populateIdIfNecessary(id);
				return maybeEmitEvent(new AfterSaveEvent<>(saved, dbDoc, collectionName)).getSource();
			});
		});
	}

	protected Mono<Object> insertDBObject(final String collectionName, final Document dbDoc, final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting Document containing fields: " + dbDoc.keySet() + " in collection: " + collectionName);
		}

		Document document = new Document(dbDoc);

		Flux<Success> execute = execute(collectionName, collection -> {

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT, collectionName, entityClass,
					dbDoc, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

			MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

			return collectionToUse.insertOne(document);
		});

		return Flux.from(execute).last().map(success -> MappedDocument.of(document).getId());
	}

	protected Flux<ObjectId> insertDocumentList(final String collectionName, final List<Document> dbDocList) {

		if (dbDocList.isEmpty()) {
			return Flux.empty();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting list of DBObjects containing " + dbDocList.size() + " items");
		}

		final List<Document> documents = new ArrayList<>();

		return execute(collectionName, collection -> {

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT_LIST, collectionName, null,
					null, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
			MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

			documents.addAll(toDocuments(dbDocList));

			return collectionToUse.insertMany(documents);

		}).flatMap(s -> {

			return Flux.fromStream(documents.stream() //
					.map(MappedDocument::of) //
					.filter(it -> it.isIdPresent(ObjectId.class)) //
					.map(it -> it.getId(ObjectId.class)));
		});
	}

	private MongoCollection<Document> prepareCollection(MongoCollection<Document> collection,
			@Nullable WriteConcern writeConcernToUse) {
		MongoCollection<Document> collectionToUse = collection;

		if (writeConcernToUse != null) {
			collectionToUse = collectionToUse.withWriteConcern(writeConcernToUse);
		}
		return collectionToUse;
	}

	protected Mono<Object> saveDocument(final String collectionName, final Document document,
			final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Saving Document containing fields: " + document.keySet());
		}

		return createMono(collectionName, collection -> {

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.SAVE, collectionName, entityClass,
					document, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
			MappedDocument mapped = MappedDocument.of(document);

			MongoCollection<Document> collectionToUse = writeConcernToUse == null //
					? collection //
					: collection.withWriteConcern(writeConcernToUse);

			Publisher<?> publisher = !mapped.hasId() //
					? collectionToUse.insertOne(document) //
					: collectionToUse.replaceOne(mapped.getIdFilter(), document, new ReplaceOptions().upsert(true));

			return Mono.from(publisher).map(o -> mapped.getId());
		});

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class)
	 */
	public Mono<UpdateResult> upsert(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, true, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.String)
	 */
	public Mono<UpdateResult> upsert(Query query, Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> upsert(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, true, false);
	}

	/*
	 * (non-Javadoc))
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class)
	 */
	public Mono<UpdateResult> updateFirst(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.String)
	 */
	public Mono<UpdateResult> updateFirst(final Query query, final Update update, final String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> updateFirst(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class)
	 */
	public Mono<UpdateResult> updateMulti(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.String)
	 */
	public Mono<UpdateResult> updateMulti(final Query query, final Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> updateMulti(final Query query, final Update update, Class<?> entityClass,
			String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	protected Mono<UpdateResult> doUpdate(final String collectionName, Query query, @Nullable Update update,
			@Nullable Class<?> entityClass, final boolean upsert, final boolean multi) {

		MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);

		Flux<UpdateResult> result = execute(collectionName, collection -> {

			increaseVersionForUpdateIfNecessary(entity, update);

			Document queryObj = queryMapper.getMappedObject(query.getQueryObject(), entity);
			Document updateObj = update == null ? new Document()
					: updateMapper.getMappedObject(update.getUpdateObject(), entity);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Calling update using query: %s and update: %s in collection: %s",
						serializeToJsonSafely(queryObj), serializeToJsonSafely(updateObj), collectionName));
			}

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName, entityClass,
					updateObj, queryObj);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
			MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

			UpdateOptions updateOptions = new UpdateOptions().upsert(upsert);
			query.getCollation().map(Collation::toMongoCollation).ifPresent(updateOptions::collation);

			if (!UpdateMapper.isUpdateObject(updateObj)) {

				ReplaceOptions replaceOptions = new ReplaceOptions();
				replaceOptions.upsert(updateOptions.isUpsert());
				replaceOptions.collation(updateOptions.getCollation());

				return collectionToUse.replaceOne(queryObj, updateObj, replaceOptions);
			}
			if (multi) {
				return collectionToUse.updateMany(queryObj, updateObj, updateOptions);
			}
			return collectionToUse.updateOne(queryObj, updateObj, updateOptions);
		}).doOnNext(updateResult -> {

			if (entity != null && entity.hasVersionProperty() && !multi) {
				if (updateResult.wasAcknowledged() && updateResult.getMatchedCount() == 0) {

					Document queryObj = query == null ? new Document()
							: queryMapper.getMappedObject(query.getQueryObject(), entity);
					Document updateObj = update == null ? new Document()
							: updateMapper.getMappedObject(update.getUpdateObject(), entity);
					if (dbObjectContainsVersionProperty(queryObj, entity))
						throw new OptimisticLockingFailureException("Optimistic lock exception on saving entity: "
								+ updateObj.toString() + " to collection " + collectionName);
				}
			}
		});

		return result.next();
	}

	private void increaseVersionForUpdateIfNecessary(@Nullable MongoPersistentEntity<?> persistentEntity, Update update) {

		if (persistentEntity != null && persistentEntity.hasVersionProperty()) {
			String versionFieldName = persistentEntity.getRequiredVersionProperty().getFieldName();
			if (!update.modifies(versionFieldName)) {
				update.inc(versionFieldName, 1L);
			}
		}
	}

	private boolean dbObjectContainsVersionProperty(Document document,
			@Nullable MongoPersistentEntity<?> persistentEntity) {

		if (persistentEntity == null || !persistentEntity.hasVersionProperty()) {
			return false;
		}

		return document.containsKey(persistentEntity.getRequiredIdProperty().getFieldName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(reactor.core.publisher.Mono)
	 */
	@Override
	public Mono<DeleteResult> remove(Mono<? extends Object> objectToRemove) {
		return objectToRemove.flatMap(this::remove);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(reactor.core.publisher.Mono, java.lang.String)
	 */
	@Override
	public Mono<DeleteResult> remove(Mono<? extends Object> objectToRemove, String collectionName) {
		return objectToRemove.flatMap(it -> remove(it, collectionName));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(java.lang.Object)
	 */
	public Mono<DeleteResult> remove(Object object) {

		Assert.notNull(object, "Object must not be null!");

		return remove(operations.forEntity(object).getByIdQuery(), object.getClass());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(java.lang.Object, java.lang.String)
	 */
	public Mono<DeleteResult> remove(Object object, String collectionName) {

		Assert.notNull(object, "Object must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		return doRemove(collectionName, operations.forEntity(object).getByIdQuery(), object.getClass());
	}

	private void assertUpdateableIdIfNotSet(Object value) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(value.getClass());

		if (entity != null && entity.hasIdProperty()) {

			MongoPersistentProperty property = entity.getRequiredIdProperty();
			Object propertyValue = entity.getPropertyAccessor(value).getProperty(property);

			if (propertyValue != null) {
				return;
			}

			if (!MongoSimpleTypes.AUTOGENERATED_ID_TYPES.contains(property.getType())) {
				throw new InvalidDataAccessApiUsageException(
						String.format("Cannot autogenerate id of type %s for entity of type %s!", property.getType().getName(),
								value.getClass().getName()));
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	public Mono<DeleteResult> remove(Query query, String collectionName) {
		return remove(query, null, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public Mono<DeleteResult> remove(Query query, Class<?> entityClass) {
		return remove(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public Mono<DeleteResult> remove(Query query, @Nullable Class<?> entityClass, String collectionName) {
		return doRemove(collectionName, query, entityClass);
	}

	protected <T> Mono<DeleteResult> doRemove(String collectionName, Query query, @Nullable Class<T> entityClass) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to remove can't be null!");
		}

		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		final Document queryObject = query.getQueryObject();
		final MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);

		return execute(collectionName, collection -> {

			Document removeQuey = queryMapper.getMappedObject(queryObject, entity);

			maybeEmitEvent(new BeforeDeleteEvent<>(removeQuey, entityClass, collectionName));

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName, entityClass,
					null, removeQuey);

			final DeleteOptions deleteOptions = new DeleteOptions();
			query.getCollation().map(Collation::toMongoCollation).ifPresent(deleteOptions::collation);

			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
			MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Remove using query: {} in collection: {}.",
						new Object[] { serializeToJsonSafely(removeQuey), collectionName });
			}

			if (query.getLimit() > 0 || query.getSkip() > 0) {

				FindPublisher<Document> cursor = new QueryFindPublisherPreparer(query, entityClass)
						.prepare(collection.find(removeQuey)) //
						.projection(MappedDocument.getIdOnlyProjection());

				return Flux.from(cursor) //
						.map(MappedDocument::of) //
						.map(MappedDocument::getId) //
						.collectList() //
						.flatMapMany(val -> {
							return collectionToUse.deleteMany(MappedDocument.getIdIn(val), deleteOptions);
						});
			} else {
				return collectionToUse.deleteMany(removeQuey, deleteOptions);
			}

		}).doOnNext(deleteResult -> maybeEmitEvent(new AfterDeleteEvent<>(queryObject, entityClass, collectionName)))
				.next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAll(java.lang.Class)
	 */
	public <T> Flux<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAll(java.lang.Class, java.lang.String)
	 */
	public <T> Flux<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(null), null,
				new ReadDocumentCallback<>(mongoConverter, entityClass, collectionName), collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Flux<T> findAllAndRemove(Query query, String collectionName) {
		return (Flux<T>) findAllAndRemove(query, Object.class, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass) {
		return findAllAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName) {
		return doFindAndDelete(collectionName, query, entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#tail(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> tail(Query query, Class<T> entityClass) {
		return tail(query, entityClass, determineCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#tail(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> Flux<T> tail(@Nullable Query query, Class<T> entityClass, String collectionName) {

		if (query == null) {

			// TODO: clean up
			LOGGER.debug(String.format("find for class: %s in collection: %s", entityClass, collectionName));

			return executeFindMultiInternal(
					collection -> new FindCallback(null).doInCollection(collection).cursorType(CursorType.TailableAwait), null,
					new ReadDocumentCallback<>(mongoConverter, entityClass, collectionName), collectionName);
		}

		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
				new TailingQueryFindPublisherPreparer(query, entityClass));
	}

	@Override
	public <T> Flux<ChangeStreamEvent<T>> changeStream(@Nullable String database, @Nullable String collectionName,
			ChangeStreamOptions options, Class<T> targetType) {

		List<Document> filter = prepareFilter(options);
		FullDocument fullDocument = ClassUtils.isAssignable(Document.class, targetType) ? FullDocument.DEFAULT
				: FullDocument.UPDATE_LOOKUP;

		MongoDatabase db = StringUtils.hasText(database) ? mongoDatabaseFactory.getMongoDatabase(database)
				: getMongoDatabase();

		ChangeStreamPublisher<Document> publisher;
		if (StringUtils.hasText(collectionName)) {
			publisher = filter.isEmpty() ? db.getCollection(collectionName).watch(Document.class)
					: db.getCollection(collectionName).watch(filter, Document.class);

		} else {
			publisher = filter.isEmpty() ? db.watch(Document.class) : db.watch(filter, Document.class);
		}

		publisher = options.getResumeToken().map(BsonValue::asDocument).map(publisher::resumeAfter).orElse(publisher);
		publisher = options.getCollation().map(Collation::toMongoCollation).map(publisher::collation).orElse(publisher);
		publisher = options.getResumeTimestamp().map(it -> new BsonTimestamp(it.toEpochMilli()))
				.map(publisher::startAtOperationTime).orElse(publisher);
		publisher = publisher.fullDocument(options.getFullDocumentLookup().orElse(fullDocument));

		return Flux.from(publisher).map(document -> new ChangeStreamEvent<>(document, targetType, getConverter()));
	}

	List<Document> prepareFilter(ChangeStreamOptions options) {

		Object filter = options.getFilter().orElse(Collections.emptyList());

		if (filter instanceof Aggregation) {
			Aggregation agg = (Aggregation) filter;
			AggregationOperationContext context = agg instanceof TypedAggregation
					? new TypeBasedAggregationOperationContext(((TypedAggregation<?>) agg).getInputType(),
							getConverter().getMappingContext(), queryMapper)
					: Aggregation.DEFAULT_CONTEXT;

			return agg.toPipeline(new PrefixingDelegatingAggregationOperationContext(context, "fullDocument",
					Arrays.asList("operationType", "fullDocument", "documentKey", "updateDescription", "ns")));
		}

		if (filter instanceof List) {
			return (List<Document>) filter;
		}

		throw new IllegalArgumentException(
				"ChangeStreamRequestOptions.filter mut be either an Aggregation or a plain list of Documents");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#mapReduce(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.Class, java.lang.String, java.lang.String, org.springframework.data.mongodb.core.mapreduce.MapReduceOptions)
	 */
	public <T> Flux<T> mapReduce(Query filterQuery, Class<?> domainType, Class<T> resultType, String mapFunction,
			String reduceFunction, MapReduceOptions options) {

		return mapReduce(filterQuery, domainType, determineCollectionName(domainType), resultType, mapFunction,
				reduceFunction, options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#mapReduce(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String, java.lang.Class, java.lang.String, java.lang.String, org.springframework.data.mongodb.core.mapreduce.MapReduceOptions)
	 */
	public <T> Flux<T> mapReduce(Query filterQuery, Class<?> domainType, String inputCollectionName, Class<T> resultType,
			String mapFunction, String reduceFunction, MapReduceOptions options) {

		Assert.notNull(filterQuery, "Filter query must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");
		Assert.hasText(inputCollectionName, "Input collection name must not be null or empty!");
		Assert.notNull(resultType, "Result type must not be null!");
		Assert.notNull(mapFunction, "Map function must not be null!");
		Assert.notNull(reduceFunction, "Reduce function must not be null!");
		Assert.notNull(options, "MapReduceOptions must not be null!");

		assertLocalFunctionNames(mapFunction, reduceFunction);

		return createFlux(inputCollectionName, collection -> {

			Document mappedQuery = queryMapper.getMappedObject(filterQuery.getQueryObject(),
					mappingContext.getPersistentEntity(domainType));

			MapReducePublisher<Document> publisher = collection.mapReduce(mapFunction, reduceFunction, Document.class);

			if (StringUtils.hasText(options.getOutputCollection())) {
				publisher = publisher.collectionName(options.getOutputCollection());
			}

			publisher.filter(mappedQuery);
			publisher.sort(getMappedSortObject(filterQuery, domainType));

			if (filterQuery.getMeta().getMaxTimeMsec() != null) {
				publisher.maxTime(filterQuery.getMeta().getMaxTimeMsec(), TimeUnit.MILLISECONDS);
			}

			if (filterQuery.getLimit() > 0 || (options.getLimit() != null)) {

				if (filterQuery.getLimit() > 0 && (options.getLimit() != null)) {
					throw new IllegalArgumentException(
							"Both Query and MapReduceOptions define a limit. Please provide the limit only via one of the two.");
				}

				if (filterQuery.getLimit() > 0) {
					publisher.limit(filterQuery.getLimit());
				}

				if (options.getLimit() != null) {
					publisher.limit(options.getLimit());
				}
			}

			Optional<Collation> collation = filterQuery.getCollation();

			Optionals.ifAllPresent(filterQuery.getCollation(), options.getCollation(), (l, r) -> {
				throw new IllegalArgumentException(
						"Both Query and MapReduceOptions define a collation. Please provide the collation only via one of the two.");
			});

			if (options.getCollation().isPresent()) {
				collation = options.getCollation();
			}

			if (!CollectionUtils.isEmpty(options.getScopeVariables())) {
				publisher = publisher.scope(new Document(options.getScopeVariables()));
			}
			if (options.getLimit() != null && options.getLimit() > 0) {
				publisher = publisher.limit(options.getLimit());
			}
			if (options.getFinalizeFunction().filter(StringUtils::hasText).isPresent()) {
				publisher = publisher.finalizeFunction(options.getFinalizeFunction().get());
			}
			if (options.getJavaScriptMode() != null) {
				publisher = publisher.jsMode(options.getJavaScriptMode());
			}
			if (options.getOutputSharded().isPresent()) {
				publisher = publisher.sharded(options.getOutputSharded().get());
			}

			publisher = collation.map(Collation::toMongoCollation).map(publisher::collation).orElse(publisher);

			return Flux.from(publisher)
					.map(new ReadDocumentCallback<>(mongoConverter, resultType, inputCollectionName)::doWith);
		});
	}

	private static void assertLocalFunctionNames(String... functions) {

		for (String function : functions) {

			if (ResourceUtils.isUrl(function)) {

				throw new IllegalArgumentException(String.format(
						"Blocking accessing to resource %s is not allowed using reactive infrastructure. You may load the resource at startup and cache its value.",
						function));
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveFindOperation#query(java.lang.Class)
	 */
	@Override
	public <T> ReactiveFind<T> query(Class<T> domainType) {
		return new ReactiveFindOperationSupport(this).query(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveUpdateOperation#update(java.lang.Class)
	 */
	@Override
	public <T> ReactiveUpdate<T> update(Class<T> domainType) {
		return new ReactiveUpdateOperationSupport(this).update(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveRemoveOperation#remove(java.lang.Class)
	 */
	@Override
	public <T> ReactiveRemove<T> remove(Class<T> domainType) {
		return new ReactiveRemoveOperationSupport(this).remove(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveInsertOperation#insert(java.lang.Class)
	 */
	@Override
	public <T> ReactiveInsert<T> insert(Class<T> domainType) {
		return new ReactiveInsertOperationSupport(this).insert(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveAggregationOperation#aggregateAndReturn(java.lang.Class)
	 */
	@Override
	public <T> ReactiveAggregation<T> aggregateAndReturn(Class<T> domainType) {
		return new ReactiveAggregationOperationSupport(this).aggregateAndReturn(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMapReduceOperation#mapReduce(java.lang.Class)
	 */
	@Override
	public <T> ReactiveMapReduce<T> mapReduce(Class<T> domainType) {
		return new ReactiveMapReduceOperationSupport(this).mapReduce(domainType);
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
	protected <T> Flux<T> doFindAndDelete(String collectionName, Query query, Class<T> entityClass) {

		Flux<T> flux = find(query, entityClass, collectionName);

		return Flux.from(flux).collectList()
				.flatMapMany(list -> Flux.from(remove(operations.getByIdInQuery(list), entityClass, collectionName))
						.flatMap(deleteResult -> Flux.fromIterable(list)));
	}

	/**
	 * Create the specified collection using the provided options
	 *
	 * @param collectionName
	 * @param collectionOptions
	 * @return the collection that was created
	 */
	protected Mono<MongoCollection<Document>> doCreateCollection(final String collectionName,
			final CreateCollectionOptions collectionOptions) {

		return createMono(db -> db.createCollection(collectionName, collectionOptions)).map(success -> {

			// TODO: Emit a collection created event
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Created collection [{}]", collectionName);
			}
			return getCollection(collectionName);
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
	 * @param collation can be {@literal null}.
	 * @return the {@link List} of converted objects.
	 */
	protected <T> Mono<T> doFindOne(String collectionName, Document query, @Nullable Document fields,
			Class<T> entityClass, @Nullable Collation collation) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);
		Document mappedFields = fields == null ? null : queryMapper.getMappedObject(fields, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("findOne using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(query), mappedFields, entityClass, collectionName));
		}

		return executeFindOneInternal(new FindOneCallback(mappedQuery, mappedFields, collation),
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
	protected <T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass) {
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
	protected <T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass,
			FindPublisherPreparer preparer) {
		return doFind(collectionName, query, fields, entityClass, preparer,
				new ReadDocumentCallback<>(mongoConverter, entityClass, collectionName));
	}

	protected <S, T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<S> entityClass,
			@Nullable FindPublisherPreparer preparer, DocumentCallback<T> objectCallback) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		Document mappedFields = queryMapper.getMappedFields(fields, entity);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("find using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(mappedQuery), mappedFields, entityClass, collectionName));
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
	<S, T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<S> sourceClass,
			Class<T> targetClass, FindPublisherPreparer preparer) {

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

	private Document getMappedFieldsObject(Document fields, MongoPersistentEntity<?> entity, Class<?> targetType) {
		return queryMapper.getMappedFields(addFieldsForProjection(fields, entity.getType(), targetType), entity);
	}

	/**
	 * For cases where {@code fields} is {@literal null} or {@literal empty} add fields required for creating the
	 * projection (target) type if the {@code targetType} is a {@literal closed interface projection}.
	 *
	 * @param fields must not be {@literal null}.
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

	protected CreateCollectionOptions convertToCreateCollectionOptions(@Nullable CollectionOptions collectionOptions) {
		return convertToCreateCollectionOptions(collectionOptions, Object.class);
	}

	protected CreateCollectionOptions convertToCreateCollectionOptions(@Nullable CollectionOptions collectionOptions,
			Class<?> entityType) {

		CreateCollectionOptions result = new CreateCollectionOptions();

		if (collectionOptions == null) {
			return result;
		}

		collectionOptions.getCapped().ifPresent(result::capped);
		collectionOptions.getSize().ifPresent(result::sizeInBytes);
		collectionOptions.getMaxDocuments().ifPresent(result::maxDocuments);
		collectionOptions.getCollation().map(Collation::toMongoCollation).ifPresent(result::collation);

		collectionOptions.getValidationOptions().ifPresent(it -> {

			ValidationOptions validationOptions = new ValidationOptions();

			it.getValidationAction().ifPresent(validationOptions::validationAction);
			it.getValidationLevel().ifPresent(validationOptions::validationLevel);

			it.getValidator().ifPresent(val -> validationOptions.validator(getMappedValidator(val, entityType)));

			result.validationOptions(validationOptions);
		});

		return result;
	}

	private Document getMappedValidator(Validator validator, Class<?> domainType) {

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
	 * @param collation collation
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects.
	 */
	protected <T> Mono<T> doFindAndRemove(String collectionName, Document query, Document fields, Document sort,
			@Nullable Collation collation, Class<T> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("findAndRemove using query: %s fields: %s sort: %s for class: %s in collection: %s",
					serializeToJsonSafely(query), fields, sort, entityClass, collectionName));
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		return executeFindOneInternal(
				new FindAndRemoveCallback(queryMapper.getMappedObject(query, entity), fields, sort, collation),
				new ReadDocumentCallback<>(this.mongoConverter, entityClass, collectionName), collectionName);
	}

	protected <T> Mono<T> doFindAndModify(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass, Update update, FindAndModifyOptions options) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		return Mono.defer(() -> {

			increaseVersionForUpdateIfNecessary(entity, update);

			Document mappedQuery = queryMapper.getMappedObject(query, entity);
			Document mappedUpdate = updateMapper.getMappedObject(update.getUpdateObject(), entity);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format(
						"findAndModify using query: %s fields: %s sort: %s for class: %s and update: %s " + "in collection: %s",
						serializeToJsonSafely(mappedQuery), fields, sort, entityClass, serializeToJsonSafely(mappedUpdate),
						collectionName));
			}

			return executeFindOneInternal(new FindAndModifyCallback(mappedQuery, fields, sort, mappedUpdate, options),
					new ReadDocumentCallback<>(this.mongoConverter, entityClass, collectionName), collectionName);
		});
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
	 * @return {@link Mono#empty()} if object does not exist, {@link FindAndReplaceOptions#isReturnNew() return new} is
	 *         {@literal false} and {@link FindAndReplaceOptions#isUpsert() upsert} is {@literal false}.
	 * @since 2.1
	 */
	protected <T> Mono<T> doFindAndReplace(String collectionName, Document mappedQuery, Document mappedFields,
			Document mappedSort, com.mongodb.client.model.Collation collation, Class<?> entityType, Document replacement,
			FindAndReplaceOptions options, Class<T> resultType) {

		return Mono.defer(() -> {

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"findAndReplace using query: {} fields: {} sort: {} for class: {} and replacement: {} "
								+ "in collection: {}",
						serializeToJsonSafely(mappedQuery), mappedFields, mappedSort, entityType,
						serializeToJsonSafely(replacement), collectionName);
			}

			maybeEmitEvent(new BeforeSaveEvent<>(replacement, replacement, collectionName));

			return executeFindOneInternal(
					new FindAndReplaceCallback(mappedQuery, mappedFields, mappedSort, replacement, collation, options),
					new ProjectingReadCallback<>(this.mongoConverter, entityType, resultType, collectionName), collectionName);
		});
	}

	protected <E extends MongoMappingEvent<T>, T> E maybeEmitEvent(E event) {

		if (null != eventPublisher) {
			eventPublisher.publishEvent(event);
		}

		return event;
	}

	private MongoCollection<Document> getAndPrepareCollection(MongoDatabase db, String collectionName) {

		try {
			MongoCollection<Document> collection = db.getCollection(collectionName, Document.class);
			return prepareCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	protected void ensureNotIterable(Object o) {

		boolean isIterable = o.getClass().isArray()
				|| ITERABLE_CLASSES.stream().anyMatch(iterableClass -> iterableClass.isAssignableFrom(o.getClass())
						|| o.getClass().getName().equals(iterableClass.getName()));

		if (isIterable) {
			throw new IllegalArgumentException("Cannot use a collection here.");
		}
	}

	/**
	 * Prepare the collection before any processing is done using it. This allows a convenient way to apply settings like
	 * slaveOk() etc. Can be overridden in sub-classes.
	 *
	 * @param collection
	 */
	protected MongoCollection<Document> prepareCollection(MongoCollection<Document> collection) {
		return this.readPreference != null ? collection.withReadPreference(readPreference) : collection;
	}

	/**
	 * @param database
	 * @return
	 * @since 2.1
	 */
	protected MongoDatabase prepareDatabase(MongoDatabase database) {
		return database;
	}

	/**
	 * Prepare the WriteConcern before any processing is done using it. This allows a convenient way to apply custom
	 * settings in sub-classes. <br />
	 * The returned {@link WriteConcern} will be defaulted to {@link WriteConcern#ACKNOWLEDGED} when
	 * {@link WriteResultChecking} is set to {@link WriteResultChecking#EXCEPTION}.
	 *
	 * @param mongoAction any WriteConcern already configured or {@literal null}.
	 * @return The prepared WriteConcern or {@literal null}.
	 * @see #setWriteConcern(WriteConcern)
	 * @see #setWriteConcernResolver(WriteConcernResolver)
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

	/**
	 * Internal method using callbacks to do queries against the datastore that requires reading a single object from a
	 * collection of objects. It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ReactiveCollectionCallback} for a {@link Document}.</li>
	 * <li>Apply the given {@link DocumentCallback} to each of the {@link Document}s to obtain the result.</li>
	 * <ol>
	 *
	 * @param collectionCallback the callback to retrieve the {@link Document}
	 * @param objectCallback the {@link DocumentCallback} to transform {@link Document}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> Mono<T> executeFindOneInternal(ReactiveCollectionCallback<Document> collectionCallback,
			DocumentCallback<T> objectCallback, String collectionName) {

		return createMono(collectionName,
				collection -> Mono.from(collectionCallback.doInCollection(collection)).map(objectCallback::doWith));
	}

	/**
	 * Internal method using callback to do queries against the datastore that requires reading a collection of objects.
	 * It will take the following steps
	 * <ol>
	 * <li>Execute the given {@link ReactiveCollectionCallback} for a {@link FindPublisher}.</li>
	 * <li>Prepare that {@link FindPublisher} with the given {@link FindPublisherPreparer} (will be skipped if
	 * {@link FindPublisherPreparer} is {@literal null}</li>
	 * <li>Apply the given {@link DocumentCallback} in {@link Flux#map(Function)} of {@link FindPublisher}</li>
	 * <ol>
	 *
	 * @param collectionCallback the callback to retrieve the {@link FindPublisher} with, must not be {@literal null}.
	 * @param preparer the {@link FindPublisherPreparer} to potentially modify the {@link FindPublisher} before iterating
	 *          over it, may be {@literal null}.
	 * @param objectCallback the {@link DocumentCallback} to transform {@link Document}s into the actual domain type, must
	 *          not be {@literal null}.
	 * @param collectionName the collection to be queried, must not be {@literal null}.
	 * @return
	 */
	private <T> Flux<T> executeFindMultiInternal(ReactiveCollectionQueryCallback<Document> collectionCallback,
			@Nullable FindPublisherPreparer preparer, DocumentCallback<T> objectCallback, String collectionName) {

		return createFlux(collectionName, collection -> {

			FindPublisher<Document> findPublisher = collectionCallback.doInCollection(collection);

			if (preparer != null) {
				findPublisher = preparer.prepare(findPublisher);
			}
			return Flux.from(findPublisher).map(objectCallback::doWith);
		});
	}

	private <T> T execute(MongoDatabaseCallback<T> action) {

		Assert.notNull(action, "MongoDatabaseCallback must not be null!");

		try {
			MongoDatabase db = this.doGetDatabase();
			return action.doInDatabase(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/**
	 * Exception translation {@link Function} intended for {@link Flux#onErrorMap(Function)} usage.
	 *
	 * @return the exception translation {@link Function}
	 */
	private Function<Throwable, Throwable> translateException() {

		return throwable -> {

			if (throwable instanceof RuntimeException) {
				return potentiallyConvertRuntimeException((RuntimeException) throwable, exceptionTranslator);
			}

			return throwable;
		};
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

	@Nullable
	private MongoPersistentEntity<?> getPersistentEntity(@Nullable Class<?> type) {
		return type == null ? null : mappingContext.getPersistentEntity(type);
	}

	@Nullable
	private MongoPersistentProperty getIdPropertyFor(@Nullable Class<?> type) {

		if (type == null) {
			return null;
		}

		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(type);
		return persistentEntity != null ? persistentEntity.getIdProperty() : null;
	}

	private <T> String determineEntityCollectionName(@Nullable T obj) {

		if (null != obj) {
			return determineCollectionName(obj.getClass());
		}

		return null;
	}

	String determineCollectionName(@Nullable Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity collection can't be determined!");
		}

		return mappingContext.getRequiredPersistentEntity(entityClass).getCollection();
	}

	private static MappingMongoConverter getDefaultMongoConverter() {

		MongoCustomConversions conversions = new MongoCustomConversions(Collections.emptyList());

		MongoMappingContext context = new MongoMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(NO_OP_REF_RESOLVER, context);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		return converter;
	}

	private Document getMappedSortObject(Query query, Class<?> type) {

		if (query == null) {
			return null;
		}

		return queryMapper.getMappedSort(query.getSortObject(), mappingContext.getPersistentEntity(type));
	}

	// Callback implementations

	/**
	 * Simple {@link ReactiveCollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link DBCollection}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Risberg
	 */
	private static class FindOneCallback implements ReactiveCollectionCallback<Document> {

		private final Document query;
		private final Optional<Document> fields;
		private final Optional<Collation> collation;

		FindOneCallback(Document query, @Nullable Document fields, @Nullable Collation collation) {
			this.query = query;
			this.fields = Optional.ofNullable(fields);
			this.collation = Optional.ofNullable(collation);
		}

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			FindPublisher<Document> publisher = collection.find(query, Document.class);

			if (LOGGER.isDebugEnabled()) {

				LOGGER.debug("findOne using query: {} fields: {} in db.collection: {}", serializeToJsonSafely(query),
						serializeToJsonSafely(fields.orElseGet(Document::new)), collection.getNamespace().getFullName());
			}

			if (fields.isPresent()) {
				publisher = publisher.projection(fields.get());
			}

			publisher = collation.map(Collation::toMongoCollation).map(publisher::collation).orElse(publisher);

			return publisher.limit(1).first();
		}
	}

	/**
	 * Simple {@link ReactiveCollectionQueryCallback} that takes a query {@link Document} plus an optional fields
	 * specification {@link Document} and executes that against the {@link MongoCollection}.
	 *
	 * @author Mark Paluch
	 */
	private static class FindCallback implements ReactiveCollectionQueryCallback<Document> {

		private final @Nullable Document query;
		private final @Nullable Document fields;

		FindCallback(@Nullable Document query) {
			this(query, null);
		}

		FindCallback(Document query, Document fields) {
			this.query = query;
			this.fields = fields;
		}

		@Override
		public FindPublisher<Document> doInCollection(MongoCollection<Document> collection) {

			FindPublisher<Document> findPublisher;
			if (ObjectUtils.isEmpty(query)) {
				findPublisher = collection.find(Document.class);
			} else {
				findPublisher = collection.find(query, Document.class);
			}

			if (ObjectUtils.isEmpty(fields)) {
				return findPublisher;
			} else {
				return findPublisher.projection(fields);
			}
		}
	}

	/**
	 * Simple {@link ReactiveCollectionCallback} that takes a query {@link Document} plus an optional fields specification
	 * {@link Document} and executes that against the {@link MongoCollection}.
	 *
	 * @author Mark Paluch
	 */
	private static class FindAndRemoveCallback implements ReactiveCollectionCallback<Document> {

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

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			FindOneAndDeleteOptions findOneAndDeleteOptions = convertToFindOneAndDeleteOptions(fields, sort);
			collation.map(Collation::toMongoCollation).ifPresent(findOneAndDeleteOptions::collation);

			return collection.findOneAndDelete(query, findOneAndDeleteOptions);
		}
	}

	/**
	 * @author Mark Paluch
	 */
	private static class FindAndModifyCallback implements ReactiveCollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Document update;
		private final FindAndModifyOptions options;

		FindAndModifyCallback(Document query, Document fields, Document sort, Document update,
				FindAndModifyOptions options) {

			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.options = options;
		}

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			if (options.isRemove()) {
				FindOneAndDeleteOptions findOneAndDeleteOptions = convertToFindOneAndDeleteOptions(fields, sort);

				findOneAndDeleteOptions = options.getCollation().map(Collation::toMongoCollation)
						.map(findOneAndDeleteOptions::collation).orElse(findOneAndDeleteOptions);

				return collection.findOneAndDelete(query, findOneAndDeleteOptions);
			}

			FindOneAndUpdateOptions findOneAndUpdateOptions = convertToFindOneAndUpdateOptions(options, fields, sort);
			return collection.findOneAndUpdate(query, update, findOneAndUpdateOptions);
		}

		private FindOneAndUpdateOptions convertToFindOneAndUpdateOptions(FindAndModifyOptions options, Document fields,
				Document sort) {

			FindOneAndUpdateOptions result = new FindOneAndUpdateOptions();

			result = result.projection(fields).sort(sort).upsert(options.isUpsert());

			if (options.isReturnNew()) {
				result = result.returnDocument(ReturnDocument.AFTER);
			} else {
				result = result.returnDocument(ReturnDocument.BEFORE);
			}

			result = options.getCollation().map(Collation::toMongoCollation).map(result::collation).orElse(result);

			return result;
		}
	}

	/**
	 * {@link ReactiveCollectionCallback} specific for find and remove operation.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
	private static class FindAndReplaceCallback implements ReactiveCollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Document update;
		private final @Nullable com.mongodb.client.model.Collation collation;
		private final FindAndReplaceOptions options;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveCollectionCallback#doInCollection(com.mongodb.reactivestreams.client.MongoCollection)
		 */
		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			FindOneAndReplaceOptions findOneAndReplaceOptions = convertToFindOneAndReplaceOptions(options, fields, sort);
			return collection.findOneAndReplace(query, update, findOneAndReplaceOptions);
		}

		private FindOneAndReplaceOptions convertToFindOneAndReplaceOptions(FindAndReplaceOptions options, Document fields,
				Document sort) {

			FindOneAndReplaceOptions result = new FindOneAndReplaceOptions().collation(collation);

			result = result.projection(fields).sort(sort).upsert(options.isUpsert());

			if (options.isReturnNew()) {
				result = result.returnDocument(ReturnDocument.AFTER);
			} else {
				result = result.returnDocument(ReturnDocument.BEFORE);
			}

			return result;
		}
	}

	private static FindOneAndDeleteOptions convertToFindOneAndDeleteOptions(Document fields, Document sort) {

		FindOneAndDeleteOptions result = new FindOneAndDeleteOptions();
		result = result.projection(fields).sort(sort);

		return result;
	}

	/**
	 * Simple internal callback to allow operations on a {@link Document}.
	 *
	 * @author Mark Paluch
	 */

	interface DocumentCallback<T> {

		T doWith(Document object);
	}

	/**
	 * Simple internal callback to allow operations on a {@link MongoDatabase}.
	 *
	 * @author Mark Paluch
	 */

	interface MongoDatabaseCallback<T> {

		T doInDatabase(MongoDatabase db);
	}

	/**
	 * Simple internal callback to allow operations on a {@link MongoDatabase}.
	 *
	 * @author Mark Paluch
	 */
	interface ReactiveCollectionQueryCallback<T> extends ReactiveCollectionCallback<T> {

		FindPublisher<T> doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException;
	}

	/**
	 * Simple {@link DocumentCallback} that will transform {@link Document} into the given target type using the given
	 * {@link EntityReader}.
	 *
	 * @author Mark Paluch
	 */
	class ReadDocumentCallback<T> implements DocumentCallback<T> {

		private final EntityReader<? super T, Bson> reader;
		private final Class<T> type;
		private final String collectionName;

		ReadDocumentCallback(EntityReader<? super T, Bson> reader, Class<T> type, String collectionName) {

			Assert.notNull(reader, "EntityReader must not be null!");
			Assert.notNull(type, "Entity type must not be null!");

			this.reader = reader;
			this.type = type;
			this.collectionName = collectionName;
		}

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
	 * {@link MongoTemplate.DocumentCallback} transforming {@link Document} into the given {@code targetType} or
	 * decorating the {@code sourceType} with a {@literal projection} in case the {@code targetType} is an
	 * {@litera interface}.
	 *
	 * @param <S>
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	private class ProjectingReadCallback<S, T> implements DocumentCallback<T> {

		private final @NonNull EntityReader<Object, Bson> reader;
		private final @NonNull Class<S> entityType;
		private final @NonNull Class<T> targetType;
		private final @NonNull String collectionName;

		@Nullable
		@SuppressWarnings("unchecked")
		public T doWith(@Nullable Document object) {

			if (object == null) {
				return null;
			}

			Class<?> typeToRead = targetType.isInterface() || targetType.isAssignableFrom(entityType) //
					? entityType //
					: targetType;

			if (null != object) {
				maybeEmitEvent(new AfterLoadEvent<>(object, typeToRead, collectionName));
			}

			Object source = reader.read(typeToRead, object);
			Object result = targetType.isInterface() ? projectionFactory.createProjection(targetType, source) : source;

			if (null != source) {
				maybeEmitEvent(new AfterConvertEvent<>(object, result, collectionName));
			}
			return (T) result;
		}
	}

	/**
	 * {@link DocumentCallback} that assumes a {@link GeoResult} to be created, delegates actual content unmarshalling to
	 * a delegate and creates a {@link GeoResult} from the result.
	 *
	 * @author Mark Paluch
	 */
	static class GeoNearResultDbObjectCallback<T> implements DocumentCallback<GeoResult<T>> {

		private final DocumentCallback<T> delegate;
		private final Metric metric;

		/**
		 * Creates a new {@link GeoNearResultDbObjectCallback} using the given {@link DocumentCallback} delegate for
		 * {@link GeoResult} content unmarshalling.
		 *
		 * @param delegate must not be {@literal null}.
		 */
		GeoNearResultDbObjectCallback(DocumentCallback<T> delegate, Metric metric) {

			Assert.notNull(delegate, "DocumentCallback must not be null!");

			this.delegate = delegate;
			this.metric = metric;
		}

		public GeoResult<T> doWith(Document object) {

			double distance = (Double) object.get("dis");
			Document content = (Document) object.get("obj");

			T doWith = delegate.doWith(content);

			return new GeoResult<>(doWith, new Distance(distance, metric));
		}
	}

	/**
	 * @author Mark Paluch
	 */
	class QueryFindPublisherPreparer implements FindPublisherPreparer {

		private final @Nullable Query query;
		private final @Nullable Class<?> type;

		QueryFindPublisherPreparer(@Nullable Query query, @Nullable Class<?> type) {

			this.query = query;
			this.type = type;
		}

		@SuppressWarnings("deprecation")
		public <T> FindPublisher<T> prepare(FindPublisher<T> findPublisher) {

			if (query == null) {
				return findPublisher;
			}

			FindPublisher<T> findPublisherToUse;

			findPublisherToUse = query.getCollation().map(Collation::toMongoCollation).map(findPublisher::collation)
					.orElse(findPublisher);

			Meta meta = query.getMeta();
			if (query.getSkip() <= 0 && query.getLimit() <= 0 && ObjectUtils.isEmpty(query.getSortObject())
					&& !StringUtils.hasText(query.getHint()) && !meta.hasValues()) {
				return findPublisherToUse;
			}

			try {

				if (query.getSkip() > 0) {
					findPublisherToUse = findPublisherToUse.skip((int) query.getSkip());
				}

				if (query.getLimit() > 0) {
					findPublisherToUse = findPublisherToUse.limit(query.getLimit());
				}

				if (!ObjectUtils.isEmpty(query.getSortObject())) {
					Document sort = type != null ? getMappedSortObject(query, type) : query.getSortObject();
					findPublisherToUse = findPublisherToUse.sort(sort);
				}

				if (StringUtils.hasText(query.getHint())) {
					findPublisherToUse = findPublisherToUse.hint(Document.parse(query.getHint()));
				}

				if (meta.hasValues()) {

					if (StringUtils.hasText(meta.getComment())) {
						findPublisherToUse = findPublisherToUse.comment(meta.getComment());
					}

					if (meta.getSnapshot()) {
						findPublisherToUse = findPublisherToUse.snapshot(meta.getSnapshot());
					}

					if (meta.getMaxScan() != null) {
						findPublisherToUse = findPublisherToUse.maxScan(meta.getMaxScan());
					}

					if (meta.getMaxTimeMsec() != null) {
						findPublisherToUse = findPublisherToUse.maxTime(meta.getMaxTimeMsec(), TimeUnit.MILLISECONDS);
					}

					if (meta.getCursorBatchSize() != null) {
						findPublisherToUse = findPublisherToUse.batchSize(meta.getCursorBatchSize());
					}
				}

			} catch (RuntimeException e) {
				throw potentiallyConvertRuntimeException(e, exceptionTranslator);
			}

			return findPublisherToUse;
		}
	}

	class TailingQueryFindPublisherPreparer extends QueryFindPublisherPreparer {

		TailingQueryFindPublisherPreparer(Query query, Class<?> type) {
			super(query, type);
		}

		@Override
		public <T> FindPublisher<T> prepare(FindPublisher<T> findPublisher) {
			return super.prepare(findPublisher.cursorType(CursorType.TailableAwait));
		}
	}

	private static List<? extends Document> toDocuments(final Collection<? extends Document> documents) {
		return new ArrayList<>(documents);
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
	static class ReactiveSessionBoundMongoTemplate extends ReactiveMongoTemplate {

		private final ReactiveMongoTemplate delegate;
		private final ClientSession session;

		/**
		 * @param session must not be {@literal null}.
		 * @param that must not be {@literal null}.
		 */
		ReactiveSessionBoundMongoTemplate(ClientSession session, ReactiveMongoTemplate that) {

			super(that.mongoDatabaseFactory.withSession(session), that);

			this.delegate = that;
			this.session = session;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveMongoTemplate#getCollection(java.lang.String)
		 */
		@Override
		public MongoCollection<Document> getCollection(String collectionName) {

			// native MongoDB objects that offer methods with ClientSession must not be proxied.
			return delegate.getCollection(collectionName);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveMongoTemplate#getMongoDatabase()
		 */
		@Override
		public MongoDatabase getMongoDatabase() {

			// native MongoDB objects that offer methods with ClientSession must not be proxied.
			return delegate.getMongoDatabase();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveMongoTemplate#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Mono<Long> count(Query query, @Nullable Class<?> entityClass, String collectionName) {

			if (!session.hasActiveTransaction()) {
				return super.count(query, entityClass, collectionName);
			}

			return createMono(collectionName, collection -> {

				final Document Document = query == null ? null
						: delegate.queryMapper.getMappedObject(query.getQueryObject(),
								entityClass == null ? null : delegate.mappingContext.getPersistentEntity(entityClass));

				CountOptions options = new CountOptions();
				if (query != null) {
					query.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);
				}

				return collection.countDocuments(Document, options);
			});
		}
	}

	@RequiredArgsConstructor
	class IndexCreatorEventListener implements ApplicationListener<MappingContextEvent<?, ?>> {

		final Consumer<Throwable> subscriptionExceptionHandler;

		@Override
		public void onApplicationEvent(MappingContextEvent<?, ?> event) {

			if (!event.wasEmittedBy(mappingContext)) {
				return;
			}

			PersistentEntity<?, ?> entity = event.getPersistentEntity();

			// Double check type as Spring infrastructure does not consider nested generics
			if (entity instanceof MongoPersistentEntity) {
				onCheckForIndexes((MongoPersistentEntity<?>) entity, subscriptionExceptionHandler);
			}
		}
	}
}
