/*
 * Copyright 2016-2019 the original author or authors.
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

import com.mongodb.client.result.InsertOneResult;
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
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseUtils;
import org.springframework.data.mongodb.SessionSynchronization;
import org.springframework.data.mongodb.core.EntityOperations.AdaptibleEntity;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.PrefixingDelegatingAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.RelaxedTypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.JsonSchemaMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.MongoJsonSchemaMapper;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
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
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.Meta.CursorOption;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.data.mongodb.core.validation.Validator;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
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
import com.mongodb.CursorType;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

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
	private final PropertyOperations propertyOperations;

	private @Nullable WriteConcern writeConcern;
	private WriteConcernResolver writeConcernResolver = DefaultWriteConcernResolver.INSTANCE;
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;
	private @Nullable ReadPreference readPreference;
	private @Nullable ApplicationEventPublisher eventPublisher;
	private @Nullable ReactiveEntityCallbacks entityCallbacks;
	private @Nullable ReactiveMongoPersistentEntityIndexCreator indexCreator;

	private SessionSynchronization sessionSynchronization = SessionSynchronization.ON_ACTUAL_TRANSACTION;

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
		this.propertyOperations = new PropertyOperations(this.mappingContext);

		// We create indexes based on mapping events
		if (this.mappingContext instanceof MongoMappingContext) {

			MongoMappingContext mongoMappingContext = (MongoMappingContext) this.mappingContext;

			if (mongoMappingContext.isAutoIndexCreation()) {
				this.indexCreator = new ReactiveMongoPersistentEntityIndexCreator(mongoMappingContext, this::indexOps);
				this.eventPublisher = new MongoMappingEventPublisher(this.indexCreatorListener);

				mongoMappingContext.setApplicationEventPublisher(this.eventPublisher);
				this.mappingContext.getPersistentEntities()
						.forEach(entity -> onCheckForIndexes(entity, subscriptionExceptionHandler));
			}
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
		this.propertyOperations = that.propertyOperations;
		this.sessionSynchronization = that.sessionSynchronization;
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

		if (entityCallbacks == null) {
			setEntityCallbacks(ReactiveEntityCallbacks.create(applicationContext));
		}

		if (mappingContext instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
		}

		projectionFactory.setBeanFactory(applicationContext);
		projectionFactory.setBeanClassLoader(applicationContext.getClassLoader());
	}

	/**
	 * Set the {@link ReactiveEntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.EntityCallback callbacks} like the
	 * {@link ReactiveBeforeSaveCallback}.
	 * <p />
	 * Overrides potentially existing {@link ReactiveEntityCallbacks}.
	 *
	 * @param entityCallbacks must not be {@literal null}.
	 * @throws IllegalArgumentException if the given instance is {@literal null}.
	 * @since 2.2
	 */
	public void setEntityCallbacks(ReactiveEntityCallbacks entityCallbacks) {

		Assert.notNull(entityCallbacks, "EntityCallbacks must not be null!");
		this.entityCallbacks = entityCallbacks;
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
		return new DefaultReactiveIndexOperations(this, getCollectionName(entityClass), this.queryMapper, entityClass);
	}

	public String getCollectionName(Class<?> entityClass) {
		return operations.determineCollectionName(entityClass);
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
	public Mono<Document> executeCommand(Document command) {
		return executeCommand(command, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#executeCommand(org.bson.Document, com.mongodb.ReadPreference)
	 */
	public Mono<Document> executeCommand(Document command, @Nullable ReadPreference readPreference) {

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
		return createFlux(getCollectionName(entityClass), action);
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

	/**
	 * Define if {@link ReactiveMongoTemplate} should participate in transactions. Default is set to
	 * {@link SessionSynchronization#ON_ACTUAL_TRANSACTION}.<br />
	 * <strong>NOTE:</strong> MongoDB transactions require at least MongoDB 4.0.
	 *
	 * @since 2.2
	 */
	public void setSessionSynchronization(SessionSynchronization sessionSynchronization) {
		this.sessionSynchronization = sessionSynchronization;
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
							(sess, err) -> sess.abortTransaction(), //
							ClientSession::commitTransaction) //
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

		return Mono.defer(this::doGetDatabase).flatMapMany(database -> callback.doInDB(prepareDatabase(database)))
				.onErrorMap(translateException());
	}

	/**
	 * Create a reusable Mono for a {@link ReactiveDatabaseCallback}. It's up to the developer to choose to obtain a new
	 * {@link Flux} or to reuse the {@link Flux}.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Mono} wrapping the {@link ReactiveDatabaseCallback}.
	 */
	public <T> Mono<T> createMono(ReactiveDatabaseCallback<T> callback) {

		Assert.notNull(callback, "ReactiveDatabaseCallback must not be null!");

		return Mono.defer(this::doGetDatabase).flatMap(database -> Mono.from(callback.doInDB(prepareDatabase(database))))
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

		Mono<MongoCollection<Document>> collectionPublisher = doGetDatabase()
				.map(database -> getAndPrepareCollection(database, collectionName));

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

		Mono<MongoCollection<Document>> collectionPublisher = doGetDatabase()
				.map(database -> getAndPrepareCollection(database, collectionName));

		return collectionPublisher.flatMap(collection -> Mono.from(callback.doInCollection(collection)))
				.onErrorMap(translateException());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.Class)
	 */
	public <T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass) {
		return createCollection(entityClass, CollectionOptions.empty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.Class, org.springframework.data.mongodb.core.CollectionOptions)
	 */
	public <T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass,
			@Nullable CollectionOptions collectionOptions) {

		Assert.notNull(entityClass, "EntityClass must not be null!");

		CollectionOptions options = collectionOptions != null ? collectionOptions : CollectionOptions.empty();
		options = Optionals
				.firstNonEmpty(() -> Optional.ofNullable(collectionOptions).flatMap(CollectionOptions::getCollation),
						() -> operations.forType(entityClass).getCollation()) //
				.map(options::collation).orElse(options);

		return doCreateCollection(getCollectionName(entityClass), convertToCreateCollectionOptions(options, entityClass));
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
	public MongoCollection<Document> getCollection(String collectionName) {

		Assert.notNull(collectionName, "Collection name must not be null!");

		try {
			return this.mongoDatabaseFactory.getMongoDatabase().getCollection(collectionName);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#collectionExists(java.lang.Class)
	 */
	public <T> Mono<Boolean> collectionExists(Class<T> entityClass) {
		return collectionExists(getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#collectionExists(java.lang.String)
	 */
	public Mono<Boolean> collectionExists(String collectionName) {
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
		return dropCollection(getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#dropCollection(java.lang.String)
	 */
	public Mono<Void> dropCollection(String collectionName) {

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
		return mongoDatabaseFactory.getMongoDatabase();
	}

	protected Mono<MongoDatabase> doGetDatabase() {
		return ReactiveMongoDatabaseUtils.getDatabase(mongoDatabaseFactory, sessionSynchronization);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findOne(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Mono<T> findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findOne(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findOne(Query query, Class<T> entityClass, String collectionName) {

		if (ObjectUtils.isEmpty(query.getSortObject())) {
			return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
					new QueryFindPublisherPreparer(query, entityClass));
		}

		query.limit(1);
		return find(query, entityClass, collectionName).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#exists(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public Mono<Boolean> exists(Query query, Class<?> entityClass) {
		return exists(query, entityClass, getCollectionName(entityClass));
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
	public Mono<Boolean> exists(Query query, @Nullable Class<?> entityClass, String collectionName) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to exist can't be null");
		}

		return createFlux(collectionName, collection -> {

			Document filter = queryMapper.getMappedObject(query.getQueryObject(), getPersistentEntity(entityClass));
			FindPublisher<Document> findPublisher = collection.find(filter, Document.class)
					.projection(new Document("_id", 1));

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("exists: {} in collection: {}", serializeToJsonSafely(filter), collectionName);
			}

			findPublisher = operations.forType(entityClass).getCollation(query).map(Collation::toMongoCollation)
					.map(findPublisher::collation).orElse(findPublisher);

			return findPublisher.limit(1);
		}).hasElements();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#find(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Flux<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, getCollectionName(entityClass));
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
		return findById(id, entityClass, getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findById(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findById(Object id, Class<T> entityClass, String collectionName) {

		String idKey = operations.getIdPropertyName(entityClass);

		return doFindOne(collectionName, new Document(idKey, id), null, entityClass, (Collation) null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findDistinct(org.springframework.data.mongodb.core.query.Query, java.lang.String, java.lang.Class, java.lang.Class)
	 */
	public <T> Flux<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass) {
		return findDistinct(query, field, getCollectionName(entityClass), entityClass, resultClass);
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

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Executing findDistinct using query {} for field: {} in collection: {}",
						serializeToJsonSafely(mappedQuery), field, collectionName);
			}

			FindPublisherPreparer preparer = new QueryFindPublisherPreparer(query, entityClass);
			if (preparer.hasReadPreference()) {
				collection = collection.withReadPreference(preparer.getReadPreference());
			}

			DistinctPublisher<T> publisher = collection.distinct(mappedFieldName, mappedQuery, mongoDriverCompatibleType);
			return operations.forType(entityClass).getCollation(query) //
					.map(Collation::toMongoCollation) //
					.map(publisher::collation) //
					.orElse(publisher);
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
		return aggregate(aggregation, getCollectionName(aggregation.getInputType()), outputType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#aggregate(org.springframework.data.mongodb.core.aggregation.Aggregation, java.lang.Class, java.lang.Class)
	 */
	@Override
	public <O> Flux<O> aggregate(Aggregation aggregation, Class<?> inputType, Class<O> outputType) {

		return aggregate(aggregation, getCollectionName(inputType), outputType,
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
		return execute(collectionName, collection -> aggregateAndMap(collection, pipeline, options, readCallback,
				aggregation instanceof TypedAggregation ? ((TypedAggregation) aggregation).getInputType() : null));
	}

	private <O> Flux<O> aggregateAndMap(MongoCollection<Document> collection, List<Document> pipeline,
			AggregationOptions options, ReadDocumentCallback<O> readCallback, @Nullable Class<?> inputType) {

		AggregatePublisher<Document> cursor = collection.aggregate(pipeline, Document.class)
				.allowDiskUse(options.isAllowDiskUse());

		if (options.getCursorBatchSize() != null) {
			cursor = cursor.batchSize(options.getCursorBatchSize());
		}

		options.getComment().ifPresent(cursor::comment);

		Optionals.firstNonEmpty(options::getCollation, () -> operations.forType(inputType).getCollation()) //
				.map(Collation::toMongoCollation) //
				.ifPresent(cursor::collation);

		if (options.hasExecutionTimeLimit()) {
			cursor = cursor.maxTime(options.getMaxTime().toMillis(), TimeUnit.MILLISECONDS);
		}

		return Flux.from(cursor).map(readCallback::doWith);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#geoNear(org.springframework.data.mongodb.core.query.NearQuery, java.lang.Class)
	 */
	@Override
	public <T> Flux<GeoResult<T>> geoNear(NearQuery near, Class<T> entityClass) {
		return geoNear(near, entityClass, getCollectionName(entityClass));
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

		String collection = StringUtils.hasText(collectionName) ? collectionName : getCollectionName(entityClass);
		String distanceField = operations.nearQueryDistanceFieldName(entityClass);

		GeoNearResultDocumentCallback<T> callback = new GeoNearResultDocumentCallback<>(distanceField,
				new ProjectingReadCallback<>(mongoConverter, entityClass, returnType, collection), near.getMetric());

		Aggregation $geoNear = TypedAggregation.newAggregation(entityClass, Aggregation.geoNear(near, distanceField))
				.withOptions(AggregationOptions.builder().collation(near.getCollation()).build());

		return aggregate($geoNear, collection, Document.class) //
				.map(callback::doWith);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.Class)
	 */
	public <T> Mono<T> findAndModify(Query query, UpdateDefinition update, Class<T> entityClass) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndModify(Query query, UpdateDefinition update, Class<T> entityClass, String collectionName) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, collectionName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, org.springframework.data.mongodb.core.FindAndModifyOptions, java.lang.Class)
	 */
	public <T> Mono<T> findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options,
			Class<T> entityClass) {
		return findAndModify(query, update, options, entityClass, getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, org.springframework.data.mongodb.core.FindAndModifyOptions, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options,
			Class<T> entityClass, String collectionName) {

		Assert.notNull(options, "Options must not be null! ");
		Assert.notNull(entityClass, "Entity class must not be null!");

		FindAndModifyOptions optionsToUse = FindAndModifyOptions.of(options);

		Optionals.ifAllPresent(query.getCollation(), optionsToUse.getCollation(), (l, r) -> {
			throw new IllegalArgumentException(
					"Both Query and FindAndModifyOptions define a collation. Please provide the collation only via one of the two.");
		});

		if (!optionsToUse.getCollation().isPresent()) {
			operations.forType(entityClass).getCollation(query).ifPresent(optionsToUse::collation);
		}

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

		return Mono.just(PersistableEntityModel.of(replacement, collectionName)) //
				.doOnNext(it -> maybeEmitEvent(new BeforeConvertEvent<>(it.getSource(), it.getCollection()))) //
				.flatMap(it -> maybeCallBeforeConvert(it.getSource(), it.getCollection()).map(it::mutate))
				.map(it -> it
						.addTargetDocument(operations.forEntity(it.getSource()).toMappedDocument(mongoConverter).getDocument())) //
				.doOnNext(it -> maybeEmitEvent(new BeforeSaveEvent(it.getSource(), it.getTarget(), it.getCollection()))) //
				.flatMap(it -> {

					PersistableEntityModel<S> flowObject = (PersistableEntityModel<S>) it;
					return maybeCallBeforeSave(flowObject.getSource(), flowObject.getTarget(), flowObject.getCollection())
							.map(potentiallyModified -> PersistableEntityModel.of(potentiallyModified, flowObject.getTarget(),
									flowObject.getCollection()));
				}).flatMap(it -> {

					PersistableEntityModel<S> flowObject = (PersistableEntityModel<S>) it;
					return doFindAndReplace(flowObject.getCollection(), mappedQuery, mappedFields, mappedSort,
							operations.forType(entityType).getCollation(query).map(Collation::toMongoCollation).orElse(null),
							entityType, flowObject.getTarget(), options, resultType);
				});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass, String collectionName) {

		operations.forType(entityClass).getCollation(query);
		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), operations.forType(entityClass).getCollation(query).orElse(null),
				entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public Mono<Long> count(Query query, Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null!");

		return count(query, entityClass, getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	public Mono<Long> count(Query query, String collectionName) {
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

			Document filter = queryMapper.getMappedObject(query.getQueryObject(),
					entityClass == null ? null : mappingContext.getPersistentEntity(entityClass));

			CountOptions options = new CountOptions();
			query.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);

			if (query.getLimit() > 0) {
				options.limit(query.getLimit());
			}
			if (query.getSkip() > 0) {
				options.skip((int) query.getSkip());
			}
			if (StringUtils.hasText(query.getHint())) {
				options.hint(Document.parse(query.getHint()));
			}

			operations.forType(entityClass).getCollation(query).map(Collation::toMongoCollation) //
					.ifPresent(options::collation);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Executing count: {} in collection: {}", serializeToJsonSafely(filter), collectionName);
			}

			return doCount(collectionName, filter, options);
		});
	}

	/**
	 * Run the actual count operation against the collection with given name.
	 *
	 * @param collectionName the name of the collection to count matching documents in.
	 * @param filter the filter to apply. Must not be {@literal null}.
	 * @param options options to apply. Like collation and the such.
	 * @return
	 */
	protected Mono<Long> doCount(String collectionName, Document filter, CountOptions options) {

		return createMono(collectionName,
				collection -> collection.countDocuments(CountQuery.of(filter).toQueryDocument(), options));
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
		return insertAll(batchToSave, getCollectionName(entityClass));
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
		return insert(objectToSave, getCollectionName(ClassUtils.getUserClass(objectToSave)));
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

		return Mono.just(PersistableEntityModel.of(objectToSave, collectionName)) //
				.doOnNext(it -> maybeEmitEvent(new BeforeConvertEvent<>(it.getSource(), it.getCollection()))) //
				.flatMap(it -> maybeCallBeforeConvert(it.getSource(), it.getCollection()).map(it::mutate)) //
				.map(it -> {

					AdaptibleEntity<T> entity = operations.forEntity(it.getSource(), mongoConverter.getConversionService());
					entity.assertUpdateableIdIfNotSet();

					return PersistableEntityModel.of(entity.initializeVersionProperty(),
							entity.toMappedDocument(writer).getDocument(), it.getCollection());
				}).doOnNext(it -> maybeEmitEvent(new BeforeSaveEvent<>(it.getSource(), it.getTarget(), it.getCollection()))) //
				.flatMap(it -> {

					return maybeCallBeforeSave(it.getSource(), it.getTarget(), it.getCollection()).map(it::mutate);

				}).flatMap(it -> {

					return insertDocument(it.getCollection(), it.getTarget(), it.getSource().getClass()).map(id -> {

						T saved = operations.forEntity(it.getSource(), mongoConverter.getConversionService())
								.populateIdIfNecessary(id);
						maybeEmitEvent(new AfterSaveEvent<>(saved, it.getTarget(), collectionName));
						return saved;
					});
				});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.util.Collection, java.lang.Class)
	 */
	public <T> Flux<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass) {
		return doInsertBatch(getCollectionName(entityClass), batchToSave, this.mongoConverter);
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

		Map<String, List<T>> elementsByCollection = new HashMap<>();

		listToSave.forEach(element -> {

			String collection = getCollectionName(element.getClass());
			List<T> collectionElements = elementsByCollection.computeIfAbsent(collection, k -> new ArrayList<>());

			collectionElements.add(element);
		});

		return Flux.fromIterable(elementsByCollection.keySet())
				.flatMap(collectionName -> doInsertBatch(collectionName, elementsByCollection.get(collectionName), writer));
	}

	protected <T> Flux<T> doInsertBatch(String collectionName, Collection<? extends T> batchToSave,
			MongoWriter<Object> writer) {

		Assert.notNull(writer, "MongoWriter must not be null!");

		Mono<List<Tuple2<AdaptibleEntity<T>, Document>>> prepareDocuments = Flux.fromIterable(batchToSave)
				.flatMap(uninitialized -> {

					BeforeConvertEvent<T> event = new BeforeConvertEvent<>(uninitialized, collectionName);
					T toConvert = maybeEmitEvent(event).getSource();

					return maybeCallBeforeConvert(toConvert, collectionName).flatMap(it -> {

						AdaptibleEntity<T> entity = operations.forEntity(it, mongoConverter.getConversionService());
						entity.assertUpdateableIdIfNotSet();

						T initialized = entity.initializeVersionProperty();
						Document dbDoc = entity.toMappedDocument(writer).getDocument();

						maybeEmitEvent(new BeforeSaveEvent<>(initialized, dbDoc, collectionName));

						return maybeCallBeforeSave(initialized, dbDoc, collectionName).thenReturn(Tuples.of(entity, dbDoc));
					});
				}).collectList();

		Flux<Tuple2<AdaptibleEntity<T>, Document>> insertDocuments = prepareDocuments.flatMapMany(tuples -> {

			List<Document> documents = tuples.stream().map(Tuple2::getT2).collect(Collectors.toList());

			return insertDocumentList(collectionName, documents).thenMany(Flux.fromIterable(tuples));
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
		return save(objectToSave, getCollectionName(ClassUtils.getUserClass(objectToSave)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#save(java.lang.Object, java.lang.String)
	 */
	public <T> Mono<T> save(T objectToSave, String collectionName) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		AdaptibleEntity<T> source = operations.forEntity(objectToSave, mongoConverter.getConversionService());

		return source.isVersionedEntity() ? doSaveVersioned(source, collectionName)
				: doSave(collectionName, objectToSave, this.mongoConverter);
	}

	private <T> Mono<T> doSaveVersioned(AdaptibleEntity<T> source, String collectionName) {

		if (source.isNew()) {
			return doInsert(collectionName, source.getBean(), this.mongoConverter);
		}

		return createMono(collectionName, collection -> {

			// Create query for entity with the id and old version
			Query query = source.getQueryForVersion();

			// Bump version number
			T toSave = source.incrementVersion();

			source.assertUpdateableIdIfNotSet();

			BeforeConvertEvent<T> event = new BeforeConvertEvent<>(toSave, collectionName);
			T afterEvent = maybeEmitEvent(event).getSource();

			return maybeCallBeforeConvert(afterEvent, collectionName).flatMap(toConvert -> {

				MappedDocument mapped = operations.forEntity(toConvert).toMappedDocument(mongoConverter);
				Document document = mapped.getDocument();

				maybeEmitEvent(new BeforeSaveEvent<>(toConvert, document, collectionName));
				return maybeCallBeforeSave(toConvert, document, collectionName).flatMap(it -> {

					return doUpdate(collectionName, query, mapped.updateWithoutId(), it.getClass(), false, false).map(result -> {
						return maybeEmitEvent(new AfterSaveEvent<T>(it, document, collectionName)).getSource();
					});
				});
			});
		});
	}

	protected <T> Mono<T> doSave(String collectionName, T objectToSave, MongoWriter<Object> writer) {

		assertUpdateableIdIfNotSet(objectToSave);

		return createMono(collectionName, collection -> {

			T toSave = maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName)).getSource();

			return maybeCallBeforeConvert(toSave, collectionName).flatMap(toConvert -> {

				AdaptibleEntity<T> entity = operations.forEntity(toConvert, mongoConverter.getConversionService());
				Document dbDoc = entity.toMappedDocument(writer).getDocument();
				maybeEmitEvent(new BeforeSaveEvent<T>(toConvert, dbDoc, collectionName));

				return maybeCallBeforeSave(toConvert, dbDoc, collectionName).flatMap(it -> {

					return saveDocument(collectionName, dbDoc, it.getClass()).map(id -> {

						T saved = entity.populateIdIfNecessary(id);
						return maybeEmitEvent(new AfterSaveEvent<>(saved, dbDoc, collectionName)).getSource();
					});
				});
			});
		});
	}

	protected Mono<Object> insertDocument(String collectionName, Document dbDoc, Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting Document containing fields: " + dbDoc.keySet() + " in collection: " + collectionName);
		}

		Document document = new Document(dbDoc);

		Flux<InsertOneResult> execute = execute(collectionName, collection -> {

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT, collectionName, entityClass,
					dbDoc, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

			MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

			return collectionToUse.insertOne(document);
		});

		return Flux.from(execute).last().map(success -> MappedDocument.of(document).getId());
	}

	protected Flux<ObjectId> insertDocumentList(String collectionName, List<Document> dbDocList) {

		if (dbDocList.isEmpty()) {
			return Flux.empty();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting list of Documents containing " + dbDocList.size() + " items");
		}

		List<Document> documents = new ArrayList<>();

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

	protected Mono<Object> saveDocument(String collectionName, Document document, Class<?> entityClass) {

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
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.Class)
	 */
	public Mono<UpdateResult> upsert(Query query, UpdateDefinition update, Class<?> entityClass) {
		return doUpdate(getCollectionName(entityClass), query, update, entityClass, true, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.String)
	 */
	public Mono<UpdateResult> upsert(Query query, UpdateDefinition update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> upsert(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, true, false);
	}

	/*
	 * (non-Javadoc))
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.Class)
	 */
	public Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, Class<?> entityClass) {
		return doUpdate(getCollectionName(entityClass), query, update, entityClass, false, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.String)
	 */
	public Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, Class<?> entityClass,
			String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.Class)
	 */
	public Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, Class<?> entityClass) {
		return doUpdate(getCollectionName(entityClass), query, update, entityClass, false, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.String)
	 */
	public Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.UpdateDefinition, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, Class<?> entityClass,
			String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	protected Mono<UpdateResult> doUpdate(String collectionName, Query query, @Nullable UpdateDefinition update,
			@Nullable Class<?> entityClass, boolean upsert, boolean multi) {

		if (query.isSorted() && LOGGER.isWarnEnabled()) {

			LOGGER.warn("{} does not support sort ('{}'). Please use findAndModify() instead.",
					upsert ? "Upsert" : "UpdateFirst", serializeToJsonSafely(query.getSortObject()));
		}

		MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);
		increaseVersionForUpdateIfNecessary(entity, update);

		Document queryObj = queryMapper.getMappedObject(query.getQueryObject(), entity);

		UpdateOptions updateOptions = new UpdateOptions().upsert(upsert);
		operations.forType(entityClass).getCollation(query) //
				.map(Collation::toMongoCollation) //
				.ifPresent(updateOptions::collation);

		if (update.hasArrayFilters()) {

			updateOptions.arrayFilters(update.getArrayFilters().stream().map(ArrayFilter::asDocument)
					.map(it -> queryMapper.getMappedObject(it, entity)).collect(Collectors.toList()));
		}

		if (multi && update.isIsolated() && !queryObj.containsKey("$isolated")) {
			queryObj.put("$isolated", 1);
		}

		Flux<UpdateResult> result;

		if (update instanceof AggregationUpdate) {

			AggregationOperationContext context = entityClass != null
					? new RelaxedTypeBasedAggregationOperationContext(entityClass, mappingContext, queryMapper)
					: Aggregation.DEFAULT_CONTEXT;

			List<Document> pipeline = new AggregationUtil(queryMapper, mappingContext)
					.createPipeline((AggregationUpdate) update, context);

			result = execute(collectionName, collection -> {

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("Calling update using query: %s and update: %s in collection: %s",
							serializeToJsonSafely(queryObj), serializeToJsonSafely(pipeline), collectionName));
				}

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName,
						entityClass, update.getUpdateObject(), queryObj);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

				collection = writeConcernToUse != null ? collection.withWriteConcern(writeConcernToUse) : collection;

				return multi ? collection.updateMany(queryObj, pipeline, updateOptions)
						: collection.updateOne(queryObj, pipeline, updateOptions);
			});
		} else {

			result = execute(collectionName, collection -> {

				Document updateObj = updateMapper.getMappedObject(update.getUpdateObject(), entity);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("Calling update using query: %s and update: %s in collection: %s",
							serializeToJsonSafely(queryObj), serializeToJsonSafely(updateObj), collectionName));
				}

				MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.UPDATE, collectionName,
						entityClass, updateObj, queryObj);
				WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
				MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

				if (!UpdateMapper.isUpdateObject(updateObj)) {

					ReplaceOptions replaceOptions = new ReplaceOptions();
					replaceOptions.upsert(updateOptions.isUpsert());
					replaceOptions.collation(updateOptions.getCollation());

					return collectionToUse.replaceOne(queryObj, updateObj, replaceOptions);
				}

				return multi ? collectionToUse.updateMany(queryObj, updateObj, updateOptions)
						: collectionToUse.updateOne(queryObj, updateObj, updateOptions);
			});
		}

		result = result.doOnNext(updateResult -> {

			if (entity != null && entity.hasVersionProperty() && !multi) {
				if (updateResult.wasAcknowledged() && updateResult.getMatchedCount() == 0) {

					Document updateObj = updateMapper.getMappedObject(update.getUpdateObject(), entity);
					if (containsVersionProperty(queryObj, entity))
						throw new OptimisticLockingFailureException("Optimistic lock exception on saving entity: "
								+ updateObj.toString() + " to collection " + collectionName);
				}
			}
		});

		return result.next();
	}

	private void increaseVersionForUpdateIfNecessary(@Nullable MongoPersistentEntity<?> persistentEntity,
			UpdateDefinition update) {

		if (persistentEntity != null && persistentEntity.hasVersionProperty()) {
			String versionFieldName = persistentEntity.getRequiredVersionProperty().getFieldName();
			if (!update.modifies(versionFieldName)) {
				update.inc(versionFieldName);
			}
		}
	}

	private boolean containsVersionProperty(Document document, @Nullable MongoPersistentEntity<?> persistentEntity) {

		if (persistentEntity == null || !persistentEntity.hasVersionProperty()) {
			return false;
		}

		return document.containsKey(persistentEntity.getRequiredVersionProperty().getFieldName());
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

		return remove(operations.forEntity(object).getRemoveByQuery(), object.getClass());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(java.lang.Object, java.lang.String)
	 */
	public Mono<DeleteResult> remove(Object object, String collectionName) {

		Assert.notNull(object, "Object must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		return doRemove(collectionName, operations.forEntity(object).getRemoveByQuery(), object.getClass());
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
		return remove(query, entityClass, getCollectionName(entityClass));
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

		Document queryObject = query.getQueryObject();
		MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);
		Document removeQuery = queryMapper.getMappedObject(queryObject, entity);

		return execute(collectionName, collection -> {

			maybeEmitEvent(new BeforeDeleteEvent<>(removeQuery, entityClass, collectionName));

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName, entityClass,
					null, removeQuery);

			DeleteOptions deleteOptions = new DeleteOptions();

			operations.forType(entityClass).getCollation(query) //
					.map(Collation::toMongoCollation) //
					.ifPresent(deleteOptions::collation);

			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
			MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Remove using query: {} in collection: {}.",
						new Object[] { serializeToJsonSafely(removeQuery), collectionName });
			}

			if (query.getLimit() > 0 || query.getSkip() > 0) {

				FindPublisher<Document> cursor = new QueryFindPublisherPreparer(query, entityClass)
						.prepare(collection.find(removeQuery)) //
						.projection(MappedDocument.getIdOnlyProjection());

				return Flux.from(cursor) //
						.map(MappedDocument::of) //
						.map(MappedDocument::getId) //
						.collectList() //
						.flatMapMany(val -> {
							return collectionToUse.deleteMany(MappedDocument.getIdIn(val), deleteOptions);
						});
			} else {
				return collectionToUse.deleteMany(removeQuery, deleteOptions);
			}

		}).doOnNext(it -> maybeEmitEvent(new AfterDeleteEvent<>(queryObject, entityClass, collectionName))) //
				.next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAll(java.lang.Class)
	 */
	public <T> Flux<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAll(java.lang.Class, java.lang.String)
	 */
	public <T> Flux<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(null), FindPublisherPreparer.NO_OP_PREPARER,
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
		return findAllAndRemove(query, entityClass, getCollectionName(entityClass));
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
		return tail(query, entityClass, getCollectionName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#tail(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> Flux<T> tail(@Nullable Query query, Class<T> entityClass, String collectionName) {

		if (query == null) {

			LOGGER.debug(String.format("Tail for class: %s in collection: %s", entityClass, collectionName));

			return executeFindMultiInternal(
					collection -> new FindCallback(null).doInCollection(collection).cursorType(CursorType.TailableAwait),
					FindPublisherPreparer.NO_OP_PREPARER, new ReadDocumentCallback<>(mongoConverter, entityClass, collectionName),
					collectionName);
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
		publisher = options.getResumeBsonTimestamp().map(publisher::startAtOperationTime).orElse(publisher);
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

		return mapReduce(filterQuery, domainType, getCollectionName(domainType), resultType, mapFunction, reduceFunction,
				options);
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

			if (StringUtils.hasText(options.getOutputCollection()) && !options.usesInlineOutput()) {
				publisher = publisher.collectionName(options.getOutputCollection()).action(options.getMapReduceAction());

				if (options.getOutputDatabase().isPresent()) {
					publisher = publisher.databaseName(options.getOutputDatabase().get());
				}
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveChangeStreamOperation#changeStream(java.lang.Class)
	 */
	@Override
	public <T> ReactiveChangeStream<T> changeStream(Class<T> domainType) {
		return new ReactiveChangeStreamOperationSupport(this).changeStream(domainType);
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

		return Flux.from(flux).collectList().filter(it -> !it.isEmpty())
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
	protected Mono<MongoCollection<Document>> doCreateCollection(String collectionName,
			CreateCollectionOptions collectionOptions) {

		return createMono(db -> db.createCollection(collectionName, collectionOptions)).doOnSuccess(it -> {


				// TODO: Emit a collection created event
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Created collection [{}]", collectionName);
				}


		}).thenReturn(getCollection(collectionName));
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

		return doFindOne(collectionName, query, fields, entityClass,
				findPublisher -> collation != null ? findPublisher.collation(collation.toMongoCollation()) : findPublisher);
	}

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to an object using the template's converter.
	 * The query document is specified as a standard {@link Document} and so is the fields specification.
	 *
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param query the query document that specifies the criteria used to find a record.
	 * @param fields the document that specifies the fields to be returned.
	 * @param entityClass the parameterized type of the returned list.
	 * @param preparer the preparer modifying collection and publisher to fit the needs.
	 * @return the {@link List} of converted objects.
	 * @since 2.2
	 */
	protected <T> Mono<T> doFindOne(String collectionName, Document query, @Nullable Document fields,
			Class<T> entityClass, FindPublisherPreparer preparer) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);
		Document mappedFields = fields == null ? null : queryMapper.getMappedObject(fields, entity);

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

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(sourceClass);

		Document mappedFields = getMappedFieldsObject(fields, entity, targetClass);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find using query: {} fields: {} for class: {} in collection: {}",
					serializeToJsonSafely(mappedQuery), mappedFields, sourceClass, collectionName);
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields), preparer,
				new ProjectingReadCallback<>(mongoConverter, sourceClass, targetClass, collectionName), collectionName);
	}

	private Document getMappedFieldsObject(Document fields, @Nullable MongoPersistentEntity<?> entity,
			Class<?> targetType) {

		if (entity == null) {
			return fields;
		}

		Document projectedFields = propertyOperations.computeFieldsForProjection(projectionFactory, fields,
				entity.getType(), targetType);

		if (ObjectUtils.nullSafeEquals(fields, projectedFields)) {
			return queryMapper.getMappedFields(projectedFields, entity);
		}

		return queryMapper.getMappedFields(projectedFields, mappingContext.getRequiredPersistentEntity(targetType));
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
			Class<T> entityClass, UpdateDefinition update, FindAndModifyOptions options) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		increaseVersionForUpdateIfNecessary(entity, update);

		return Mono.defer(() -> {

			Document mappedQuery = queryMapper.getMappedObject(query, entity);

			Object mappedUpdate;
			if (update instanceof AggregationUpdate) {

				AggregationOperationContext context = new RelaxedTypeBasedAggregationOperationContext(entityClass,
						mappingContext, queryMapper);

				mappedUpdate = new AggregationUtil(queryMapper, mappingContext).createPipeline((Aggregation) update, context);
			} else {
				mappedUpdate = updateMapper.getMappedObject(update.getUpdateObject(), entity);
			}

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format(
						"findAndModify using query: %s fields: %s sort: %s for class: %s and update: %s " + "in collection: %s",
						serializeToJsonSafely(mappedQuery), fields, sort, entityClass, serializeToJsonSafely(mappedUpdate),
						collectionName));
			}

			return executeFindOneInternal(
					new FindAndModifyCallback(mappedQuery, fields, sort, mappedUpdate,
							update.getArrayFilters().stream().map(ArrayFilter::asDocument).collect(Collectors.toList()), options),
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

	@SuppressWarnings("unchecked")
	protected <T> Mono<T> maybeCallBeforeConvert(T object, String collection) {

		if (null != entityCallbacks) {
			return entityCallbacks.callback(ReactiveBeforeConvertCallback.class, object, collection);
		}

		return Mono.just(object);
	}

	@SuppressWarnings("unchecked")
	protected <T> Mono<T> maybeCallBeforeSave(T object, Document document, String collection) {

		if (null != entityCallbacks) {
			return entityCallbacks.callback(ReactiveBeforeSaveCallback.class, object, document, collection);
		}

		return Mono.just(object);
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
			return Flux.from(preparer.initiateFind(collection, collectionCallback::doInCollection))
					.map(objectCallback::doWith);
		});
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

	private MappingMongoConverter getDefaultMongoConverter() {

		MongoCustomConversions conversions = new MongoCustomConversions(Collections.emptyList());

		MongoMappingContext context = new MongoMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(NO_OP_REF_RESOLVER, context);
		converter.setCustomConversions(conversions);
		converter.setCodecRegistryProvider(this.mongoDatabaseFactory);
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
	 * @author Christoph Strobl
	 */
	private static class FindOneCallback implements ReactiveCollectionCallback<Document> {

		private final Document query;
		private final Optional<Document> fields;
		private final FindPublisherPreparer preparer;

		FindOneCallback(Document query, @Nullable Document fields, FindPublisherPreparer preparer) {
			this.query = query;
			this.fields = Optional.ofNullable(fields);
			this.preparer = preparer;
		}

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			if (LOGGER.isDebugEnabled()) {

				LOGGER.debug("findOne using query: {} fields: {} in db.collection: {}", serializeToJsonSafely(query),
						serializeToJsonSafely(fields.orElseGet(Document::new)), collection.getNamespace().getFullName());
			}

			FindPublisher<Document> publisher = preparer.initiateFind(collection, col -> col.find(query, Document.class));

			if (fields.isPresent()) {
				publisher = publisher.projection(fields.get());
			}

			return publisher.limit(1).first();
		}
	}

	/**
	 * Simple {@link ReactiveCollectionQueryCallback} that takes a query {@link Document} plus an optional fields
	 * specification {@link Document} and executes that against the {@link MongoCollection}.
	 *
	 * @author Mark Paluch
	 */
	@RequiredArgsConstructor
	private static class FindCallback implements ReactiveCollectionQueryCallback<Document> {

		private final @Nullable Document query;
		private final @Nullable Document fields;

		FindCallback(@Nullable Document query) {
			this(query, null);
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
	@RequiredArgsConstructor
	private static class FindAndModifyCallback implements ReactiveCollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Object update;
		private final List<Document> arrayFilters;
		private final FindAndModifyOptions options;

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			if (options.isRemove()) {
				FindOneAndDeleteOptions findOneAndDeleteOptions = convertToFindOneAndDeleteOptions(fields, sort);

				findOneAndDeleteOptions = options.getCollation().map(Collation::toMongoCollation)
						.map(findOneAndDeleteOptions::collation).orElse(findOneAndDeleteOptions);

				return collection.findOneAndDelete(query, findOneAndDeleteOptions);
			}

			FindOneAndUpdateOptions findOneAndUpdateOptions = convertToFindOneAndUpdateOptions(options, fields, sort,
					arrayFilters);
			if (update instanceof Document) {
				return collection.findOneAndUpdate(query, (Document) update, findOneAndUpdateOptions);
			} else if (update instanceof List) {
				return collection.findOneAndUpdate(query, (List<Document>) update, findOneAndUpdateOptions);
			}

			return Flux
					.error(new IllegalArgumentException(String.format("Using %s is not supported in findOneAndUpdate", update)));
		}

		private static FindOneAndUpdateOptions convertToFindOneAndUpdateOptions(FindAndModifyOptions options,
				Document fields, Document sort, List<Document> arrayFilters) {

			FindOneAndUpdateOptions result = new FindOneAndUpdateOptions();

			result = result.projection(fields).sort(sort).upsert(options.isUpsert());

			if (options.isReturnNew()) {
				result = result.returnDocument(ReturnDocument.AFTER);
			} else {
				result = result.returnDocument(ReturnDocument.BEFORE);
			}

			result = options.getCollation().map(Collation::toMongoCollation).map(result::collation).orElse(result);
			result.arrayFilters(arrayFilters);

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
	 * @author Chrstoph Strobl
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
	 * @author Mark Paluch
	 */
	class QueryFindPublisherPreparer implements FindPublisherPreparer {

		private final Query query;
		private final @Nullable Class<?> type;

		QueryFindPublisherPreparer(Query query, @Nullable Class<?> type) {

			this.query = query;
			this.type = type;
		}

		@SuppressWarnings("deprecation")
		public FindPublisher<Document> prepare(FindPublisher<Document> findPublisher) {

			FindPublisher<Document> findPublisherToUse = operations.forType(type) //
					.getCollation(query) //
					.map(Collation::toMongoCollation) //
					.map(findPublisher::collation) //
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

		@Override
		public ReadPreference getReadPreference() {
			return query.getMeta().getFlags().contains(CursorOption.SLAVE_OK) ? ReadPreference.primaryPreferred() : null;
		}
	}

	class TailingQueryFindPublisherPreparer extends QueryFindPublisherPreparer {

		TailingQueryFindPublisherPreparer(Query query, Class<?> type) {
			super(query, type);
		}

		@Override
		public FindPublisher<Document> prepare(FindPublisher<Document> findPublisher) {
			return super.prepare(findPublisher.cursorType(CursorType.TailableAwait));
		}
	}

	private static List<? extends Document> toDocuments(Collection<? extends Document> documents) {
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

	/**
	 * Value object chaining together a given source document with its mapped representation and the collection to persist
	 * it to.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.2
	 */
	private static class PersistableEntityModel<T> {

		private final T source;
		private final @Nullable Document target;
		private final String collection;

		private PersistableEntityModel(T source, @Nullable Document target, String collection) {

			this.source = source;
			this.target = target;
			this.collection = collection;
		}

		static <T> PersistableEntityModel<T> of(T source, String collection) {
			return new PersistableEntityModel<>(source, null, collection);
		}

		static <T> PersistableEntityModel<T> of(T source, Document target, String collection) {
			return new PersistableEntityModel<>(source, target, collection);
		}

		PersistableEntityModel<T> mutate(T source) {
			return new PersistableEntityModel(source, target, collection);
		}

		PersistableEntityModel<T> addTargetDocument(Document target) {
			return new PersistableEntityModel(source, target, collection);
		}

		T getSource() {
			return source;
		}

		@Nullable
		Document getTarget() {
			return target;
		}

		String getCollection() {
			return collection;
		}
	}
}
