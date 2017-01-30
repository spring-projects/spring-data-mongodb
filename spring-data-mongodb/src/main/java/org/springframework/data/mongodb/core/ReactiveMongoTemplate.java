/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
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
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Metric;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DbRefProxyHandler;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DbRefResolverCallback;
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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBRef;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.util.JSONParseException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

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

	public static final DbRefResolver NO_OP_REF_RESOLVER = new NoOpDbRefResolver();

	private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMongoTemplate.class);
	private static final String ID_FIELD = "_id";
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

	private WriteConcern writeConcern;
	private WriteConcernResolver writeConcernResolver = DefaultWriteConcernResolver.INSTANCE;
	private WriteResultChecking writeResultChecking = WriteResultChecking.NONE;
	private ReadPreference readPreference;
	private ApplicationEventPublisher eventPublisher;
	private MongoPersistentEntityIndexCreator indexCreator;

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 */
	public ReactiveMongoTemplate(MongoClient mongoClient, String databaseName) {
		this(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName), null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDatabaseFactory must not be {@literal null}.
	 */
	public ReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory) {
		this(mongoDatabaseFactory, null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param mongoDatabaseFactory must not be {@literal null}.
	 * @param mongoConverter
	 */
	public ReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory, MongoConverter mongoConverter) {

		Assert.notNull(mongoDatabaseFactory, "ReactiveMongoDatabaseFactory must not be null!");

		this.mongoDatabaseFactory = mongoDatabaseFactory;
		this.exceptionTranslator = mongoDatabaseFactory.getExceptionTranslator();
		this.mongoConverter = mongoConverter == null ? getDefaultMongoConverter() : mongoConverter;
		this.queryMapper = new QueryMapper(this.mongoConverter);
		this.updateMapper = new UpdateMapper(this.mongoConverter);

		// We always have a mapping context in the converter, whether it's a simple one or not
		mappingContext = this.mongoConverter.getMappingContext();
		// We create indexes based on mapping events

		if (null != mappingContext && mappingContext instanceof MongoMappingContext) {
			indexCreator = new MongoPersistentEntityIndexCreator((MongoMappingContext) mappingContext,
					(collectionName) -> IndexOperationsAdapter.blocking(indexOps(collectionName)));
			eventPublisher = new MongoMappingEventPublisher(indexCreator);
			if (mappingContext instanceof ApplicationEventPublisherAware) {
				((ApplicationEventPublisherAware) mappingContext).setApplicationEventPublisher(eventPublisher);
			}
		}
	}

	/**
	 * Configures the {@link WriteResultChecking} to be used with the template. Setting {@literal null} will reset the
	 * default of {@link ReactiveMongoTemplate#DEFAULT_WRITE_RESULT_CHECKING}.
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
	 * Used by @{link {@link #prepareCollection(MongoCollection)} to set the {@link ReadPreference} before any operations
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
	}

	/**
	 * Inspects the given {@link ApplicationContext} for {@link MongoPersistentEntityIndexCreator} and those in turn if
	 * they were registered for the current {@link MappingContext}. If no creator for the current {@link MappingContext}
	 * can be found we manually add the internally created one as {@link ApplicationListener} to make sure indexes get
	 * created appropriately for entity types persisted through this {@link ReactiveMongoTemplate} instance.
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
	 * Returns the default {@link MongoConverter}.
	 *
	 * @return
	 */
	public MongoConverter getConverter() {
		return this.mongoConverter;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#reactiveIndexOps(java.lang.String)
	 */
	public ReactiveIndexOperations indexOps(String collectionName) {
		return new DefaultReactiveIndexOperations(this, collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#reactiveIndexOps(java.lang.Class)
	 */
	public ReactiveIndexOperations indexOps(Class<?> entityClass) {
		return new DefaultReactiveIndexOperations(this, determineCollectionName(entityClass));
	}

	public String getCollectionName(Class<?> entityClass) {
		return this.determineCollectionName(entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#executeCommand(java.lang.String)
	 */
	public Mono<Document> executeCommand(String jsonCommand) {

		Assert.notNull(jsonCommand, "Command must not be empty!");

		return executeCommand(Document.parse(jsonCommand));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#executeCommand(org.bson.Document)
	 */
	public Mono<Document> executeCommand(final Document command) {

		Assert.notNull(command, "Command must not be null!");

		return createFlux(db -> readPreference != null ? db.runCommand(command, readPreference) : db.runCommand(command))
				.next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#executeCommand(org.bson.Document, com.mongodb.ReadPreference)
	 */
	public Mono<Document> executeCommand(final Document command, final ReadPreference readPreference) {

		Assert.notNull(command, "Command must not be null!");

		return createFlux(db -> readPreference != null ? db.runCommand(command, readPreference) : db.runCommand(command))
				.next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#execute(java.lang.Class, org.springframework.data.mongodb.core.ReactiveCollectionCallback)
	 */
	@Override
	public <T> Flux<T> execute(Class<?> entityClass, ReactiveCollectionCallback<T> action) {
		return createFlux(determineCollectionName(entityClass), action);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#execute(org.springframework.data.mongodb.core.ReactiveDbCallback)
	 */
	@Override
	public <T> Flux<T> execute(ReactiveDatabaseCallback<T> action) {
		return createFlux(action);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#execute(java.lang.String, org.springframework.data.mongodb.core.ReactiveCollectionCallback)
	 */
	public <T> Flux<T> execute(String collectionName, ReactiveCollectionCallback<T> callback) {

		Assert.notNull(callback, "ReactiveCollectionCallback must not be null!");
		return createFlux(collectionName, callback);
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

		return Flux.defer(() -> callback.doInDB(getMongoDatabase())).onErrorResumeWith(translateFluxException());
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

		return Mono.defer(() -> Mono.from(callback.doInDB(getMongoDatabase()))).otherwise(translateMonoException());
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
				.fromCallable(() -> getAndPrepareCollection(getMongoDatabase(), collectionName));

		return collectionPublisher.flatMap(callback::doInCollection).onErrorResumeWith(translateFluxException());
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
				.fromCallable(() -> getAndPrepareCollection(getMongoDatabase(), collectionName));

		return collectionPublisher.then(collection -> Mono.from(callback.doInCollection(collection)))
				.otherwise(translateMonoException());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.Class)
	 */
	public <T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass) {
		return createCollection(determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.Class, org.springframework.data.mongodb.core.CollectionOptions)
	 */
	public <T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass,
			CollectionOptions collectionOptions) {
		return createCollection(determineCollectionName(entityClass), collectionOptions);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.String)
	 */
	public Mono<MongoCollection<Document>> createCollection(final String collectionName) {
		return doCreateCollection(collectionName, new CreateCollectionOptions());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#createCollection(java.lang.String, org.springframework.data.mongodb.core.CollectionOptions)
	 */
	public Mono<MongoCollection<Document>> createCollection(final String collectionName,
			final CollectionOptions collectionOptions) {
		return doCreateCollection(collectionName, convertToCreateCollectionOptions(collectionOptions));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#getCollection(java.lang.String)
	 */
	public MongoCollection<Document> getCollection(final String collectionName) {
		return execute((MongoDatabaseCallback<MongoCollection<Document>>) db -> db.getCollection(collectionName));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#collectionExists(java.lang.Class)
	 */
	public <T> Mono<Boolean> collectionExists(Class<T> entityClass) {
		return collectionExists(determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#collectionExists(java.lang.String)
	 */
	public Mono<Boolean> collectionExists(final String collectionName) {
		return createMono(db -> Flux.from(db.listCollectionNames()) //
				.filter(s -> s.equals(collectionName)) //
				.map(s -> true) //
				.single(false));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#dropCollection(java.lang.Class)
	 */
	public <T> Mono<Void> dropCollection(Class<T> entityClass) {
		return dropCollection(determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#dropCollection(java.lang.String)
	 */
	public Mono<Void> dropCollection(final String collectionName) {

		return createMono(db -> db.getCollection(collectionName).drop()).doOnSuccess(success -> {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Dropped collection [" + collectionName + "]");
			}
		}).then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#getCollectionNames()
	 */
	public Flux<String> getCollectionNames() {
		return createFlux(MongoDatabase::listCollectionNames);
	}

	public MongoDatabase getMongoDatabase() {
		return mongoDatabaseFactory.getMongoDatabase();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findOne(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Mono<T> findOne(Query query, Class<T> entityClass) {
		return findOne(query, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findOne(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findOne(Query query, Class<T> entityClass, String collectionName) {

		if (query.getSortObject() == null) {
			return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass);
		}

		query.limit(1);
		return find(query, entityClass, collectionName).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#exists(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public Mono<Boolean> exists(Query query, Class<?> entityClass) {
		return exists(query, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#exists(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	public Mono<Boolean> exists(Query query, String collectionName) {
		return exists(query, null, collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#exists(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public Mono<Boolean> exists(final Query query, final Class<?> entityClass, String collectionName) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to exist can't be null");
		}

		return createFlux(collectionName, collection -> {

			Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(), getPersistentEntity(entityClass));
			return collection.find(mappedQuery).limit(1);
		}).hasElements();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#find(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Flux<T> find(Query query, Class<T> entityClass) {
		return find(query, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#find(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public <T> Flux<T> find(final Query query, Class<T> entityClass, String collectionName) {

		if (query == null) {
			return findAll(entityClass, collectionName);
		}

		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
				new QueryFindPublisherPreparer(query, entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findById(java.lang.Object, java.lang.Class)
	 */
	public <T> Mono<T> findById(Object id, Class<T> entityClass) {
		return findById(id, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findById(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findById(Object id, Class<T> entityClass, String collectionName) {

		Optional<? extends MongoPersistentEntity<?>> persistentEntity = mappingContext.getPersistentEntity(entityClass);
		MongoPersistentProperty idProperty = persistentEntity.isPresent() ? persistentEntity.get().getIdProperty().orElse(null) : null;

		String idKey = idProperty == null ? ID_FIELD : idProperty.getName();

		return doFindOne(collectionName, new Document(idKey, id), null, entityClass);
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
	@SuppressWarnings("unchecked")
	public <T> Flux<GeoResult<T>> geoNear(NearQuery near, Class<T> entityClass, String collectionName) {

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

			GeoNearResultDbObjectCallback<T> callback = new GeoNearResultDbObjectCallback<T>(
					new ReadDocumentCallback<T>(mongoConverter, entityClass, collectionName), near.getMetric());

			return executeCommand(command, this.readPreference).flatMap(document -> {

				List<Document> l = document.get("results", List.class);
				if (l == null) {
					return Flux.empty();
				}
				return Flux.fromIterable(l);
			}).skip(near.getSkip() != null ? near.getSkip() : 0).map(new Function<Document, GeoResult<T>>() {
				@Override
				public GeoResult<T> apply(Document object) {
					return callback.doWith(object);
				}
			});
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class)
	 */
	public <T> Mono<T> findAndModify(Query query, Update update, Class<T> entityClass) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndModify(Query query, Update update, Class<T> entityClass, String collectionName) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, org.springframework.data.mongodb.core.FindAndModifyOptions, java.lang.Class)
	 */
	public <T> Mono<T> findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass) {
		return findAndModify(query, update, options, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndModify(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, org.springframework.data.mongodb.core.FindAndModifyOptions, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass,
			String collectionName) {
		return doFindAndModify(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass, update, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass, String collectionName) {

		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public Mono<Long> count(Query query, Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null!");

		return count(query, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	public Mono<Long> count(final Query query, String collectionName) {
		return count(query, null, collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#count(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public Mono<Long> count(final Query query, final Class<?> entityClass, String collectionName) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		return createMono(collectionName, collection -> {

			final Document Document = query == null ? null
					: queryMapper.getMappedObject(query.getQueryObject(),
							entityClass == null ? null : mappingContext.getPersistentEntity(entityClass));

			return collection.count(Document);
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(reactor.core.publisher.Mono)
	 */
	@Override
	public <T> Mono<T> insert(Mono<? extends T> objectToSave) {
		return objectToSave.then(this::insert);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(org.reactivestreams.Publisher, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> batchToSave, Class<?> entityClass) {
		return insertAll(batchToSave, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(org.reactivestreams.Publisher, java.lang.String)
	 */
	@Override
	public <T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> batchToSave, String collectionName) {
		return Flux.from(batchToSave).flatMap(collection -> insert(collection, collectionName));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.lang.Object)
	 */
	public <T> Mono<T> insert(T objectToSave) {

		ensureNotIterable(objectToSave);
		return insert(objectToSave, determineEntityCollectionName(objectToSave));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.lang.Object, java.lang.String)
	 */
	public <T> Mono<T> insert(T objectToSave, String collectionName) {

		ensureNotIterable(objectToSave);
		return doInsert(collectionName, objectToSave, this.mongoConverter);
	}

	protected <T> Mono<T> doInsert(String collectionName, T objectToSave, MongoWriter<Object> writer) {

		assertUpdateableIdIfNotSet(objectToSave);

		return Mono.defer(() -> {

			initializeVersionProperty(objectToSave);
			maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));

			Document dbDoc = toDbObject(objectToSave, writer);

			maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc, collectionName));

			Mono<T> afterInsert = insertDBObject(collectionName, dbDoc, objectToSave.getClass()).then(id -> {
				populateIdIfNecessary(objectToSave, id);
				maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc, collectionName));
				return Mono.just(objectToSave);
			});

			return afterInsert;
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.util.Collection, java.lang.Class)
	 */
	public <T> Flux<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass) {
		return doInsertBatch(determineCollectionName(entityClass), batchToSave, this.mongoConverter);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insert(java.util.Collection, java.lang.String)
	 */
	public <T> Flux<T> insert(Collection<? extends T> batchToSave, String collectionName) {
		return doInsertBatch(collectionName, batchToSave, this.mongoConverter);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insertAll(java.util.Collection)
	 */
	public <T> Flux<T> insertAll(Collection<? extends T> objectsToSave) {
		return doInsertAll(objectsToSave, this.mongoConverter);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#insertAll(org.reactivestreams.Publisher)
	 */
	@Override
	public <T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> objectsToSave) {
		return Flux.from(objectsToSave).flatMap(this::insertAll);
	}

	protected <T> Flux<T> doInsertAll(Collection<? extends T> listToSave, MongoWriter<Object> writer) {

		final Map<String, List<T>> elementsByCollection = new HashMap<String, List<T>>();

		listToSave.forEach(element -> {
			MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(element.getClass());

			String collection = entity.getCollection();
			List<T> collectionElements = elementsByCollection.get(collection);

			if (null == collectionElements) {
				collectionElements = new ArrayList<T>();
				elementsByCollection.put(collection, collectionElements);
			}

			collectionElements.add(element);
		});

		return Flux.fromIterable(elementsByCollection.keySet())
				.flatMap(collectionName -> doInsertBatch(collectionName, elementsByCollection.get(collectionName), writer));
	}

	protected <T> Flux<T> doInsertBatch(final String collectionName, final Collection<? extends T> batchToSave,
			final MongoWriter<Object> writer) {

		Assert.notNull(writer, "MongoWriter must not be null!");

		Mono<List<Tuple2<T, Document>>> prepareDocuments = Flux.fromIterable(batchToSave)
				.flatMap(new Function<T, Flux<Tuple2<T, Document>>>() {
					@Override
					public Flux<Tuple2<T, Document>> apply(T o) {

						initializeVersionProperty(o);
						maybeEmitEvent(new BeforeConvertEvent<T>(o, collectionName));

						Document dbDoc = toDbObject(o, writer);

						maybeEmitEvent(new BeforeSaveEvent<T>(o, dbDoc, collectionName));
						return Flux.zip(Mono.just(o), Mono.just(dbDoc));
					}
				}).collectList();

		Flux<Tuple2<T, Document>> insertDocuments = prepareDocuments.flatMap(tuples -> {

			List<Document> dbObjects = tuples.stream().map(Tuple2::getT2).collect(Collectors.toList());

			return insertDocumentList(collectionName, dbObjects).thenMany(Flux.fromIterable(tuples));
		});

		return insertDocuments.map(tuple -> {

			populateIdIfNecessary(tuple.getT1(), tuple.getT2().get(ID_FIELD));
			maybeEmitEvent(new AfterSaveEvent<T>(tuple.getT1(), tuple.getT2(), collectionName));
			return tuple.getT1();
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#save(reactor.core.publisher.Mono)
	 */
	@Override
	public <T> Mono<T> save(Mono<? extends T> objectToSave) {
		return objectToSave.then(this::save);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#save(reactor.core.publisher.Mono, java.lang.String)
	 */
	@Override
	public <T> Mono<T> save(Mono<? extends T> objectToSave, String collectionName) {
		return objectToSave.then(o -> save(o, collectionName));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#save(java.lang.Object)
	 */
	public <T> Mono<T> save(T objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null!");
		return save(objectToSave, determineEntityCollectionName(objectToSave));
	}

	/* (non-Javadoc)
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

		return createMono(collectionName, collection -> {

			ConvertingPropertyAccessor convertingAccessor = new ConvertingPropertyAccessor(
					entity.getPropertyAccessor(objectToSave), mongoConverter.getConversionService());

			MongoPersistentProperty idProperty = entity.getIdProperty().orElseThrow(() -> new IllegalArgumentException("No id property present!"));
			MongoPersistentProperty versionProperty = entity.getVersionProperty().orElseThrow(() -> new IllegalArgumentException("No version property present!"));;

			Optional<Object> version = convertingAccessor.getProperty(versionProperty);
			Optional<Number> versionNumber = convertingAccessor.getProperty(versionProperty, Number.class);

			// Fresh instance -> initialize version property
			if (!version.isPresent()) {
				return doInsert(collectionName, objectToSave, mongoConverter);
			}

			ReactiveMongoTemplate.this.assertUpdateableIdIfNotSet(objectToSave);

			// Create query for entity with the id and old version
			Object id = convertingAccessor.getProperty(idProperty);
			Query query = new Query(Criteria.where(idProperty.getName()).is(id).and(versionProperty.getName()).is(version));

			// Bump version number
			convertingAccessor.setProperty(versionProperty, Optional.of(versionNumber.orElse(0).longValue() + 1));

			ReactiveMongoTemplate.this.maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));

			Document document = ReactiveMongoTemplate.this.toDbObject(objectToSave, mongoConverter);

			ReactiveMongoTemplate.this.maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, document, collectionName));
			Update update = Update.fromDocument(document, ID_FIELD);

			return doUpdate(collectionName, query, update, objectToSave.getClass(), false, false).map(updateResult -> {

				maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, document, collectionName));
				return objectToSave;
			});
		});
	}

	protected <T> Mono<T> doSave(String collectionName, T objectToSave, MongoWriter<Object> writer) {

		assertUpdateableIdIfNotSet(objectToSave);

		return createMono(collectionName, collection -> {

			maybeEmitEvent(new BeforeConvertEvent<T>(objectToSave, collectionName));
			Document dbDoc = toDbObject(objectToSave, writer);
			maybeEmitEvent(new BeforeSaveEvent<T>(objectToSave, dbDoc, collectionName));

			return saveDocument(collectionName, dbDoc, objectToSave.getClass()).map(id -> {

				populateIdIfNecessary(objectToSave, id);
				maybeEmitEvent(new AfterSaveEvent<T>(objectToSave, dbDoc, collectionName));
				return objectToSave;
			});
		});
	}

	protected Mono<Object> insertDBObject(final String collectionName, final Document dbDoc, final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Inserting Document containing fields: " + dbDoc.keySet() + " in collection: " + collectionName);
		}

		final Document document = new Document(dbDoc);
		Flux<Success> execute = execute(collectionName, collection -> {

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.INSERT, collectionName, entityClass,
					dbDoc, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

			MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

			return collectionToUse.insertOne(document);
		});

		return Flux.from(execute).last().map(success -> document.get(ID_FIELD));
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

			List<Document> documentsWithIds = documents.stream()
					.filter(document -> document.get(ID_FIELD) instanceof ObjectId).collect(Collectors.toList());
			return Flux.fromIterable(documentsWithIds);
		}).map(document -> document.get(ID_FIELD, ObjectId.class));
	}

	private MongoCollection<Document> prepareCollection(MongoCollection<Document> collection,
			WriteConcern writeConcernToUse) {
		MongoCollection<Document> collectionToUse = collection;

		if (writeConcernToUse != null) {
			collectionToUse = collectionToUse.withWriteConcern(writeConcernToUse);
		}
		return collectionToUse;
	}

	protected Mono<Object> saveDocument(final String collectionName, final Document dbDoc, final Class<?> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Saving Document containing fields: " + dbDoc.keySet());
		}

		return createMono(collectionName, collection -> {

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.SAVE, collectionName, entityClass,
					dbDoc, null);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);

			Publisher<?> publisher;
			if (!dbDoc.containsKey(ID_FIELD)) {
				if (writeConcernToUse == null) {
					publisher = collection.insertOne(dbDoc);
				} else {
					publisher = collection.withWriteConcern(writeConcernToUse).insertOne(dbDoc);
				}
			} else if (writeConcernToUse == null) {
				publisher = collection.replaceOne(Filters.eq(ID_FIELD, dbDoc.get(ID_FIELD)), dbDoc,
						new UpdateOptions().upsert(true));
			} else {
				publisher = collection.withWriteConcern(writeConcernToUse).replaceOne(Filters.eq(ID_FIELD, dbDoc.get(ID_FIELD)),
						dbDoc, new UpdateOptions().upsert(true));
			}

			return Mono.from(publisher).map(o -> dbDoc.get(ID_FIELD));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class)
	 */
	public Mono<UpdateResult> upsert(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, true, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.String)
	 */
	public Mono<UpdateResult> upsert(Query query, Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> upsert(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, true, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class)
	 */
	public Mono<UpdateResult> updateFirst(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.String)
	 */
	public Mono<UpdateResult> updateFirst(final Query query, final Update update, final String collectionName) {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateFirst(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> updateFirst(Query query, Update update, Class<?> entityClass, String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class)
	 */
	public Mono<UpdateResult> updateMulti(Query query, Update update, Class<?> entityClass) {
		return doUpdate(determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.String)
	 */
	public Mono<UpdateResult> updateMulti(final Query query, final Update update, String collectionName) {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update, java.lang.Class, java.lang.String)
	 */
	public Mono<UpdateResult> updateMulti(final Query query, final Update update, Class<?> entityClass,
			String collectionName) {
		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	protected Mono<UpdateResult> doUpdate(final String collectionName, final Query query, final Update update,
			final Class<?> entityClass, final boolean upsert, final boolean multi) {

		MongoPersistentEntity<?> entity = entityClass == null ? null : getPersistentEntity(entityClass);

		Flux<UpdateResult> result = execute(collectionName, collection -> {

			increaseVersionForUpdateIfNecessary(entity, update);

			Document queryObj = query == null ? new Document() : queryMapper.getMappedObject(query.getQueryObject(), entity);
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

			if (!UpdateMapper.isUpdateObject(updateObj)) {
				return collectionToUse.replaceOne(queryObj, updateObj, updateOptions);
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

	private void increaseVersionForUpdateIfNecessary(MongoPersistentEntity<?> persistentEntity, Update update) {

		if (persistentEntity != null && persistentEntity.hasVersionProperty()) {
			String versionFieldName = persistentEntity.getVersionProperty().get().getFieldName();
			if (!update.modifies(versionFieldName)) {
				update.inc(versionFieldName, 1L);
			}
		}
	}

	private boolean dbObjectContainsVersionProperty(Document document, MongoPersistentEntity<?> persistentEntity) {

		if (persistentEntity == null || !persistentEntity.hasVersionProperty()) {
			return false;
		}

		return document.containsKey(persistentEntity.getVersionProperty().get().getFieldName());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(reactor.core.publisher.Mono)
	 */
	@Override
	public Mono<DeleteResult> remove(Mono<? extends Object> objectToRemove) {
		return objectToRemove.then(this::remove);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(reactor.core.publisher.Mono, java.lang.String)
	 */
	@Override
	public Mono<DeleteResult> remove(Mono<? extends Object> objectToRemove, String collection) {
		return objectToRemove.then(o -> remove(objectToRemove, collection));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(java.lang.Object)
	 */
	public Mono<DeleteResult> remove(Object object) {

		if (object == null) {
			return null;
		}

		return remove(getIdQueryFor(object), object.getClass());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(java.lang.Object, java.lang.String)
	 */
	public Mono<DeleteResult> remove(Object object, String collection) {

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

		if (object instanceof Document) {
			return Collections.singletonMap(ID_FIELD, ((Document) object).get(ID_FIELD)).entrySet().iterator().next();
		}

		Optional<? extends MongoPersistentEntity<?>> entity = mappingContext.getPersistentEntity(objectType);
		MongoPersistentProperty idProp = entity.isPresent() ? entity.get().getIdProperty().orElse(null) : null;

		if (idProp == null) {
			throw new MappingException("No id property found for object of type " + objectType);
		}

		Object idValue = entity.get().getPropertyAccessor(object).getProperty(idProp);
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

		Optional<? extends MongoPersistentEntity<?>> persistentEntity = mappingContext.getPersistentEntity(entity.getClass());
		Optional<MongoPersistentProperty> idProperty = persistentEntity.isPresent() ? persistentEntity.get().getIdProperty() : Optional.empty();

		if (!idProperty.isPresent()) {
			return;
		}

		Optional<Object> idValue = persistentEntity.get().getPropertyAccessor(entity).getProperty(idProperty.get());

		if (!idValue.isPresent() && !MongoSimpleTypes.AUTOGENERATED_ID_TYPES.contains(idProperty.get().getType())) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Cannot autogenerate id of type %s for entity of type %s!", idProperty.get().getType().getName(),
							entity.getClass().getName()));
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	public Mono<DeleteResult> remove(Query query, String collectionName) {
		return remove(query, null, collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	public Mono<DeleteResult> remove(Query query, Class<?> entityClass) {
		return remove(query, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#remove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	public Mono<DeleteResult> remove(Query query, Class<?> entityClass, String collectionName) {
		return doRemove(collectionName, query, entityClass);
	}

	protected <T> Mono<DeleteResult> doRemove(final String collectionName, final Query query,
			final Class<T> entityClass) {

		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to remove can't be null!");
		}

		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		final Document queryObject = query.getQueryObject();
		final MongoPersistentEntity<?> entity = getPersistentEntity(entityClass);

		return execute(collectionName, collection -> {

			maybeEmitEvent(new BeforeDeleteEvent<T>(queryObject, entityClass, collectionName));

			Document dboq = queryMapper.getMappedObject(queryObject, entity);

			MongoAction mongoAction = new MongoAction(writeConcern, MongoActionOperation.REMOVE, collectionName, entityClass,
					null, queryObject);
			WriteConcern writeConcernToUse = prepareWriteConcern(mongoAction);
			MongoCollection<Document> collectionToUse = prepareCollection(collection, writeConcernToUse);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Remove using query: {} in collection: {}.",
						new Object[] { serializeToJsonSafely(dboq), collectionName });
			}

			return collectionToUse.deleteMany(dboq);
		}).doOnNext(deleteResult -> maybeEmitEvent(new AfterDeleteEvent<T>(queryObject, entityClass, collectionName)))
				.next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAll(java.lang.Class)
	 */
	public <T> Flux<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAll(java.lang.Class, java.lang.String)
	 */
	public <T> Flux<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(null), null,
				new ReadDocumentCallback<T>(mongoConverter, entityClass, collectionName), collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.String)
	 */
	@Override
	public <T> Flux<T> findAllAndRemove(Query query, String collectionName) {

		return findAllAndRemove(query, null, collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass) {
		return findAllAndRemove(query, entityClass, determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveMongoOperations#findAllAndRemove(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName) {
		return doFindAndDelete(collectionName, query, entityClass);
	}

	@Override
	public <T> Flux<T> tail(Query query, Class<T> entityClass) {
		return tail(query, entityClass, determineCollectionName(entityClass));
	}

	@Override
	public <T> Flux<T> tail(Query query, Class<T> entityClass, String collectionName) {

		if (query == null) {

			// TODO: clean up
			LOGGER.debug(String.format("find for class: %s in collection: %s", entityClass, collectionName));

			return executeFindMultiInternal(
					collection -> new FindCallback(null).doInCollection(collection).cursorType(CursorType.TailableAwait), null,
					new ReadDocumentCallback<T>(mongoConverter, entityClass, collectionName), collectionName);
		}

		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
				new TailingQueryFindPublisherPreparer(query, entityClass));
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
				.flatMap(list -> Flux.from(remove(getIdInQueryFor(list), entityClass, collectionName))
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
	 * @return the {@link List} of converted objects.
	 */
	protected <T> Mono<T> doFindOne(String collectionName, Document query, Document fields, Class<T> entityClass) {

		Optional<? extends MongoPersistentEntity<?>> entity = mappingContext.getPersistentEntity(entityClass);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);
		Document mappedFields = fields == null ? null : queryMapper.getMappedObject(fields, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("findOne using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(query), mappedFields, entityClass, collectionName));
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
	protected <T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass) {
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
	protected <T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass,
			FindPublisherPreparer preparer) {
		return doFind(collectionName, query, fields, entityClass, preparer,
				new ReadDocumentCallback<T>(mongoConverter, entityClass, collectionName));
	}

	protected <S, T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<S> entityClass,
			FindPublisherPreparer preparer, DocumentCallback<T> objectCallback) {

		Optional<? extends MongoPersistentEntity<?>> entity = mappingContext.getPersistentEntity(entityClass);

		Document mappedFields = queryMapper.getMappedFields(fields, entity);
		Document mappedQuery = queryMapper.getMappedObject(query, entity);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("find using query: %s fields: %s for class: %s in collection: %s",
					serializeToJsonSafely(mappedQuery), mappedFields, entityClass, collectionName));
		}

		return executeFindMultiInternal(new FindCallback(mappedQuery, mappedFields), preparer, objectCallback,
				collectionName);
	}

	protected CreateCollectionOptions convertToCreateCollectionOptions(CollectionOptions collectionOptions) {

		CreateCollectionOptions result = new CreateCollectionOptions();
		if (collectionOptions != null) {

			if (collectionOptions.getCapped() != null) {
				result = result.capped(collectionOptions.getCapped());
			}

			if (collectionOptions.getSize() != null) {
				result = result.sizeInBytes(collectionOptions.getSize());
			}

			if (collectionOptions.getMaxDocuments() != null) {
				result = result.maxDocuments(collectionOptions.getMaxDocuments());
			}
		}
		return result;
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
	protected <T> Mono<T> doFindAndRemove(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("findAndRemove using query: %s fields: %s sort: %s for class: %s in collection: %s",
					serializeToJsonSafely(query), fields, sort, entityClass, collectionName));
		}

		Optional<? extends MongoPersistentEntity<?>> entity = mappingContext.getPersistentEntity(entityClass);

		return executeFindOneInternal(new FindAndRemoveCallback(queryMapper.getMappedObject(query, entity), fields, sort),
				new ReadDocumentCallback<T>(this.mongoConverter, entityClass, collectionName), collectionName);
	}

	protected <T> Mono<T> doFindAndModify(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass, Update update, FindAndModifyOptions options) {

		FindAndModifyOptions optionsToUse;
		if (options == null) {
			optionsToUse = new FindAndModifyOptions();
		} else {
			optionsToUse = options;
		}

		Optional<? extends MongoPersistentEntity<?>> entity = mappingContext.getPersistentEntity(entityClass);

		return Mono.defer(() -> {

			increaseVersionForUpdateIfNecessary(entity.get(), update);

			Document mappedQuery = queryMapper.getMappedObject(query, entity);
			Document mappedUpdate = updateMapper.getMappedObject(update.getUpdateObject(), entity);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format(
						"findAndModify using query: %s fields: %s sort: %s for class: %s and update: %s " + "in collection: %s",
						serializeToJsonSafely(mappedQuery), fields, sort, entityClass, serializeToJsonSafely(mappedUpdate),
						collectionName));
			}

			return executeFindOneInternal(new FindAndModifyCallback(mappedQuery, fields, sort, mappedUpdate, optionsToUse),
					new ReadDocumentCallback<T>(this.mongoConverter, entityClass, collectionName), collectionName);
		});
	}

	protected <T> void maybeEmitEvent(MongoMappingEvent<T> event) {
		if (null != eventPublisher) {
			eventPublisher.publishEvent(event);
		}
	}

	/**
	 * Populates the id property of the saved object, if it's not set already.
	 *
	 * @param savedObject
	 * @param id
	 */
	private void populateIdIfNecessary(Object savedObject, Object id) {

		if (id == null) {
			return;
		}

		if (savedObject instanceof Document) {
			Document Document = (Document) savedObject;
			Document.put(ID_FIELD, id);
			return;
		}

		MongoPersistentProperty idProp = getIdPropertyFor(savedObject.getClass());

		if (idProp == null) {
			return;
		}

		ConversionService conversionService = mongoConverter.getConversionService();
		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(savedObject.getClass());
		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(savedObject);

		if (accessor.getProperty(idProp) != null) {
			return;
		}

		new ConvertingPropertyAccessor(accessor, conversionService).setProperty(idProp, Optional.ofNullable(id));
	}

	private MongoCollection<Document> getAndPrepareCollection(MongoDatabase db, String collectionName) {

		try {
			MongoCollection<Document> collection = db.getCollection(collectionName);
			return prepareCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	protected void ensureNotIterable(Object o) {
		if (null != o) {

			boolean isIterable = o.getClass().isArray();

			if (!isIterable) {
				for (Class iterableClass : ITERABLE_CLASSES) {
					if (iterableClass.isAssignableFrom(o.getClass()) || o.getClass().getName().equals(iterableClass.getName())) {
						isIterable = true;
						break;
					}
				}
			}

			if (isIterable) {
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
	 * The returned {@link WriteConcern} will be defaulted to {@link WriteConcern#ACKNOWLEDGED} when
	 * {@link WriteResultChecking} is set to {@link WriteResultChecking#EXCEPTION}.
	 *
	 * @param mongoAction any WriteConcern already configured or null
	 * @return The prepared WriteConcern or null
	 * @see #setWriteConcern(WriteConcern)
	 * @see #setWriteConcernResolver(WriteConcernResolver)
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
	 *          over it, may be {@literal null}
	 * @param objectCallback the {@link DocumentCallback} to transform {@link Document}s into the actual domain type, must
	 *          not be {@literal null}.
	 * @param collectionName the collection to be queried, must not be {@literal null}.
	 * @return
	 */
	private <T> Flux<T> executeFindMultiInternal(ReactiveCollectionQueryCallback<Document> collectionCallback,
			FindPublisherPreparer preparer, DocumentCallback<T> objectCallback, String collectionName) {

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
			MongoDatabase db = this.getMongoDatabase();
			return action.doInDatabase(db);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, exceptionTranslator);
		}
	}

	/**
	 * Exception translation {@link Function} intended for {@link Flux#onErrorResumeWith(Function)} usage.
	 *
	 * @return the exception translation {@link Function}
	 */
	private <T> Function<Throwable, Publisher<? extends T>> translateFluxException() {

		return throwable -> {

			if (throwable instanceof RuntimeException) {
				return Flux.error(potentiallyConvertRuntimeException((RuntimeException) throwable, exceptionTranslator));
			}

			return Flux.error(throwable);
		};
	}

	/**
	 * Exception translation {@link Function} intended for {@link Mono#otherwise(Function)} usage.
	 *
	 * @return the exception translation {@link Function}
	 */
	private <T> Function<Throwable, Mono<? extends T>> translateMonoException() {

		return throwable -> {

			if (throwable instanceof RuntimeException) {
				return Mono.error(potentiallyConvertRuntimeException((RuntimeException) throwable, exceptionTranslator));
			}

			return Mono.error(throwable);
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

	private MongoPersistentEntity<?> getPersistentEntity(Class<?> type) {
		return type == null ? null : mappingContext.getPersistentEntity(type).orElse(null);
	}

	private MongoPersistentProperty getIdPropertyFor(Class<?> type) {
		Optional<? extends MongoPersistentEntity<?>> persistentEntity = mappingContext.getPersistentEntity(type);
		return persistentEntity.isPresent() ? persistentEntity.get().getIdProperty().orElse(null) : null;
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

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
		return entity.getCollection();
	}

	private static MappingMongoConverter getDefaultMongoConverter() {

		MappingMongoConverter converter = new MappingMongoConverter(NO_OP_REF_RESOLVER, new MongoMappingContext());
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
	 * @param objectToSave
	 * @param writer
	 * @return
	 */
	private <T> Document toDbObject(T objectToSave, MongoWriter<T> writer) {

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
			} catch (JSONParseException | org.bson.json.JsonParseException e) {
				throw new MappingException("Could not parse given String to save into a JSON document!", e);
			}
		}
	}

	private void initializeVersionProperty(Object entity) {

		MongoPersistentEntity<?> mongoPersistentEntity = getPersistentEntity(entity.getClass());

		if (mongoPersistentEntity != null && mongoPersistentEntity.hasVersionProperty()) {
			ConvertingPropertyAccessor accessor = new ConvertingPropertyAccessor(
					mongoPersistentEntity.getPropertyAccessor(entity), mongoConverter.getConversionService());
			accessor.setProperty(mongoPersistentEntity.getVersionProperty().get(), Optional.of(0));
		}
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
		private final Document fields;

		FindOneCallback(Document query, Document fields) {
			this.query = query;
			this.fields = fields;
		}

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			if (fields == null) {

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("findOne using query: %s in db.collection: %s", serializeToJsonSafely(query),
							collection.getNamespace().getFullName()));
				}

				return collection.find(query).limit(1).first();
			} else {

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("findOne using query: %s fields: %s in db.collection: %s",
							serializeToJsonSafely(query), fields, collection.getNamespace().getFullName()));
				}

				return collection.find(query).projection(fields).limit(1);
			}
		}
	}

	/**
	 * Simple {@link ReactiveCollectionQueryCallback} that takes a query {@link Document} plus an optional fields
	 * specification {@link Document} and executes that against the {@link MongoCollection}.
	 *
	 * @author Mark Paluch
	 */
	private static class FindCallback implements ReactiveCollectionQueryCallback<Document> {

		private final Document query;
		private final Document fields;

		FindCallback(Document query) {
			this(query, null);
		}

		FindCallback(Document query, Document fields) {
			this.query = query;
			this.fields = fields;
		}

		@Override
		public FindPublisher<Document> doInCollection(MongoCollection<Document> collection) {

			FindPublisher<Document> findPublisher;
			if (query == null || query.isEmpty()) {
				findPublisher = collection.find();
			} else {
				findPublisher = collection.find(query);
			}

			if (fields == null || fields.isEmpty()) {
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

		FindAndRemoveCallback(Document query, Document fields, Document sort) {

			this.query = query;
			this.fields = fields;
			this.sort = sort;
		}

		@Override
		public Publisher<Document> doInCollection(MongoCollection<Document> collection)
				throws MongoException, DataAccessException {

			FindOneAndDeleteOptions findOneAndDeleteOptions = convertToFindOneAndDeleteOptions(fields, sort);
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
	private class ReadDocumentCallback<T> implements DocumentCallback<T> {

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
		 * Creates a new {@link GeoNearResultDbObjectCallback} using the given {@link DbObjectCallback} delegate for
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

			return new GeoResult<T>(doWith, new Distance(distance, metric));
		}
	}

	/**
	 * @author Mark Paluch
	 */
	class QueryFindPublisherPreparer implements FindPublisherPreparer {

		private final Query query;
		private final Class<?> type;

		QueryFindPublisherPreparer(Query query, Class<?> type) {

			this.query = query;
			this.type = type;
		}

		public <T> FindPublisher<T> prepare(FindPublisher<T> findPublisher) {

			if (query == null) {
				return findPublisher;
			}

			if (query.getSkip() <= 0 && query.getLimit() <= 0 && query.getSortObject() == null
					&& !StringUtils.hasText(query.getHint()) && !query.getMeta().hasValues()) {
				return findPublisher;
			}

			FindPublisher<T> findPublisherToUse = findPublisher;

			try {
				if (query.getSkip() > 0) {
					findPublisherToUse = findPublisherToUse.skip((int)query.getSkip());
				}
				if (query.getLimit() > 0) {
					findPublisherToUse = findPublisherToUse.limit(query.getLimit());
				}
				if (query.getSortObject() != null) {
					Document sort = type != null ? getMappedSortObject(query, type) : query.getSortObject();
					findPublisherToUse = findPublisherToUse.sort(sort);
				}
				BasicDBObject modifiers = new BasicDBObject();

				if (StringUtils.hasText(query.getHint())) {
					modifiers.append("$hint", query.getHint());
				}

				if (query.getMeta().hasValues()) {
					for (Entry<String, Object> entry : query.getMeta().values()) {
						modifiers.append(entry.getKey(), entry.getValue());
					}
				}

				if (!modifiers.isEmpty()) {
					findPublisherToUse = findPublisherToUse.modifiers(modifiers);
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
	 * No-Operation {@link org.springframework.data.mongodb.core.mapping.DBRef} resolver.
	 *
	 * @author Mark Paluch
	 */
	static class NoOpDbRefResolver implements DbRefResolver {

		@Override
		public Optional<Object> resolveDbRef(MongoPersistentProperty property, DBRef dbref, DbRefResolverCallback callback,
				DbRefProxyHandler proxyHandler) {
			return Optional.empty();
		}

		@Override
		public DBRef createDbRef(org.springframework.data.mongodb.core.mapping.DBRef annotation,
				MongoPersistentEntity<?> entity, Object id) {
			return null;
		}

		@Override
		public Document fetch(DBRef dbRef) {
			return null;
		}

		@Override
		public List<Document> bulkFetch(List<DBRef> dbRefs) {
			return null;
		}
	}
}
