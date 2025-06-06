/*
 * Copyright 2016-2025 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadPreference;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * Interface that specifies a basic set of MongoDB operations executed in a reactive way.
 * <p>
 * Implemented by {@link ReactiveMongoTemplate}. Not often used but a useful option for extensibility and testability
 * (as it can be easily mocked, stubbed, or be the target of a JDK proxy). Command execution using
 * {@link ReactiveMongoOperations} is deferred until subscriber subscribes to the {@link Publisher}. <br />
 * <strong>NOTE:</strong> Some operations cannot be executed within a MongoDB transaction. Please refer to the MongoDB
 * specific documentation to learn more about <a href="https://docs.mongodb.com/manual/core/transactions/">Multi
 * Document Transactions</a>.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 * @since 2.0
 * @see Flux
 * @see Mono
 * @see <a href="https://projectreactor.io/docs/">Project Reactor</a>
 */
public interface ReactiveMongoOperations extends ReactiveFluentMongoOperations {

	/**
	 * Returns the reactive operations that can be performed on indexes
	 *
	 * @param collectionName must not be {@literal null}.
	 * @return index operations on the named collection
	 */
	ReactiveIndexOperations indexOps(String collectionName);

	/**
	 * Returns the reactive operations that can be performed on indexes
	 *
	 * @param entityClass must not be {@literal null}.
	 * @return index operations on the named collection associated with the given entity class
	 */
	ReactiveIndexOperations indexOps(Class<?> entityClass);

	/**
	 * Execute a MongoDB command expressed as a JSON string. This will call the method JSON.parse that is part of the
	 * MongoDB driver to convert the JSON string to a Document. Any errors that result from executing this command will be
	 * converted into Spring's DAO exception hierarchy.
	 *
	 * @param jsonCommand a MongoDB command expressed as a JSON string.
	 * @return a result object returned by the action
	 */
	Mono<Document> executeCommand(String jsonCommand);

	/**
	 * Execute a MongoDB command. Any errors that result from executing this command will be converted into Spring's DAO
	 * exception hierarchy.
	 *
	 * @param command a MongoDB command.
	 * @return a result object returned by the action
	 */
	Mono<Document> executeCommand(Document command);

	/**
	 * Execute a MongoDB command. Any errors that result from executing this command will be converted into Spring's data
	 * access exception hierarchy.
	 *
	 * @param command a MongoDB command, must not be {@literal null}.
	 * @param readPreference read preferences to use, can be {@literal null}.
	 * @return a result object returned by the action.
	 */
	Mono<Document> executeCommand(Document command, @Nullable ReadPreference readPreference);

	/**
	 * Executes a {@link ReactiveDatabaseCallback} translating any exceptions as necessary. <br />
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 *
	 * @param action callback object that specifies the MongoDB actions to perform on the passed in DB instance. Must not
	 *          be {@literal null}.
	 * @param <T> return type.
	 * @return a result object returned by the action
	 */
	<T> Flux<T> execute(ReactiveDatabaseCallback<T> action);

	/**
	 * Executes the given {@link ReactiveCollectionCallback} on the entity collection of the specified class. <br />
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 *
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @param action callback object that specifies the MongoDB action. Must not be {@literal null}.
	 * @param <T> return type.
	 * @return a result object returned by the action or {@literal null}.
	 */
	<T> Flux<T> execute(Class<?> entityClass, ReactiveCollectionCallback<T> action);

	/**
	 * Executes the given {@link ReactiveCollectionCallback} on the collection of the given name. <br />
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 *
	 * @param collectionName the name of the collection that specifies which {@link MongoCollection} instance will be
	 *          passed into. Must not be {@literal null} or empty.
	 * @param action callback object that specifies the MongoDB action the callback action. Must not be {@literal null}.
	 * @param <T> return type.
	 * @return a result object returned by the action or {@literal null}.
	 */
	<T> Flux<T> execute(String collectionName, ReactiveCollectionCallback<T> action);

	/**
	 * Obtain a {@link ClientSession session} bound instance of {@link SessionScoped} binding the {@link ClientSession}
	 * provided by the given {@link Supplier} to each and every command issued against MongoDB. <br />
	 * <strong>Note:</strong> It is up to the caller to manage the {@link ClientSession} lifecycle. Use
	 * {@link ReactiveSessionScoped#execute(ReactiveSessionCallback, Consumer)} to provide a hook for processing the
	 * {@link ClientSession} when done.
	 *
	 * @param sessionProvider must not be {@literal null}.
	 * @return new instance of {@link ReactiveSessionScoped}. Never {@literal null}.
	 * @since 2.1
	 */
	default ReactiveSessionScoped withSession(Supplier<ClientSession> sessionProvider) {

		Assert.notNull(sessionProvider, "SessionProvider must not be null");

		return withSession(Mono.fromSupplier(sessionProvider));
	}

	/**
	 * Obtain a {@link ClientSession session} bound instance of {@link SessionScoped} binding a new {@link ClientSession}
	 * with given {@literal sessionOptions} to each and every command issued against MongoDB. <br />
	 * <strong>Note:</strong> It is up to the caller to manage the {@link ClientSession} lifecycle. Use
	 * {@link ReactiveSessionScoped#execute(ReactiveSessionCallback, Consumer)} to provide a hook for processing the
	 * {@link ClientSession} when done.
	 *
	 * @param sessionOptions must not be {@literal null}.
	 * @return new instance of {@link ReactiveSessionScoped}. Never {@literal null}.
	 * @since 2.1
	 */
	ReactiveSessionScoped withSession(ClientSessionOptions sessionOptions);

	/**
	 * Obtain a {@link ClientSession session} bound instance of {@link ReactiveSessionScoped} binding the
	 * {@link ClientSession} provided by the given {@link Publisher} to each and every command issued against MongoDB.
	 * <br />
	 * <strong>Note:</strong> It is up to the caller to manage the {@link ClientSession} lifecycle. Use
	 * {@link ReactiveSessionScoped#execute(ReactiveSessionCallback, Consumer)} to provide a hook for processing the
	 * {@link ClientSession} when done.
	 *
	 * @param sessionProvider must not be {@literal null}.
	 * @return new instance of {@link ReactiveSessionScoped}. Never {@literal null}.
	 * @since 2.1
	 */
	ReactiveSessionScoped withSession(Publisher<ClientSession> sessionProvider);

	/**
	 * Obtain a {@link ClientSession} bound instance of {@link ReactiveMongoOperations}. <br />
	 * <strong>Note:</strong> It is up to the caller to manage the {@link ClientSession} lifecycle.
	 *
	 * @return {@link ClientSession} bound instance of {@link ReactiveMongoOperations}.
	 * @since 2.1
	 */
	ReactiveMongoOperations withSession(ClientSession session);

	/**
	 * Create an uncapped collection with a name based on the provided entity class.
	 * <p>
	 * This method derives {@link CollectionOptions} from the given {@code entityClass} using
	 * {@link org.springframework.data.mongodb.core.mapping.Document} and
	 * {@link org.springframework.data.mongodb.core.mapping.TimeSeries} annotations to determine:
	 * <ul>
	 * <li>Collation</li>
	 * <li>TimeSeries time and meta fields, granularity and {@code expireAfter}</li>
	 * </ul>
	 * Any other options such as change stream options, schema-based details (validation, encryption) are not considered
	 * and must be provided through {@link #createCollection(Class, Function)} or
	 * {@link #createCollection(Class, CollectionOptions)}.
	 *
	 * @param entityClass class that determines the collection to create.
	 * @return the created collection.
	 * @see #createCollection(Class, Function)
	 * @see #createCollection(Class, CollectionOptions)
	 */
	<T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass);

	/**
	 * Create an uncapped collection with a name based on the provided entity class allowing to customize derived
	 * {@link CollectionOptions}.
	 * <p>
	 * This method derives {@link CollectionOptions} from the given {@code entityClass} using
	 * {@link org.springframework.data.mongodb.core.mapping.Document} and
	 * {@link org.springframework.data.mongodb.core.mapping.TimeSeries} annotations to determine:
	 * <ul>
	 * <li>Collation</li>
	 * <li>TimeSeries time and meta fields, granularity and {@code expireAfter}</li>
	 * </ul>
	 * Any other options such as change stream options, schema-based details (validation, encryption) are not considered
	 * and must be provided through {@link CollectionOptions}.
	 *
	 * @param entityClass class that determines the collection to create.
	 * @param collectionOptionsCustomizer customizer function to customize the derived {@link CollectionOptions}.
	 * @return the created collection.
	 * @see #createCollection(Class, CollectionOptions)
	 * @since 5.0
	 */
	<T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass,
			Function<? super CollectionOptions, ? extends CollectionOptions> collectionOptionsCustomizer);

	/**
	 * Create a collection with a name based on the provided entity class using the options.
	 *
	 * @param entityClass class that determines the collection to create. Must not be {@literal null}.
	 * @param collectionOptions options to use when creating the collection.
	 * @return the created collection.
	 */
	<T> Mono<MongoCollection<Document>> createCollection(Class<T> entityClass,
			@Nullable CollectionOptions collectionOptions);

	/**
	 * Create an uncapped collection with the provided name.
	 *
	 * @param collectionName name of the collection.
	 * @return the created collection.
	 */
	Mono<MongoCollection<Document>> createCollection(String collectionName);

	/**
	 * Create a collection with the provided name and options.
	 *
	 * @param collectionName name of the collection. Must not be {@literal null} nor empty.
	 * @param collectionOptions options to use when creating the collection.
	 * @return the created collection.
	 */
	Mono<MongoCollection<Document>> createCollection(String collectionName, CollectionOptions collectionOptions);

	/**
	 * Create a view with the provided name. The view content is defined by the {@link AggregationOperation pipeline
	 * stages} on another collection or view identified by the given {@link #getCollectionName(Class) source type}.
	 *
	 * @param name the name of the view to create.
	 * @param source the type defining the views source collection.
	 * @param stages the {@link AggregationOperation aggregation pipeline stages} defining the view content.
	 * @since 4.0
	 */
	default Mono<MongoCollection<Document>> createView(String name, Class<?> source, AggregationOperation... stages) {
		return createView(name, source, AggregationPipeline.of(stages));
	}

	/**
	 * Create a view with the provided name. The view content is defined by the {@link AggregationPipeline pipeline} on
	 * another collection or view identified by the given {@link #getCollectionName(Class) source type}.
	 *
	 * @param name the name of the view to create.
	 * @param source the type defining the views source collection.
	 * @param pipeline the {@link AggregationPipeline} defining the view content.
	 * @since 4.0
	 */
	default Mono<MongoCollection<Document>> createView(String name, Class<?> source, AggregationPipeline pipeline) {
		return createView(name, source, pipeline, null);
	}

	/**
	 * Create a view with the provided name. The view content is defined by the {@link AggregationPipeline pipeline} on
	 * another collection or view identified by the given {@link #getCollectionName(Class) source type}.
	 *
	 * @param name the name of the view to create.
	 * @param source the type defining the views source collection.
	 * @param pipeline the {@link AggregationPipeline} defining the view content.
	 * @param options additional settings to apply when creating the view. Can be {@literal null}.
	 * @since 4.0
	 */
	Mono<MongoCollection<Document>> createView(String name, Class<?> source, AggregationPipeline pipeline,
			@Nullable ViewOptions options);

	/**
	 * Create a view with the provided name. The view content is defined by the {@link AggregationPipeline pipeline} on
	 * another collection or view identified by the given source.
	 *
	 * @param name the name of the view to create.
	 * @param source the name of the collection or view defining the to be created views source.
	 * @param pipeline the {@link AggregationPipeline} defining the view content.
	 * @param options additional settings to apply when creating the view. Can be {@literal null}.
	 * @since 4.0
	 */
	Mono<MongoCollection<Document>> createView(String name, String source, AggregationPipeline pipeline,
			@Nullable ViewOptions options);

	/**
	 * A set of collection names.
	 *
	 * @return Flux of collection names.
	 */
	Flux<String> getCollectionNames();

	/**
	 * Get a {@link MongoCollection} by name. The returned collection may not exists yet (except in local memory) and is
	 * created on first interaction with the server. Collections can be explicitly created via
	 * {@link #createCollection(Class)}. Please make sure to check if the collection {@link #collectionExists(Class)
	 * exists} first. <br />
	 * Translate any exceptions as necessary.
	 *
	 * @param collectionName name of the collection.
	 * @return an existing collection or one created on first server interaction.
	 */
	Mono<MongoCollection<Document>> getCollection(String collectionName);

	/**
	 * Check to see if a collection with a name indicated by the entity class exists. <br />
	 * Translate any exceptions as necessary.
	 *
	 * @param entityClass class that determines the name of the collection. Must not be {@literal null}.
	 * @return true if a collection with the given name is found, false otherwise.
	 */
	<T> Mono<Boolean> collectionExists(Class<T> entityClass);

	/**
	 * Check to see if a collection with a given name exists. <br />
	 * Translate any exceptions as necessary.
	 *
	 * @param collectionName name of the collection. Must not be {@literal null}.
	 * @return true if a collection with the given name is found, false otherwise.
	 */
	Mono<Boolean> collectionExists(String collectionName);

	/**
	 * Drop the collection with the name indicated by the entity class. <br />
	 * Translate any exceptions as necessary.
	 *
	 * @param entityClass class that determines the collection to drop/delete. Must not be {@literal null}.
	 */
	<T> Mono<Void> dropCollection(Class<T> entityClass);

	/**
	 * Drop the collection with the given name. <br />
	 * Translate any exceptions as necessary.
	 *
	 * @param collectionName name of the collection to drop/delete.
	 */
	Mono<Void> dropCollection(String collectionName);

	/**
	 * Returns a new {@link ReactiveBulkOperations} for the given collection. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping, etc. is not available for {@literal update} or
	 * {@literal remove} operations in bulk mode due to the lack of domain type information. Use
	 * {@link #bulkOps(BulkMode, Class, String)} to get full type specific support.
	 *
	 * @param mode the {@link BulkMode} to use for bulk operations, must not be {@literal null}.
	 * @param collectionName the name of the collection to work on, must not be {@literal null} or empty.
	 * @return {@link ReactiveBulkOperations} on the named collection
	 * @since 4.1
	 */
	ReactiveBulkOperations bulkOps(BulkMode mode, String collectionName);

	/**
	 * Returns a new {@link ReactiveBulkOperations} for the given entity type.
	 *
	 * @param mode the {@link BulkMode} to use for bulk operations, must not be {@literal null}.
	 * @param entityClass the name of the entity class, must not be {@literal null}.
	 * @return {@link ReactiveBulkOperations} on the named collection associated of the given entity class.
	 * @since 4.1
	 */
	ReactiveBulkOperations bulkOps(BulkMode mode, Class<?> entityClass);

	/**
	 * Returns a new {@link ReactiveBulkOperations} for the given entity type and collection name.
	 *
	 * @param mode the {@link BulkMode} to use for bulk operations, must not be {@literal null}.
	 * @param entityType the name of the entity class. Can be {@literal null}.
	 * @param collectionName the name of the collection to work on, must not be {@literal null} or empty.
	 * @return {@link ReactiveBulkOperations} on the named collection associated with the given entity class.
	 * @since 4.1
	 */
	ReactiveBulkOperations bulkOps(BulkMode mode, @Nullable Class<?> entityType, String collectionName);

	/**
	 * Query for a {@link Flux} of objects of type T from the collection used by the entity class. <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient way
	 * to map objects since the test for class type is done in the client and not on the server.
	 *
	 * @param entityClass the parametrized type of the returned {@link Flux}.
	 * @return the converted collection.
	 */
	<T> Flux<T> findAll(Class<T> entityClass);

	/**
	 * Query for a {@link Flux} of objects of type T from the specified collection. <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient way
	 * to map objects since the test for class type is done in the client and not on the server.
	 *
	 * @param entityClass the parametrized type of the returned {@link Flux}.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @return the converted collection.
	 */
	<T> Flux<T> findAll(Class<T> entityClass, String collectionName);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity class to a single instance of an object of the
	 * specified type. <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned {@link Mono}.
	 * @return the converted object.
	 */
	<T> Mono<T> findOne(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a single instance of an object of the specified
	 * type. <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned {@link Mono}.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @return the converted object.
	 */
	<T> Mono<T> findOne(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Determine result of given {@link Query} contains at least one element. <br />
	 * <strong>NOTE:</strong> Any additional support for query/field mapping, etc. is not available due to the lack of
	 * domain type information. Use {@link #exists(Query, Class, String)} to get full type specific support.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find a document.
	 * @param collectionName name of the collection to check for objects.
	 * @return {@literal true} if the query yields a result.
	 */
	Mono<Boolean> exists(Query query, String collectionName);

	/**
	 * Determine result of given {@link Query} contains at least one element.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find a document.
	 * @param entityClass the parametrized type.
	 * @return {@literal true} if the query yields a result.
	 */
	Mono<Boolean> exists(Query query, Class<?> entityClass);

	/**
	 * Determine result of given {@link Query} contains at least one element.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find a document.
	 * @param entityClass the parametrized type. Can be {@literal null}.
	 * @param collectionName name of the collection to check for objects.
	 * @return {@literal true} if the query yields a result.
	 */
	Mono<Boolean> exists(Query query, @Nullable Class<?> entityClass, String collectionName);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity class to a {@link Flux} of the specified type.
	 * <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param entityClass the parametrized type of the returned {@link Flux}. Must not be {@literal null}.
	 * @return the {@link Flux} of converted objects.
	 */
	<T> Flux<T> find(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a {@link Flux} of the specified type. <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param entityClass the parametrized type of the returned {@link Flux}.
	 * @param collectionName name of the collection to retrieve the objects from. Must not be {@literal null}.
	 * @return the {@link Flux} of converted objects.
	 */
	<T> Flux<T> find(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Query for a scroll of objects of type T from the specified collection. <br />
	 * Make sure to either set {@link Query#skip(long)} or {@link Query#with(KeysetScrollPosition)} along with
	 * {@link Query#limit(int)} to limit large query results for efficient scrolling. <br />
	 * Result objects are converted from the MongoDB native representation using an instance of {@see MongoConverter}.
	 * Unless configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient way
	 * to map objects since the test for class type is done in the client and not on the server.
	 * <p>
	 * When using {@link KeysetScrollPosition}, make sure to use non-nullable {@link org.springframework.data.domain.Sort
	 * sort properties} as MongoDB does not support criteria to reconstruct a query result from absent document fields or
	 * {@literal null} values through {@code $gt/$lt} operators.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param entityType the parametrized type of the returned list.
	 * @return {@link Mono} emitting the converted window.
	 * @throws IllegalStateException if a potential {@link Query#getKeyset() KeysetScrollPosition} contains an invalid
	 *           position.
	 * @since 4.1
	 * @see Query#with(org.springframework.data.domain.OffsetScrollPosition)
	 * @see Query#with(org.springframework.data.domain.KeysetScrollPosition)
	 */
	<T> Mono<Window<T>> scroll(Query query, Class<T> entityType);

	/**
	 * Query for a window of objects of type T from the specified collection. <br />
	 * Make sure to either set {@link Query#skip(long)} or {@link Query#with(KeysetScrollPosition)} along with
	 * {@link Query#limit(int)} to limit large query results for efficient scrolling. <br />
	 * Result objects are converted from the MongoDB native representation using an instance of {@see MongoConverter}.
	 * Unless configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient way
	 * to map objects since the test for class type is done in the client and not on the server.
	 * <p>
	 * When using {@link KeysetScrollPosition}, make sure to use non-nullable {@link org.springframework.data.domain.Sort
	 * sort properties} as MongoDB does not support criteria to reconstruct a query result from absent document fields or
	 * {@literal null} values through {@code $gt/$lt} operators.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param entityType the parametrized type of the returned list.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @return {@link Mono} emitting the converted window.
	 * @throws IllegalStateException if a potential {@link Query#getKeyset() KeysetScrollPosition} contains an invalid
	 *           position.
	 * @since 4.1
	 * @see Query#with(org.springframework.data.domain.OffsetScrollPosition)
	 * @see Query#with(org.springframework.data.domain.KeysetScrollPosition)
	 */
	<T> Mono<Window<T>> scroll(Query query, Class<T> entityType, String collectionName);

	/**
	 * Returns a document with the given id mapped onto the given class. The collection the query is ran against will be
	 * derived from the given target class as well.
	 *
	 * @param id the id of the document to return. Must not be {@literal null}.
	 * @param entityClass the type the document shall be converted into. Must not be {@literal null}.
	 * @return the document with the given id mapped onto the given target class.
	 */
	<T> Mono<T> findById(Object id, Class<T> entityClass);

	/**
	 * Returns the document with the given id from the given collection mapped onto the given target class.
	 *
	 * @param id the id of the document to return.
	 * @param entityClass the type to convert the document to.
	 * @param collectionName the collection to query for the document.
	 * @return the converted object.
	 */
	<T> Mono<T> findById(Object id, Class<T> entityClass, String collectionName);

	/**
	 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view and
	 * returns the results in a {@link Flux}.
	 *
	 * @param field the name of the field to inspect for distinct values. Must not be {@literal null}.
	 * @param entityClass the domain type used for determining the actual {@link MongoCollection}. Must not be
	 *          {@literal null}.
	 * @param resultClass the result type. Must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	default <T> Flux<T> findDistinct(String field, Class<?> entityClass, Class<T> resultClass) {
		return findDistinct(new Query(), field, entityClass, resultClass);
	}

	/**
	 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view and
	 * returns the results in a {@link Flux}.
	 *
	 * @param query filter {@link Query} to restrict search. Must not be {@literal null}.
	 * @param field the name of the field to inspect for distinct values. Must not be {@literal null}.
	 * @param entityClass the domain type used for determining the actual {@link MongoCollection} and mapping the
	 *          {@link Query} to the domain type fields. Must not be {@literal null}.
	 * @param resultClass the result type. Must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	<T> Flux<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass);

	/**
	 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view and
	 * returns the results in a {@link Flux}.
	 *
	 * @param query filter {@link Query} to restrict search. Must not be {@literal null}.
	 * @param field the name of the field to inspect for distinct values. Must not be {@literal null}.
	 * @param collectionName the explicit name of the actual {@link MongoCollection}. Must not be {@literal null}.
	 * @param entityClass the domain type used for mapping the {@link Query} to the domain type fields.
	 * @param resultClass the result type. Must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	<T> Flux<T> findDistinct(Query query, String field, String collectionName, Class<?> entityClass,
			Class<T> resultClass);

	/**
	 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view and
	 * returns the results in a {@link Flux}.
	 *
	 * @param query filter {@link Query} to restrict search. Must not be {@literal null}.
	 * @param field the name of the field to inspect for distinct values. Must not be {@literal null}.
	 * @param collection the explicit name of the actual {@link MongoCollection}. Must not be {@literal null}.
	 * @param resultClass the result type. Must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	default <T> Flux<T> findDistinct(Query query, String field, String collection, Class<T> resultClass) {
		return findDistinct(query, field, collection, Object.class, resultClass);
	}

	/**
	 * Execute an aggregation operation.
	 * <p>
	 * The raw results will be mapped to the given entity class.
	 * <p>
	 * Aggregation streaming cannot be used with {@link AggregationOptions#isExplain() aggregation explain} nor with
	 * {@link AggregationOptions#getCursorBatchSize()}. Enabling explanation mode or setting batch size cause
	 * {@link IllegalArgumentException}.
	 *
	 * @param aggregation The {@link TypedAggregation} specification holding the aggregation operations. Must not be
	 *          {@literal null}.
	 * @param collectionName The name of the input collection to use for the aggregation. Must not be {@literal null}.
	 * @param outputType The parametrized type of the returned {@link Flux}. Must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @throws IllegalArgumentException if {@code aggregation}, {@code collectionName} or {@code outputType} is
	 *           {@literal null}.
	 */
	<O> Flux<O> aggregate(TypedAggregation<?> aggregation, String collectionName, Class<O> outputType);

	/**
	 * Execute an aggregation operation. <br />
	 * The raw results will be mapped to the given entity class and are returned as stream. The name of the
	 * inputCollection is derived from the {@link TypedAggregation#getInputType() aggregation input type}. <br />
	 * Aggregation streaming cannot be used with {@link AggregationOptions#isExplain() aggregation explain} nor with
	 * {@link AggregationOptions#getCursorBatchSize()}. Enabling explanation mode or setting batch size cause
	 * {@link IllegalArgumentException}.
	 *
	 * @param aggregation The {@link TypedAggregation} specification holding the aggregation operations. Must not be
	 *          {@literal null}.
	 * @param outputType The parametrized type of the returned {@link Flux}. Must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @throws IllegalArgumentException if {@code aggregation} or {@code outputType} is {@literal null}.
	 */
	<O> Flux<O> aggregate(TypedAggregation<?> aggregation, Class<O> outputType);

	/**
	 * Execute an aggregation operation. <br />
	 * The raw results will be mapped to the given {@code ouputType}. The name of the inputCollection is derived from the
	 * {@code inputType}. <br />
	 * Aggregation streaming cannot be used with {@link AggregationOptions#isExplain() aggregation explain} nor with
	 * {@link AggregationOptions#getCursorBatchSize()}. Enabling explanation mode or setting batch size cause
	 * {@link IllegalArgumentException}.
	 *
	 * @param aggregation The {@link Aggregation} specification holding the aggregation operations. Must not be
	 *          {@literal null}.
	 * @param inputType the inputType where the aggregation operation will read from. Must not be {@literal null}.
	 * @param outputType The parametrized type of the returned {@link Flux}. Must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @throws IllegalArgumentException if {@code aggregation}, {@code inputType} or {@code outputType} is
	 *           {@literal null}.
	 */
	<O> Flux<O> aggregate(Aggregation aggregation, Class<?> inputType, Class<O> outputType);

	/**
	 * Execute an aggregation operation. <br />
	 * The raw results will be mapped to the given entity class. <br />
	 * Aggregation streaming cannot be used with {@link AggregationOptions#isExplain() aggregation explain} nor with
	 * {@link AggregationOptions#getCursorBatchSize()}. Enabling explanation mode or setting batch size cause
	 * {@link IllegalArgumentException}.
	 *
	 * @param aggregation The {@link Aggregation} specification holding the aggregation operations. Must not be
	 *          {@literal null}.
	 * @param collectionName the collection where the aggregation operation will read from. Must not be {@literal null} or
	 *          empty.
	 * @param outputType The parametrized type of the returned {@link Flux}. Must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @throws IllegalArgumentException if {@code aggregation}, {@code collectionName} or {@code outputType} is
	 *           {@literal null}.
	 */
	<O> Flux<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType);

	/**
	 * Returns {@link Flux} of {@link GeoResult} for all entities matching the given {@link NearQuery}. Will consider
	 * entity mapping information to determine the collection the query is ran against. Note, that MongoDB limits the
	 * number of results by default. Make sure to add an explicit limit to the {@link NearQuery} if you expect a
	 * particular number of results.
	 * <p>
	 * MongoDB 4.2 has removed the {@code geoNear} command. This method uses since version 2.2 aggregations and the
	 * {@code $geoNear} aggregation command to emulate {@code geoNear} command functionality. We recommend using
	 * aggregations directly:
	 * </p>
	 *
	 * <pre class="code">
	 * TypedAggregation&lt;T&gt; geoNear = TypedAggregation.newAggregation(entityClass, Aggregation.geoNear(near, "dis"))
	 * 		.withOptions(AggregationOptions.builder().collation(near.getCollation()).build());
	 * Flux&lt;Document&gt; results = aggregate(geoNear, Document.class);
	 * </pre>
	 *
	 * @param near must not be {@literal null}.
	 * @param entityClass must not be {@literal null}.
	 * @return the converted {@link GeoResult}s.
	 * @deprecated since 2.2. The {@code eval} command has been removed in MongoDB Server 4.2.0. Use Aggregations with
	 *             {@link Aggregation#geoNear(NearQuery, String)} instead.
	 */
	@Deprecated
	<T> Flux<GeoResult<T>> geoNear(NearQuery near, Class<T> entityClass);

	/**
	 * Returns {@link Flux} of {@link GeoResult} for all entities matching the given {@link NearQuery}. Note, that MongoDB
	 * limits the number of results by default. Make sure to add an explicit limit to the {@link NearQuery} if you expect
	 * a particular number of results.
	 * <p>
	 * MongoDB 4.2 has removed the {@code geoNear} command. This method uses since version 2.2 aggregations and the
	 * {@code $geoNear} aggregation command to emulate {@code geoNear} command functionality. We recommend using
	 * aggregations directly:
	 * </p>
	 *
	 * <pre class="code">
	 * TypedAggregation&lt;T&gt; geoNear = TypedAggregation.newAggregation(entityClass, Aggregation.geoNear(near, "dis"))
	 * 		.withOptions(AggregationOptions.builder().collation(near.getCollation()).build());
	 * Flux&lt;Document&gt; results = aggregate(geoNear, Document.class);
	 * </pre>
	 *
	 * @param near must not be {@literal null}.
	 * @param entityClass must not be {@literal null}.
	 * @param collectionName the collection to trigger the query against. If no collection name is given the entity class
	 *          will be inspected.
	 * @return the converted {@link GeoResult}s.
	 * @deprecated since 2.2. The {@code eval} command has been removed in MongoDB Server 4.2.0. Use Aggregations with
	 *             {@link Aggregation#geoNear(NearQuery, String)} instead.
	 */
	@Deprecated
	<T> Flux<GeoResult<T>> geoNear(NearQuery near, Class<T> entityClass, String collectionName);

	/**
	 * Triggers <a href="https://docs.mongodb.org/manual/reference/method/db.collection.findAndModify/">findAndModify</a>
	 * to apply provided {@link Update} on documents matching {@link Criteria} of given {@link Query}.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} to apply on matching documents. Must not be {@literal null}.
	 * @param entityClass the parametrized type. Must not be {@literal null}.
	 * @return the converted object that was updated before it was updated.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	<T> Mono<T> findAndModify(Query query, UpdateDefinition update, Class<T> entityClass);

	/**
	 * Triggers <a href="https://docs.mongodb.org/manual/reference/method/db.collection.findAndModify/">findAndModify</a>
	 * to apply provided {@link Update} on documents matching {@link Criteria} of given {@link Query}.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} to apply on matching documents. Must not be {@literal null}.
	 * @param entityClass the parametrized type. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the converted object that was updated before it was updated.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	<T> Mono<T> findAndModify(Query query, UpdateDefinition update, Class<T> entityClass, String collectionName);

	/**
	 * Triggers <a href="https://docs.mongodb.org/manual/reference/method/db.collection.findAndModify/">findAndModify</a>
	 * to apply provided {@link Update} on documents matching {@link Criteria} of given {@link Query} taking
	 * {@link FindAndModifyOptions} into account.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification.
	 * @param update the {@link UpdateDefinition} to apply on matching documents.
	 * @param options the {@link FindAndModifyOptions} holding additional information.
	 * @param entityClass the parametrized type.
	 * @return the converted object that was updated. Depending on the value of {@link FindAndModifyOptions#isReturnNew()}
	 *         this will either be the object as it was before the update or as it is after the update.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	<T> Mono<T> findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass);

	/**
	 * Triggers <a href="https://docs.mongodb.org/manual/reference/method/db.collection.findAndModify/">findAndModify</a>
	 * to apply provided {@link Update} on documents matching {@link Criteria} of given {@link Query} taking
	 * {@link FindAndModifyOptions} into account.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} to apply on matching documents. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @param entityClass the parametrized type. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the converted object that was updated. Depending on the value of {@link FindAndModifyOptions#isReturnNew()}
	 *         this will either be the object as it was before the update or as it is after the update.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	<T> Mono<T> findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass,
			String collectionName);

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace</a>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement}
	 * document. <br />
	 * Options are defaulted to {@link FindAndReplaceOptions#empty()}. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @return the converted object that was updated or {@link Mono#empty()}, if not found.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given replacement value.
	 * @since 2.1
	 */
	default <T> Mono<T> findAndReplace(Query query, T replacement) {
		return findAndReplace(query, replacement, FindAndReplaceOptions.empty());
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace</a>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement}
	 * document. <br />
	 * Options are defaulted to {@link FindAndReplaceOptions#empty()}. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the converted object that was updated or {@link Mono#empty()}, if not found.
	 * @since 2.1
	 */
	default <T> Mono<T> findAndReplace(Query query, T replacement, String collectionName) {
		return findAndReplace(query, replacement, FindAndReplaceOptions.empty(), collectionName);
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace</a>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @return the converted object that was updated or {@link Mono#empty()}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given replacement value.
	 * @since 2.1
	 */
	default <T> Mono<T> findAndReplace(Query query, T replacement, FindAndReplaceOptions options) {
		return findAndReplace(query, replacement, options, getCollectionName(ClassUtils.getUserClass(replacement)));
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace</a>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @return the converted object that was updated or {@link Mono#empty()}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @since 2.1
	 */
	default <T> Mono<T> findAndReplace(Query query, T replacement, FindAndReplaceOptions options, String collectionName) {

		Assert.notNull(replacement, "Replacement must not be null");
		return findAndReplace(query, replacement, options, (Class<T>) ClassUtils.getUserClass(replacement), collectionName);
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace</a>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @param entityType the parametrized type. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the converted object that was updated or {@link Mono#empty()}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @since 2.1
	 */
	default <T> Mono<T> findAndReplace(Query query, T replacement, FindAndReplaceOptions options, Class<T> entityType,
			String collectionName) {

		return findAndReplace(query, replacement, options, entityType, collectionName, entityType);
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace</a>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @param entityType the type used for mapping the {@link Query} to domain type fields and deriving the collection
	 *          from. Must not be {@literal null}.
	 * @param resultType the parametrized type projection return type. Must not be {@literal null}, use the domain type of
	 *          {@code Object.class} instead.
	 * @return the converted object that was updated or {@link Mono#empty()}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given replacement value.
	 * @since 2.1
	 */
	default <S, T> Mono<T> findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType,
			Class<T> resultType) {

		return findAndReplace(query, replacement, options, entityType,
				getCollectionName(ClassUtils.getUserClass(entityType)), resultType);
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace</a>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document and also an
	 *          optional fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @param entityType the type used for mapping the {@link Query} to domain type fields and deriving the collection
	 *          from. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @param resultType resultType the parametrized type projection return type. Must not be {@literal null}, use the
	 *          domain type of {@code Object.class} instead.
	 * @return the converted object that was updated or {@link Mono#empty()}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @since 2.1
	 */
	<S, T> Mono<T> findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType,
			String collectionName, Class<T> resultType);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity type to a single instance of an object of the
	 * specified type. The first document that matches the query is returned and also removed from the collection in the
	 * database. <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. <br />
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned {@link Mono}.
	 * @return the converted object
	 */
	<T> Mono<T> findAndRemove(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a single instance of an object of the specified
	 * type. The first document that matches the query is returned and also removed from the collection in the database.
	 * <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned {@link Mono}.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @return the converted object.
	 */
	<T> Mono<T> findAndRemove(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Returns the number of documents for the given {@link Query} by querying the collection of the given entity class.
	 * <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches. <br />
	 * This method may choose to use {@link #estimatedCount(Class)} for empty queries instead of running an
	 * {@link com.mongodb.reactivestreams.client.MongoCollection#countDocuments(org.bson.conversions.Bson, com.mongodb.client.model.CountOptions)
	 * aggregation execution} which may have an impact on performance.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents. Must not be
	 *          {@literal null}.
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @return the count of matching documents.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 * @see #exactCount(Query, Class)
	 * @see #estimatedCount(Class)
	 */
	Mono<Long> count(Query query, Class<?> entityClass);

	/**
	 * Returns the number of documents for the given {@link Query} querying the given collection. The given {@link Query}
	 * must solely consist of document field references as we lack type information to map potential property references
	 * onto document fields. Use {@link #count(Query, Class, String)} to get full type specific support. <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches. <br />
	 * This method may choose to use {@link #estimatedCount(Class)} for empty queries instead of running an
	 * {@link com.mongodb.reactivestreams.client.MongoCollection#countDocuments(org.bson.conversions.Bson, com.mongodb.client.model.CountOptions)
	 * aggregation execution} which may have an impact on performance.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents.
	 * @param collectionName must not be {@literal null} or empty.
	 * @return the count of matching documents.
	 * @see #count(Query, Class, String)
	 * @see #estimatedCount(String)
	 * @see #exactCount(Query, String)
	 */
	Mono<Long> count(Query query, String collectionName);

	/**
	 * Returns the number of documents for the given {@link Query} by querying the given collection using the given entity
	 * class to map the given {@link Query}. <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches. <br />
	 * This method may choose to use {@link #estimatedCount(Class)} for empty queries instead of running an
	 * {@link com.mongodb.reactivestreams.client.MongoCollection#countDocuments(org.bson.conversions.Bson, com.mongodb.client.model.CountOptions)
	 * aggregation execution} which may have an impact on performance.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents. Must not be
	 *          {@literal null}.
	 * @param entityClass the parametrized type. Can be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 * @return the count of matching documents.
	 * @see #estimatedCount(String)
	 * @see #exactCount(Query, Class, String)
	 */
	Mono<Long> count(Query query, @Nullable Class<?> entityClass, String collectionName);

	/**
	 * Estimate the number of documents, in the collection {@link #getCollectionName(Class) identified by the given type},
	 * based on collection statistics. <br />
	 * Please make sure to read the MongoDB reference documentation about limitations on eg. sharded cluster or inside
	 * transactions.
	 *
	 * @param entityClass must not be {@literal null}.
	 * @return a {@link Mono} emitting the estimated number of documents.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 * @since 3.1
	 */
	default Mono<Long> estimatedCount(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null");
		return estimatedCount(getCollectionName(entityClass));
	}

	/**
	 * Estimate the number of documents in the given collection based on collection statistics. <br />
	 * Please make sure to read the MongoDB reference documentation about limitations on eg. sharded cluster or inside
	 * transactions.
	 *
	 * @param collectionName must not be {@literal null}.
	 * @return a {@link Mono} emitting the estimated number of documents.
	 * @since 3.1
	 */
	Mono<Long> estimatedCount(String collectionName);

	/**
	 * Returns the number of documents for the given {@link Query} by querying the collection of the given entity class.
	 * <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches. <br />
	 * This method uses an
	 * {@link com.mongodb.reactivestreams.client.MongoCollection#countDocuments(org.bson.conversions.Bson, com.mongodb.client.model.CountOptions)
	 * aggregation execution} even for empty {@link Query queries} which may have an impact on performance, but guarantees
	 * shard, session and transaction compliance. In case an inaccurate count satisfies the applications needs use
	 * {@link #estimatedCount(Class)} for empty queries instead.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents. Must not be
	 *          {@literal null}.
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @return the count of matching documents.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 * @since 3.4
	 */
	default Mono<Long> exactCount(Query query, Class<?> entityClass) {
		return exactCount(query, entityClass, getCollectionName(entityClass));
	}

	/**
	 * Returns the number of documents for the given {@link Query} querying the given collection. The given {@link Query}
	 * must solely consist of document field references as we lack type information to map potential property references
	 * onto document fields. Use {@link #count(Query, Class, String)} to get full type specific support. <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches. <br />
	 * This method uses an
	 * {@link com.mongodb.reactivestreams.client.MongoCollection#countDocuments(org.bson.conversions.Bson, com.mongodb.client.model.CountOptions)
	 * aggregation execution} even for empty {@link Query queries} which may have an impact on performance, but guarantees
	 * shard, session and transaction compliance. In case an inaccurate count satisfies the applications needs use
	 * {@link #estimatedCount(String)} for empty queries instead.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents.
	 * @param collectionName must not be {@literal null} or empty.
	 * @return the count of matching documents.
	 * @see #count(Query, Class, String)
	 * @since 3.4
	 */
	default Mono<Long> exactCount(Query query, String collectionName) {
		return exactCount(query, null, collectionName);
	}

	/**
	 * Returns the number of documents for the given {@link Query} by querying the given collection using the given entity
	 * class to map the given {@link Query}. <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches. <br />
	 * This method uses an
	 * {@link com.mongodb.reactivestreams.client.MongoCollection#countDocuments(org.bson.conversions.Bson, com.mongodb.client.model.CountOptions)
	 * aggregation execution} even for empty {@link Query queries} which may have an impact on performance, but guarantees
	 * shard, session and transaction compliance. In case an inaccurate count satisfies the applications needs use
	 * {@link #estimatedCount(String)} for empty queries instead.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents. Must not be
	 *          {@literal null}.
	 * @param entityClass the parametrized type. Can be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 * @return the count of matching documents.
	 * @since 3.4
	 */
	Mono<Long> exactCount(Query query, @Nullable Class<?> entityClass, String collectionName);

	/**
	 * Insert the object into the collection for the entity type of the object to save. <br />
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. <br />
	 * If your object has an {@literal Id} property which holds a {@literal null} value, it will be set with the generated
	 * Id from MongoDB. If your Id property is a String then MongoDB ObjectId will be used to populate that string.
	 * Otherwise, the conversion from ObjectId to your property type will be handled by Spring's BeanWrapper class that
	 * leverages Type Conversion API. See
	 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation" > Spring's
	 * Type Conversion"</a> for more details. <br />
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 * <p>
	 * Inserting new objects will trigger {@link org.springframework.data.annotation.Version} property initialization.
	 * <p>
	 * The {@code objectToSave} must not be collection-like.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @return the inserted object.
	 * @throws IllegalArgumentException in case the {@code objectToSave} is collection-like.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given object type.
	 */
	<T> Mono<T> insert(T objectToSave);

	/**
	 * Insert the object into the specified collection. <br />
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 * <p>
	 * Inserting new objects will trigger {@link org.springframework.data.annotation.Version} property initialization.
	 * <p>
	 * The {@code objectToSave} must not be collection-like.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @param collectionName name of the collection to store the object in. Must not be {@literal null}.
	 * @return the inserted object.
	 * @throws IllegalArgumentException in case the {@code objectToSave} is collection-like.
	 */
	<T> Mono<T> insert(T objectToSave, String collectionName);

	/**
	 * Insert a Collection of objects into a collection in a single batch write to the database.
	 * <p>
	 * If an object within the batch has an {@literal Id} property which holds a {@literal null} value, it will be set
	 * with the generated Id from MongoDB.
	 * <p>
	 * Inserting new objects will trigger {@link org.springframework.data.annotation.Version} property initialization.
	 *
	 * @param batchToSave the batch of objects to save. Must not be {@literal null}.
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @return the inserted objects.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 */
	<T> Flux<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass);

	/**
	 * Insert a batch of objects into the specified collection in a single batch write to the database.
	 * <p>
	 * If an object within the batch has an {@literal Id} property which holds a {@literal null} value, it will be set
	 * with the generated Id from MongoDB.
	 * <p>
	 * Inserting new objects will trigger {@link org.springframework.data.annotation.Version} property initialization.
	 *
	 * @param batchToSave the list of objects to save. Must not be {@literal null}.
	 * @param collectionName name of the collection to store the object in. Must not be {@literal null}.
	 * @return the inserted objects.
	 */
	<T> Flux<T> insert(Collection<? extends T> batchToSave, String collectionName);

	/**
	 * Insert a mixed Collection of objects into a database collection determining the collection name to use based on the
	 * class.
	 * <p>
	 * If an object within the batch has an {@literal Id} property which holds a {@literal null} value, it will be set
	 * with the generated Id from MongoDB.
	 * <p>
	 * Inserting new objects will trigger {@link org.springframework.data.annotation.Version} property initialization.
	 *
	 * @param objectsToSave the list of objects to save. Must not be {@literal null}.
	 * @return the saved objects.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} for the given objects.
	 */
	<T> Flux<T> insertAll(Collection<? extends T> objectsToSave);

	/**
	 * Insert the object into the collection for the entity type of the object to save. <br />
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. <br />
	 * If your object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Type Conversion API. See
	 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation" > Spring's
	 * Type Conversion"</a> for more details. <br />
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} the property will be auto incremented. The
	 * operation raises an error in case the document has been modified in between.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @return the inserted objects.
	 */
	<T> Mono<T> insert(Mono<? extends T> objectToSave);

	/**
	 * Insert a Collection of objects into a collection in a single batch write to the database.
	 * <p>
	 * If an object within the batch has an {@literal Id} property which holds a {@literal null} value, it will be set
	 * with the generated Id from MongoDB.
	 * <p>
	 * Inserting new objects will trigger {@link org.springframework.data.annotation.Version} property initialization.
	 *
	 * @param batchToSave the publisher which provides objects to save. Must not be {@literal null}.
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @return the inserted objects.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} for the type.
	 */
	<T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> batchToSave, Class<?> entityClass);

	/**
	 * Insert objects into the specified collection in a single batch write to the database.
	 * <p>
	 * If an object within the batch has an {@literal Id} property which holds a {@literal null} value, it will be set
	 * with the generated Id from MongoDB.
	 * <p>
	 * Inserting new objects will trigger {@link org.springframework.data.annotation.Version} property initialization.
	 *
	 * @param batchToSave the publisher which provides objects to save. Must not be {@literal null}.
	 * @param collectionName name of the collection to store the object in. Must not be {@literal null}.
	 * @return the inserted objects.
	 */
	<T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> batchToSave, String collectionName);

	/**
	 * Insert a mixed Collection of objects into a database collection determining the collection name to use based on the
	 * class.
	 * <p>
	 * If an object within the batch has an {@literal Id} property which holds a {@literal null} value, it will be set
	 * with the generated Id from MongoDB.
	 * <p>
	 * Inserting new objects will trigger {@link org.springframework.data.annotation.Version} property initialization.
	 *
	 * @param objectsToSave the publisher which provides objects to save. Must not be {@literal null}.
	 * @return the inserted objects.
	 */
	<T> Flux<T> insertAll(Mono<? extends Collection<? extends T>> objectsToSave);

	/**
	 * Save the object to the collection for the entity type of the object to save. This will perform an insert if the
	 * object is not already present, that is an 'upsert'. <br />
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * If your object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Type Conversion API. See
	 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation" > Spring's
	 * Type Conversion"</a> for more details.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} the property will be auto incremented. The
	 * operation raises an error in case the document has been modified in between.
	 * <p>
	 * The {@code objectToSave} must not be collection-like.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @return the saved object.
	 * @throws IllegalArgumentException in case the {@code objectToSave} is collection-like.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given object type.
	 * @throws org.springframework.dao.OptimisticLockingFailureException in case of version mismatch in case a
	 *           {@link org.springframework.data.annotation.Version} is defined.
	 */
	<T> Mono<T> save(T objectToSave);

	/**
	 * Save the object to the specified collection. This will perform an insert if the object is not already present, that
	 * is an 'upsert'. <br />
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * If your object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Type Conversion API. See
	 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation">Spring's Type
	 * Conversion</a> for more details.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} the property will be auto incremented. The
	 * operation raises an error in case the document has been modified in between.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @param collectionName name of the collection to store the object in. Must not be {@literal null}.
	 * @return the saved object.
	 * @throws IllegalArgumentException in case the {@code objectToSave} is collection-like.
	 * @throws org.springframework.dao.OptimisticLockingFailureException in case of version mismatch in case a
	 *           {@link org.springframework.data.annotation.Version} is defined.
	 */
	<T> Mono<T> save(T objectToSave, String collectionName);

	/**
	 * Save the object to the collection for the entity type of the object to save. This will perform an insert if the
	 * object is not already present, that is an 'upsert'. <br />
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * If your object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Type Conversion API. See
	 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation"> Spring's Type
	 * Conversion</a> for more details.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} the property will be auto incremented. The
	 * operation raises an error in case the document has been modified in between.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @return the saved object.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given object type.
	 * @throws org.springframework.dao.OptimisticLockingFailureException in case of version mismatch in case a
	 *           {@link org.springframework.data.annotation.Version} is defined.
	 */
	<T> Mono<T> save(Mono<? extends T> objectToSave);

	/**
	 * Save the object to the specified collection. This will perform an insert if the object is not already present, that
	 * is an 'upsert'. <br />
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * If your object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Type Conversion API. See
	 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation">Spring's Type
	 * Conversion</a> for more details.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} the property will be auto incremented. The
	 * operation raises an error in case the document has been modified in between.
	 *
	 * @param objectToSave the object to store in the collReactiveMongoOperationsection. Must not be {@literal null}.
	 * @param collectionName name of the collection to store the object in. Must not be {@literal null}.
	 * @return the saved object.
	 * @throws org.springframework.dao.OptimisticLockingFailureException in case of version mismatch in case a
	 *           {@link org.springframework.data.annotation.Version} is defined.
	 */
	<T> Mono<T> save(Mono<? extends T> objectToSave, String collectionName);

	/**
	 * Performs an upsert. If no document is found that matches the query, a new document is created and inserted by
	 * combining the query document and the update document.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 * <p>
	 * <strong>NOTE:</strong> {@link Query#getSortObject() sorting} is not supported by {@code db.collection.updateOne}.
	 * Use {@link #findAndModify(Query, UpdateDefinition, Class)} instead.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be upserted. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing object. Must not be {@literal null}.
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> upsert(Query query, UpdateDefinition update, Class<?> entityClass);

	/**
	 * Performs an upsert. If no document is found that matches the query, a new document is created and inserted by
	 * combining the query document and the update document. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping, versions, etc. is not available due to the lack of
	 * domain type information. Use {@link #upsert(Query, UpdateDefinition, Class, String)} to get full type specific
	 * support.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be upserted. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing object. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> upsert(Query query, UpdateDefinition update, String collectionName);

	/**
	 * Performs an upsert. If no document is found that matches the query, a new document is created and inserted by
	 * combining the query document and the update document.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be upserted. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing object. Must not be {@literal null}.
	 * @param entityClass class of the pojo to be operated on. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> upsert(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName);

	/**
	 * Updates the first object that is found in the collection of the entity class that matches the query document with
	 * the provided update document.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be updated. The
	 *          {@link Query} may define a {@link Query#with(Sort) sort order} to influence which document to update when
	 *          potentially matching multiple candidates. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param entityClass class that determines the collection to use.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, Class<?> entityClass);

	/**
	 * Updates the first object that is found in the specified collection that matches the query document criteria with
	 * the provided updated document. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping, versions, etc. is not available due to the lack of
	 * domain type information. Use {@link #updateFirst(Query, UpdateDefinition, Class, String)} to get full type specific
	 * support.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be updated. The
	 *          {@link Query} may define a {@link Query#with(Sort) sort order} to influence which document to update when
	 *          potentially matching multiple candidates. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, String collectionName);

	/**
	 * Updates the first object that is found in the specified collection that matches the query document criteria with
	 * the provided updated document.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be updated. The
	 *          {@link Query} may define a {@link Query#with(Sort) sort order} to influence which document to update when
	 *          potentially matching multiple candidates. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param entityClass class of the pojo to be operated on. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName);

	/**
	 * Updates all objects that are found in the collection for the entity class that matches the query document criteria
	 * with the provided updated document.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be updated. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param entityClass class of the pojo to be operated on. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, Class<?> entityClass);

	/**
	 * Updates all objects that are found in the specified collection that matches the query document criteria with the
	 * provided updated document. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping, versions, etc. is not available due to the lack of
	 * domain type information. Use {@link #updateMulti(Query, UpdateDefinition, Class, String)} to get full type specific
	 * support.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be updated. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, String collectionName);

	/**
	 * Updates all objects that are found in the collection for the entity class that matches the query document criteria
	 * with the provided updated document.
	 * <p>
	 * A potential {@link org.springframework.data.annotation.Version} property of the {@literal entityClass} will be
	 * auto-incremented if not explicitly specified in the update.
	 *
	 * @param query the query document that specifies the criteria used to select a document to be updated. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param entityClass class of the pojo to be operated on. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName);

	/**
	 * Remove the given object from the collection by {@literal id} and (if applicable) its
	 * {@link org.springframework.data.annotation.Version}.
	 *
	 * @param object must not be {@literal null}.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given object type.
	 */
	Mono<DeleteResult> remove(Object object);

	/**
	 * Removes the given object from the given collection.
	 *
	 * @param object must not be {@literal null}.
	 * @param collectionName name of the collection where the documents will be removed from, must not be {@literal null}
	 *          or empty.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 */
	Mono<DeleteResult> remove(Object object, String collectionName);

	/**
	 * Remove the given object from the collection by {@literal id} and (if applicable) its
	 * {@link org.springframework.data.annotation.Version}.
	 *
	 * @param objectToRemove must not be {@literal null}.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given object type.
	 */
	Mono<DeleteResult> remove(Mono<? extends Object> objectToRemove);

	/**
	 * Removes the given object from the given collection by {@literal id} and (if applicable) its
	 * {@link org.springframework.data.annotation.Version}.
	 *
	 * @param objectToRemove must not be {@literal null}.
	 * @param collectionName name of the collection where the documents will be removed from, must not be {@literal null}
	 *          or empty.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 */
	Mono<DeleteResult> remove(Mono<? extends Object> objectToRemove, String collectionName);

	/**
	 * Remove all documents that match the provided query document criteria from the collection used to store the
	 * entityClass. The Class parameter is also used to help convert the Id of the object if it is present in the query.
	 *
	 * @param query the query document that specifies the criteria used to remove a document.
	 * @param entityClass class that determines the collection to use.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 */
	Mono<DeleteResult> remove(Query query, Class<?> entityClass);

	/**
	 * Remove all documents that match the provided query document criteria from the collection used to store the
	 * entityClass. The Class parameter is also used to help convert the Id of the object if it is present in the query.
	 *
	 * @param query the query document that specifies the criteria used to remove a document.
	 * @param entityClass class of the pojo to be operated on. Can be {@literal null}.
	 * @param collectionName name of the collection where the documents will be removed from, must not be {@literal null}
	 *          or empty.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 */
	Mono<DeleteResult> remove(Query query, @Nullable Class<?> entityClass, String collectionName);

	/**
	 * Remove all documents from the specified collection that match the provided query document criteria. There is no
	 * conversion/mapping done for any criteria using the id field. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping is not available due to the lack of domain type
	 * information. Use {@link #remove(Query, Class, String)} to get full type specific support.
	 *
	 * @param query the query document that specifies the criteria used to remove a document.
	 * @param collectionName name of the collection where the documents will be removed from, must not be {@literal null}
	 *          or empty.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 */
	Mono<DeleteResult> remove(Query query, String collectionName);

	/**
	 * Returns and removes all documents form the specified collection that match the provided query. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping is not available due to the lack of domain type
	 * information. Use {@link #findAllAndRemove(Query, Class, String)} to get full type specific support.
	 *
	 * @param query the query document that specifies the criteria used to find and remove documents.
	 * @param collectionName name of the collection where the documents will be removed from, must not be {@literal null}
	 *          or empty.
	 * @return the {@link Flux} converted objects deleted by this operation.
	 */
	<T> Flux<T> findAllAndRemove(Query query, String collectionName);

	/**
	 * Returns and removes all documents matching the given query form the collection used to store the entityClass.
	 *
	 * @param query the query document that specifies the criteria used to find and remove documents.
	 * @param entityClass class of the pojo to be operated on.
	 * @return the {@link Flux} converted objects deleted by this operation.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 */
	<T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass);

	/**
	 * Returns and removes all documents that match the provided query document criteria from the collection used to store
	 * the entityClass. The Class parameter is also used to help convert the Id of the object if it is present in the
	 * query.
	 *
	 * @param query the query document that specifies the criteria used to find and remove documents.
	 * @param entityClass class of the pojo to be operated on.
	 * @param collectionName name of the collection where the documents will be removed from, must not be {@literal null}
	 *          or empty.
	 * @return the {@link Flux} converted objects deleted by this operation.
	 */
	<T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Replace a single document matching the {@link Criteria} of given {@link Query} with the {@code replacement}
	 * document. <br />
	 * The collection name is derived from the {@literal replacement} type. <br />
	 * Options are defaulted to {@link ReplaceOptions#none()}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document. The query may
	 *          contain an index {@link Query#withHint(String) hint} or the {@link Query#collation(Collation) collation}
	 *          to use. The {@link Query} may define a {@link Query#with(Sort) sort order} to influence which document to
	 *          replace when potentially matching multiple candidates. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous replacement.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given replacement value.
	 * @since 4.2
	 */
	default <T> Mono<UpdateResult> replace(Query query, T replacement) {
		return replace(query, replacement, ReplaceOptions.none());
	}

	/**
	 * Replace a single document matching the {@link Criteria} of given {@link Query} with the {@code replacement}
	 * document. Options are defaulted to {@link ReplaceOptions#none()}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document. The query may
	 *          contain an index {@link Query#withHint(String) hint} or the {@link Query#collation(Collation) collation}
	 *          to use. The {@link Query} may define a {@link Query#with(Sort) sort order} to influence which document to
	 *          replace when potentially matching multiple candidates. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous replacement.
	 * @since 4.2
	 */
	default <T> Mono<UpdateResult> replace(Query query, T replacement, String collectionName) {
		return replace(query, replacement, ReplaceOptions.none(), collectionName);
	}

	/**
	 * Replace a single document matching the {@link Criteria} of given {@link Query} with the {@code replacement}
	 * document taking {@link ReplaceOptions} into account.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document.The query may
	 *          contain an index {@link Query#withHint(String) hint} or the {@link Query#collation(Collation) collation}
	 *          to use. The {@link Query} may define a {@link Query#with(Sort) sort order} to influence which document to
	 *          replace when potentially matching multiple candidates. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link ReplaceOptions} holding additional information. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous replacement.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given replacement value.
	 * @since 4.2
	 */
	default <T> Mono<UpdateResult> replace(Query query, T replacement, ReplaceOptions options) {
		return replace(query, replacement, options, getCollectionName(ClassUtils.getUserClass(replacement)));
	}

	/**
	 * Replace a single document matching the {@link Criteria} of given {@link Query} with the {@code replacement}
	 * document taking {@link ReplaceOptions} into account.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a document. The query may *
	 *          contain an index {@link Query#withHint(String) hint} or the {@link Query#collation(Collation) collation}
	 *          to use. The {@link Query} may define a {@link Query#with(Sort) sort order} to influence which document to
	 *          replace when potentially matching multiple candidates. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link ReplaceOptions} holding additional information. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous replacement.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given replacement value.
	 * @since 4.2
	 */
	<T> Mono<UpdateResult> replace(Query query, T replacement, ReplaceOptions options, String collectionName);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity class to a stream of objects of the specified
	 * type. The stream uses a {@link com.mongodb.CursorType#TailableAwait tailable} cursor that may be an infinite
	 * stream. The stream will not be completed unless the {@link org.reactivestreams.Subscription} is
	 * {@link Subscription#cancel() canceled}. <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned {@link Flux}.
	 * @return the {@link Flux} of converted objects.
	 * @throws org.springframework.data.mapping.MappingException if the target collection name cannot be
	 *           {@link #getCollectionName(Class) derived} from the given type.
	 */
	<T> Flux<T> tail(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity class to a stream of objects of the specified
	 * type. The stream uses a {@link com.mongodb.CursorType#TailableAwait tailable} cursor that may be an infinite
	 * stream. The stream will not be completed unless the {@link org.reactivestreams.Subscription} is
	 * {@link Subscription#cancel() canceled}. <br />
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used. <br />
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a document and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned {@link Flux}.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @return the {@link Flux} of converted objects.
	 */
	<T> Flux<T> tail(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Subscribe to a MongoDB <a href="https://docs.mongodb.com/manual/changeStreams/">Change Stream</a> for all events in
	 * the configured default database via the reactive infrastructure. Use the optional provided {@link Aggregation} to
	 * filter events. The stream will not be completed unless the {@link org.reactivestreams.Subscription} is
	 * {@link Subscription#cancel() canceled}. <br />
	 * The {@link ChangeStreamEvent#getBody()} is mapped to the {@literal resultType} while the
	 * {@link ChangeStreamEvent#getRaw()} contains the unmodified payload. <br />
	 * Use {@link ChangeStreamOptions} to set arguments like {@link ChangeStreamOptions#getResumeToken() the resumseToken}
	 * for resuming change streams.
	 *
	 * @param options must not be {@literal null}. Use {@link ChangeStreamOptions#empty()}.
	 * @param targetType the result type to use.
	 * @param <T>
	 * @return the {@link Flux} emitting {@link ChangeStreamEvent events} as they arrive.
	 * @since 2.1
	 * @see ReactiveMongoDatabaseFactory#getMongoDatabase()
	 * @see ChangeStreamOptions#getFilter()
	 */
	default <T> Flux<ChangeStreamEvent<T>> changeStream(ChangeStreamOptions options, Class<T> targetType) {
		return changeStream(null, options, targetType);
	}

	/**
	 * Subscribe to a MongoDB <a href="https://docs.mongodb.com/manual/changeStreams/">Change Stream</a> for all events in
	 * the given collection via the reactive infrastructure. Use the optional provided {@link Aggregation} to filter
	 * events. The stream will not be completed unless the {@link org.reactivestreams.Subscription} is
	 * {@link Subscription#cancel() canceled}. <br />
	 * The {@link ChangeStreamEvent#getBody()} is mapped to the {@literal resultType} while the
	 * {@link ChangeStreamEvent#getRaw()} contains the unmodified payload. <br />
	 * Use {@link ChangeStreamOptions} to set arguments like {@link ChangeStreamOptions#getResumeToken() the resumseToken}
	 * for resuming change streams.
	 *
	 * @param collectionName the collection to watch. Can be {@literal null} to watch all collections.
	 * @param options must not be {@literal null}. Use {@link ChangeStreamOptions#empty()}.
	 * @param targetType the result type to use.
	 * @param <T>
	 * @return the {@link Flux} emitting {@link ChangeStreamEvent events} as they arrive.
	 * @since 2.1
	 * @see ChangeStreamOptions#getFilter()
	 */
	default <T> Flux<ChangeStreamEvent<T>> changeStream(@Nullable String collectionName, ChangeStreamOptions options,
			Class<T> targetType) {

		return changeStream(null, collectionName, options, targetType);
	}

	/**
	 * Subscribe to a MongoDB <a href="https://docs.mongodb.com/manual/changeStreams/">Change Stream</a> via the reactive
	 * infrastructure. Use the optional provided {@link Aggregation} to filter events. The stream will not be completed
	 * unless the {@link org.reactivestreams.Subscription} is {@link Subscription#cancel() canceled}. <br />
	 * The {@link ChangeStreamEvent#getBody()} is mapped to the {@literal resultType} while the
	 * {@link ChangeStreamEvent#getRaw()} contains the unmodified payload. <br />
	 * Use {@link ChangeStreamOptions} to set arguments like {@link ChangeStreamOptions#getResumeToken() the resumseToken}
	 * for resuming change streams.
	 *
	 * @param database the database to watch. Can be {@literal null}, uses configured default if so.
	 * @param collectionName the collection to watch. Can be {@literal null}, watches all collections if so.
	 * @param options must not be {@literal null}. Use {@link ChangeStreamOptions#empty()}.
	 * @param targetType the result type to use.
	 * @param <T>
	 * @return the {@link Flux} emitting {@link ChangeStreamEvent events} as they arrive.
	 * @since 2.1
	 * @see ChangeStreamOptions#getFilter()
	 */
	<T> Flux<ChangeStreamEvent<T>> changeStream(@Nullable String database, @Nullable String collectionName,
			ChangeStreamOptions options, Class<T> targetType);

	/**
	 * Execute a map-reduce operation. Use {@link MapReduceOptions} to optionally specify an output collection and other
	 * args.
	 *
	 * @param filterQuery the selection criteria for the documents going input to the map function. Must not be
	 *          {@literal null}.
	 * @param domainType source type used to determine the input collection name and map the filter {@link Query} against.
	 *          Must not be {@literal null}.
	 * @param resultType the mapping target of the operations result documents. Must not be {@literal null}.
	 * @param mapFunction the JavaScript map function. Must not be {@literal null}.
	 * @param reduceFunction the JavaScript reduce function. Must not be {@literal null}.
	 * @param options additional options like output collection. Must not be {@literal null}.
	 * @return a {@link Flux} emitting the result document sequence. Never {@literal null}.
	 * @since 2.1
	 * @deprecated since 3.4 in favor of {@link #aggregate(TypedAggregation, Class)}.
	 */
	@Deprecated
	<T> Flux<T> mapReduce(Query filterQuery, Class<?> domainType, Class<T> resultType, String mapFunction,
			String reduceFunction, MapReduceOptions options);

	/**
	 * Execute a map-reduce operation. Use {@link MapReduceOptions} to optionally specify an output collection and other
	 * args.
	 *
	 * @param filterQuery the selection criteria for the documents going input to the map function. Must not be
	 *          {@literal null}.
	 * @param domainType source type used to map the filter {@link Query} against. Must not be {@literal null}.
	 * @param inputCollectionName the input collection.
	 * @param resultType the mapping target of the operations result documents. Must not be {@literal null}.
	 * @param mapFunction the JavaScript map function. Must not be {@literal null}.
	 * @param reduceFunction the JavaScript reduce function. Must not be {@literal null}.
	 * @param options additional options like output collection. Must not be {@literal null}.
	 * @return a {@link Flux} emitting the result document sequence. Never {@literal null}.
	 * @since 2.1
	 * @deprecated since 3.4 in favor of {@link #aggregate(TypedAggregation, Class)}.
	 */
	@Deprecated
	<T> Flux<T> mapReduce(Query filterQuery, Class<?> domainType, String inputCollectionName, Class<T> resultType,
			String mapFunction, String reduceFunction, MapReduceOptions options);

	/**
	 * Returns the underlying {@link MongoConverter}.
	 *
	 * @return never {@literal null}.
	 */
	MongoConverter getConverter();

	/**
	 * The collection name used for the specified class by this template.
	 *
	 * @param entityClass must not be {@literal null}.
	 * @return never {@literal null}.
	 * @throws org.springframework.data.mapping.MappingException if the collection name cannot be derived from the type.
	 * @since 2.1
	 */
	String getCollectionName(Class<?> entityClass);

}
