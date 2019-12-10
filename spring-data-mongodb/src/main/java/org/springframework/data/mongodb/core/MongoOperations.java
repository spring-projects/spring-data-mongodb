/*
 * Copyright 2011-2019 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.bson.Document;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.util.CloseableIterator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * Interface that specifies a basic set of MongoDB operations. Implemented by {@link MongoTemplate}. Not often used but
 * a useful option for extensibility and testability (as it can be easily mocked, stubbed, or be the target of a JDK
 * proxy).
 * <p/>
 * <strong>NOTE:</strong> Some operations cannot be executed within a MongoDB transaction. Please refer to the MongoDB
 * specific documentation to learn more about <a href="https://docs.mongodb.com/manual/core/transactions/">Multi
 * Document Transactions</a>.
 *
 * @author Thomas Risberg
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Tobias Trelle
 * @author Chuong Ngo
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Maninder Singh
 * @author Mark Paluch
 */
public interface MongoOperations extends FluentMongoOperations {

	/**
	 * The collection name used for the specified class by this template.
	 *
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getCollectionName(Class<?> entityClass);

	/**
	 * Execute the a MongoDB command expressed as a JSON string. Parsing is delegated to {@link Document#parse(String)} to
	 * obtain the {@link Document} holding the actual command. Any errors that result from executing this command will be
	 * converted into Spring's DAO exception hierarchy.
	 *
	 * @param jsonCommand a MongoDB command expressed as a JSON string. Must not be {@literal null}.
	 * @return a result object returned by the action.
	 */
	Document executeCommand(String jsonCommand);

	/**
	 * Execute a MongoDB command. Any errors that result from executing this command will be converted into Spring's DAO
	 * exception hierarchy.
	 *
	 * @param command a MongoDB command.
	 * @return a result object returned by the action.
	 */
	Document executeCommand(Document command);

	/**
	 * Execute a MongoDB command. Any errors that result from executing this command will be converted into Spring's data
	 * access exception hierarchy.
	 *
	 * @param command a MongoDB command, must not be {@literal null}.
	 * @param readPreference read preferences to use, can be {@literal null}.
	 * @return a result object returned by the action.
	 * @since 1.7
	 */
	Document executeCommand(Document command, @Nullable ReadPreference readPreference);

	/**
	 * Execute a MongoDB query and iterate over the query results on a per-document basis with a DocumentCallbackHandler.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @param dch the handler that will extract results, one document at a time.
	 */
	void executeQuery(Query query, String collectionName, DocumentCallbackHandler dch);

	/**
	 * Executes a {@link DbCallback} translating any exceptions as necessary.
	 * <p/>
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 *
	 * @param action callback object that specifies the MongoDB actions to perform on the passed in DB instance. Must not
	 *          be {@literal null}.
	 * @param <T> return type.
	 * @return a result object returned by the action or {@literal null}.
	 */
	@Nullable
	<T> T execute(DbCallback<T> action);

	/**
	 * Executes the given {@link CollectionCallback} on the entity collection of the specified class.
	 * <p/>
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 *
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @param action callback object that specifies the MongoDB action. Must not be {@literal null}.
	 * @param <T> return type.
	 * @return a result object returned by the action or {@literal null}.
	 */
	@Nullable
	<T> T execute(Class<?> entityClass, CollectionCallback<T> action);

	/**
	 * Executes the given {@link CollectionCallback} on the collection of the given name.
	 * <p/>
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 *
	 * @param collectionName the name of the collection that specifies which {@link MongoCollection} instance will be
	 *          passed into. Must not be {@literal null} or empty.
	 * @param action callback object that specifies the MongoDB action the callback action. Must not be {@literal null}.
	 * @param <T> return type.
	 * @return a result object returned by the action or {@literal null}.
	 */
	@Nullable
	<T> T execute(String collectionName, CollectionCallback<T> action);

	/**
	 * Obtain a {@link ClientSession session} bound instance of {@link SessionScoped} binding a new {@link ClientSession}
	 * with given {@literal sessionOptions} to each and every command issued against MongoDB.
	 *
	 * @param sessionOptions must not be {@literal null}.
	 * @return new instance of {@link SessionScoped}. Never {@literal null}.
	 * @since 2.1
	 */
	SessionScoped withSession(ClientSessionOptions sessionOptions);

	/**
	 * Obtain a {@link ClientSession session} bound instance of {@link SessionScoped} binding the {@link ClientSession}
	 * provided by the given {@link Supplier} to each and every command issued against MongoDB.
	 * <p/>
	 * <strong>Note:</strong> It is up to the caller to manage the {@link ClientSession} lifecycle. Use the
	 * {@link SessionScoped#execute(SessionCallback, Consumer)} hook to potentially close the {@link ClientSession}.
	 *
	 * @param sessionProvider must not be {@literal null}.
	 * @since 2.1
	 */
	default SessionScoped withSession(Supplier<ClientSession> sessionProvider) {

		Assert.notNull(sessionProvider, "SessionProvider must not be null!");

		return new SessionScoped() {

			private final Object lock = new Object();
			private @Nullable ClientSession session = null;

			@Override
			public <T> T execute(SessionCallback<T> action, Consumer<ClientSession> onComplete) {

				synchronized (lock) {
					if (session == null) {
						session = sessionProvider.get();
					}
				}

				try {
					return action.doInSession(MongoOperations.this.withSession(session));
				} finally {
					onComplete.accept(session);
				}
			}
		};
	}

	/**
	 * Obtain a {@link ClientSession} bound instance of {@link MongoOperations}.
	 * <p/>
	 * <strong>Note:</strong> It is up to the caller to manage the {@link ClientSession} lifecycle.
	 *
	 * @param session must not be {@literal null}.
	 * @return {@link ClientSession} bound instance of {@link MongoOperations}.
	 * @since 2.1
	 */
	MongoOperations withSession(ClientSession session);

	/**
	 * Executes the given {@link Query} on the entity collection of the specified {@code entityType} backed by a Mongo DB
	 * {@link com.mongodb.client.FindIterable}.
	 * <p>
	 * Returns a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.client.FindIterable} that needs to
	 * be closed.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param entityType must not be {@literal null}.
	 * @param <T> element return type
	 * @return will never be {@literal null}.
	 * @since 1.7
	 */
	<T> CloseableIterator<T> stream(Query query, Class<T> entityType);

	/**
	 * Executes the given {@link Query} on the entity collection of the specified {@code entityType} and collection backed
	 * by a Mongo DB {@link com.mongodb.client.FindIterable}.
	 * <p>
	 * Returns a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.client.FindIterable} that needs to
	 * be closed.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param entityType must not be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 * @param <T> element return type
	 * @return will never be {@literal null}.
	 * @since 1.10
	 */
	<T> CloseableIterator<T> stream(Query query, Class<T> entityType, String collectionName);

	/**
	 * Create an uncapped collection with a name based on the provided entity class.
	 *
	 * @param entityClass class that determines the collection to create.
	 * @return the created collection.
	 */
	<T> MongoCollection<Document> createCollection(Class<T> entityClass);

	/**
	 * Create a collection with a name based on the provided entity class using the options.
	 *
	 * @param entityClass class that determines the collection to create. Must not be {@literal null}.
	 * @param collectionOptions options to use when creating the collection.
	 * @return the created collection.
	 */
	<T> MongoCollection<Document> createCollection(Class<T> entityClass, @Nullable CollectionOptions collectionOptions);

	/**
	 * Create an uncapped collection with the provided name.
	 *
	 * @param collectionName name of the collection.
	 * @return the created collection.
	 */
	MongoCollection<Document> createCollection(String collectionName);

	/**
	 * Create a collection with the provided name and options.
	 *
	 * @param collectionName name of the collection. Must not be {@literal null} nor empty.
	 * @param collectionOptions options to use when creating the collection.
	 * @return the created collection.
	 */
	MongoCollection<Document> createCollection(String collectionName, @Nullable CollectionOptions collectionOptions);

	/**
	 * A set of collection names.
	 *
	 * @return list of collection names.
	 */
	Set<String> getCollectionNames();

	/**
	 * Get a {@link MongoCollection} by its name. The returned collection may not exists yet (except in local memory) and
	 * is created on first interaction with the server. Collections can be explicitly created via
	 * {@link #createCollection(Class)}. Please make sure to check if the collection {@link #collectionExists(Class)
	 * exists} first.
	 * <p/>
	 * Translate any exceptions as necessary.
	 *
	 * @param collectionName name of the collection. Must not be {@literal null}.
	 * @return an existing collection or one created on first server interaction.
	 */
	MongoCollection<Document> getCollection(String collectionName);

	/**
	 * Check to see if a collection with a name indicated by the entity class exists.
	 * <p/>
	 * Translate any exceptions as necessary.
	 *
	 * @param entityClass class that determines the name of the collection. Must not be {@literal null}.
	 * @return true if a collection with the given name is found, false otherwise.
	 */
	<T> boolean collectionExists(Class<T> entityClass);

	/**
	 * Check to see if a collection with a given name exists.
	 * <p/>
	 * Translate any exceptions as necessary.
	 *
	 * @param collectionName name of the collection. Must not be {@literal null}.
	 * @return true if a collection with the given name is found, false otherwise.
	 */
	boolean collectionExists(String collectionName);

	/**
	 * Drop the collection with the name indicated by the entity class.
	 * <p/>
	 * Translate any exceptions as necessary.
	 *
	 * @param entityClass class that determines the collection to drop/delete. Must not be {@literal null}.
	 */
	<T> void dropCollection(Class<T> entityClass);

	/**
	 * Drop the collection with the given name.
	 * <p/>
	 * Translate any exceptions as necessary.
	 *
	 * @param collectionName name of the collection to drop/delete.
	 */
	void dropCollection(String collectionName);

	/**
	 * Returns the operations that can be performed on indexes
	 *
	 * @return index operations on the named collection
	 */
	IndexOperations indexOps(String collectionName);

	/**
	 * Returns the operations that can be performed on indexes
	 *
	 * @return index operations on the named collection associated with the given entity class
	 */
	IndexOperations indexOps(Class<?> entityClass);

	/**
	 * Returns the {@link ScriptOperations} that can be performed on {@link com.mongodb.client.MongoDatabase} level.
	 *
	 * @return
	 * @since 1.7
	 * @deprecated since 2.2. The {@code eval} command has been removed without replacement in MongoDB Server 4.2.0.
	 */
	@Deprecated
	ScriptOperations scriptOps();

	/**
	 * Returns a new {@link BulkOperations} for the given collection. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping, etc. is not available for {@literal update} or
	 * {@literal remove} operations in bulk mode due to the lack of domain type information. Use
	 * {@link #bulkOps(BulkMode, Class, String)} to get full type specific support.
	 *
	 * @param mode the {@link BulkMode} to use for bulk operations, must not be {@literal null}.
	 * @param collectionName the name of the collection to work on, must not be {@literal null} or empty.
	 * @return {@link BulkOperations} on the named collection
	 */
	BulkOperations bulkOps(BulkMode mode, String collectionName);

	/**
	 * Returns a new {@link BulkOperations} for the given entity type.
	 *
	 * @param mode the {@link BulkMode} to use for bulk operations, must not be {@literal null}.
	 * @param entityType the name of the entity class, must not be {@literal null}.
	 * @return {@link BulkOperations} on the named collection associated of the given entity class.
	 */
	BulkOperations bulkOps(BulkMode mode, Class<?> entityType);

	/**
	 * Returns a new {@link BulkOperations} for the given entity type and collection name.
	 *
	 * @param mode the {@link BulkMode} to use for bulk operations, must not be {@literal null}.
	 * @param entityType the name of the entity class. Can be {@literal null}.
	 * @param collectionName the name of the collection to work on, must not be {@literal null} or empty.
	 * @return {@link BulkOperations} on the named collection associated with the given entity class.
	 */
	BulkOperations bulkOps(BulkMode mode, @Nullable Class<?> entityType, String collectionName);

	/**
	 * Query for a list of objects of type T from the collection used by the entity class.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient way
	 * to map objects since the test for class type is done in the client and not on the server.
	 *
	 * @param entityClass the parametrized type of the returned list.
	 * @return the converted collection.
	 */
	<T> List<T> findAll(Class<T> entityClass);

	/**
	 * Query for a list of objects of type T from the specified collection.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient way
	 * to map objects since the test for class type is done in the client and not on the server.
	 *
	 * @param entityClass the parametrized type of the returned list.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @return the converted collection.
	 */
	<T> List<T> findAll(Class<T> entityClass, String collectionName);

	/**
	 * Execute a group operation over the entire collection. The group operation entity class should match the 'shape' of
	 * the returned object that takes int account the initial document structure as well as any finalize functions.
	 *
	 * @param inputCollectionName the collection where the group operation will read from
	 * @param groupBy the conditions under which the group operation will be performed, e.g. keys, initial document,
	 *          reduce function.
	 * @param entityClass The parametrized type of the returned list
	 * @return The results of the group operation
	 * @deprecated since 2.2. The {@code group} command has been removed in MongoDB Server 4.2.0. <br />
	 *             Please use {@link #aggregate(TypedAggregation, String, Class) } with a
	 *             {@link org.springframework.data.mongodb.core.aggregation.GroupOperation} instead.
	 */
	@Deprecated
	<T> GroupByResults<T> group(String inputCollectionName, GroupBy groupBy, Class<T> entityClass);

	/**
	 * Execute a group operation restricting the rows to those which match the provided Criteria. The group operation
	 * entity class should match the 'shape' of the returned object that takes int account the initial document structure
	 * as well as any finalize functions.
	 *
	 * @param criteria The criteria that restricts the row that are considered for grouping. If not specified all rows are
	 *          considered.
	 * @param inputCollectionName the collection where the group operation will read from
	 * @param groupBy the conditions under which the group operation will be performed, e.g. keys, initial document,
	 *          reduce function.
	 * @param entityClass The parametrized type of the returned list
	 * @return The results of the group operation
	 * @deprecated since 2.2. The {@code group} command has been removed in MongoDB Server 4.2.0. <br />
	 *             Please use {@link #aggregate(TypedAggregation, String, Class) } with a
	 *             {@link org.springframework.data.mongodb.core.aggregation.GroupOperation} and
	 *             {@link org.springframework.data.mongodb.core.aggregation.MatchOperation} instead.
	 */
	@Deprecated
	<T> GroupByResults<T> group(@Nullable Criteria criteria, String inputCollectionName, GroupBy groupBy,
			Class<T> entityClass);

	/**
	 * Execute an aggregation operation. The raw results will be mapped to the given entity class. The name of the
	 * inputCollection is derived from the inputType of the aggregation.
	 *
	 * @param aggregation The {@link TypedAggregation} specification holding the aggregation operations, must not be
	 *          {@literal null}.
	 * @param collectionName The name of the input collection to use for the aggreation.
	 * @param outputType The parametrized type of the returned list, must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @since 1.3
	 */
	<O> AggregationResults<O> aggregate(TypedAggregation<?> aggregation, String collectionName, Class<O> outputType);

	/**
	 * Execute an aggregation operation. The raw results will be mapped to the given entity class. The name of the
	 * inputCollection is derived from the inputType of the aggregation.
	 *
	 * @param aggregation The {@link TypedAggregation} specification holding the aggregation operations, must not be
	 *          {@literal null}.
	 * @param outputType The parametrized type of the returned list, must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @since 1.3
	 */
	<O> AggregationResults<O> aggregate(TypedAggregation<?> aggregation, Class<O> outputType);

	/**
	 * Execute an aggregation operation. The raw results will be mapped to the given entity class.
	 *
	 * @param aggregation The {@link Aggregation} specification holding the aggregation operations, must not be
	 *          {@literal null}.
	 * @param inputType the inputType where the aggregation operation will read from, must not be {@literal null} or
	 *          empty.
	 * @param outputType The parametrized type of the returned list, must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @since 1.3
	 */
	<O> AggregationResults<O> aggregate(Aggregation aggregation, Class<?> inputType, Class<O> outputType);

	/**
	 * Execute an aggregation operation. The raw results will be mapped to the given entity class.
	 *
	 * @param aggregation The {@link Aggregation} specification holding the aggregation operations, must not be
	 *          {@literal null}.
	 * @param collectionName the collection where the aggregation operation will read from, must not be {@literal null} or
	 *          empty.
	 * @param outputType The parametrized type of the returned list, must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @since 1.3
	 */
	<O> AggregationResults<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType);

	/**
	 * Execute an aggregation operation backed by a Mongo DB {@link com.mongodb.client.AggregateIterable}.
	 * <p>
	 * Returns a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.client.AggregateIterable} that
	 * needs to be closed. The raw results will be mapped to the given entity class. The name of the inputCollection is
	 * derived from the inputType of the aggregation.
	 * <p>
	 * Aggregation streaming can't be used with {@link AggregationOptions#isExplain() aggregation explain}. Enabling
	 * explanation mode will throw an {@link IllegalArgumentException}.
	 *
	 * @param aggregation The {@link TypedAggregation} specification holding the aggregation operations, must not be
	 *          {@literal null}.
	 * @param collectionName The name of the input collection to use for the aggreation.
	 * @param outputType The parametrized type of the returned list, must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @since 2.0
	 */
	<O> CloseableIterator<O> aggregateStream(TypedAggregation<?> aggregation, String collectionName, Class<O> outputType);

	/**
	 * Execute an aggregation operation backed by a Mongo DB {@link com.mongodb.client.AggregateIterable}.
	 * <p/>
	 * Returns a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.client.AggregateIterable} that
	 * needs to be closed. The raw results will be mapped to the given entity class and are returned as stream. The name
	 * of the inputCollection is derived from the inputType of the aggregation.
	 * <p/>
	 * Aggregation streaming can't be used with {@link AggregationOptions#isExplain() aggregation explain}. Enabling
	 * explanation mode will throw an {@link IllegalArgumentException}.
	 *
	 * @param aggregation The {@link TypedAggregation} specification holding the aggregation operations, must not be
	 *          {@literal null}.
	 * @param outputType The parametrized type of the returned list, must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @since 2.0
	 */
	<O> CloseableIterator<O> aggregateStream(TypedAggregation<?> aggregation, Class<O> outputType);

	/**
	 * Execute an aggregation operation backed by a Mongo DB {@link com.mongodb.client.AggregateIterable}.
	 * <p/>
	 * Returns a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.client.AggregateIterable} that
	 * needs to be closed. The raw results will be mapped to the given entity class.
	 * <p/>
	 * Aggregation streaming can't be used with {@link AggregationOptions#isExplain() aggregation explain}. Enabling
	 * explanation mode will throw an {@link IllegalArgumentException}.
	 *
	 * @param aggregation The {@link Aggregation} specification holding the aggregation operations, must not be
	 *          {@literal null}.
	 * @param inputType the inputType where the aggregation operation will read from, must not be {@literal null} or
	 *          empty.
	 * @param outputType The parametrized type of the returned list, must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @since 2.0
	 */
	<O> CloseableIterator<O> aggregateStream(Aggregation aggregation, Class<?> inputType, Class<O> outputType);

	/**
	 * Execute an aggregation operation backed by a Mongo DB {@link com.mongodb.client.AggregateIterable}.
	 * <p/>
	 * Returns a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.client.AggregateIterable} that
	 * needs to be closed. The raw results will be mapped to the given entity class.
	 * <p/>
	 * Aggregation streaming can't be used with {@link AggregationOptions#isExplain() aggregation explain}. Enabling
	 * explanation mode will throw an {@link IllegalArgumentException}.
	 *
	 * @param aggregation The {@link Aggregation} specification holding the aggregation operations, must not be
	 *          {@literal null}.
	 * @param collectionName the collection where the aggregation operation will read from, must not be {@literal null} or
	 *          empty.
	 * @param outputType The parametrized type of the returned list, must not be {@literal null}.
	 * @return The results of the aggregation operation.
	 * @since 2.0
	 */
	<O> CloseableIterator<O> aggregateStream(Aggregation aggregation, String collectionName, Class<O> outputType);

	/**
	 * Execute a map-reduce operation. The map-reduce operation will be formed with an output type of INLINE
	 *
	 * @param inputCollectionName the collection where the map-reduce will read from. Must not be {@literal null}.
	 * @param mapFunction The JavaScript map function.
	 * @param reduceFunction The JavaScript reduce function
	 * @param entityClass The parametrized type of the returned list. Must not be {@literal null}.
	 * @return The results of the map reduce operation
	 */
	<T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			Class<T> entityClass);

	/**
	 * Execute a map-reduce operation that takes additional map-reduce options.
	 *
	 * @param inputCollectionName the collection where the map-reduce will read from. Must not be {@literal null}.
	 * @param mapFunction The JavaScript map function
	 * @param reduceFunction The JavaScript reduce function
	 * @param mapReduceOptions Options that specify detailed map-reduce behavior.
	 * @param entityClass The parametrized type of the returned list. Must not be {@literal null}.
	 * @return The results of the map reduce operation
	 */
	<T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			@Nullable MapReduceOptions mapReduceOptions, Class<T> entityClass);

	/**
	 * Execute a map-reduce operation that takes a query. The map-reduce operation will be formed with an output type of
	 * INLINE
	 *
	 * @param query The query to use to select the data for the map phase. Must not be {@literal null}.
	 * @param inputCollectionName the collection where the map-reduce will read from. Must not be {@literal null}.
	 * @param mapFunction The JavaScript map function
	 * @param reduceFunction The JavaScript reduce function
	 * @param entityClass The parametrized type of the returned list. Must not be {@literal null}.
	 * @return The results of the map reduce operation
	 */
	<T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction, String reduceFunction,
			Class<T> entityClass);

	/**
	 * Execute a map-reduce operation that takes a query and additional map-reduce options
	 *
	 * @param query The query to use to select the data for the map phase. Must not be {@literal null}.
	 * @param inputCollectionName the collection where the map-reduce will read from. Must not be {@literal null}.
	 * @param mapFunction The JavaScript map function
	 * @param reduceFunction The JavaScript reduce function
	 * @param mapReduceOptions Options that specify detailed map-reduce behavior
	 * @param entityClass The parametrized type of the returned list. Must not be {@literal null}.
	 * @return The results of the map reduce operation
	 */
	<T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction, String reduceFunction,
			@Nullable MapReduceOptions mapReduceOptions, Class<T> entityClass);

	/**
	 * Returns {@link GeoResults} for all entities matching the given {@link NearQuery}. Will consider entity mapping
	 * information to determine the collection the query is ran against. Note, that MongoDB limits the number of results
	 * by default. Make sure to add an explicit limit to the {@link NearQuery} if you expect a particular number of
	 * results.
	 * <p>
	 * MongoDB 4.2 has removed the {@code geoNear} command. This method uses since version 2.2 aggregations and the
	 * {@code $geoNear} aggregation command to emulate {@code geoNear} command functionality. We recommend using
	 * aggregations directly:
	 * </p>
	 *
	 * <pre class="code">
	 * TypedAggregation&lt;T&gt; geoNear = TypedAggregation.newAggregation(entityClass, Aggregation.geoNear(near, "dis"))
	 * 		.withOptions(AggregationOptions.builder().collation(near.getCollation()).build());
	 * AggregationResults&lt;Document&gt; results = aggregate(geoNear, Document.class);
	 * </pre>
	 *
	 * @param near must not be {@literal null}.
	 * @param entityClass must not be {@literal null}.
	 * @return
	 * @deprecated since 2.2. The {@code eval} command has been removed in MongoDB Server 4.2.0. Use Aggregations with
	 *             {@link Aggregation#geoNear(NearQuery, String)} instead.
	 */
	@Deprecated
	<T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass);

	/**
	 * Returns {@link GeoResults} for all entities matching the given {@link NearQuery}. Note, that MongoDB limits the
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
	 * AggregationResults&lt;Document&gt; results = aggregate(geoNear, Document.class);
	 * </pre>
	 *
	 * @param near must not be {@literal null}.
	 * @param entityClass must not be {@literal null}.
	 * @param collectionName the collection to trigger the query against. If no collection name is given the entity class
	 *          will be inspected. Must not be {@literal null} nor empty.
	 * @return
	 * @deprecated since 2.2. The {@code eval} command has been removed in MongoDB Server 4.2.0. Use Aggregations with
	 *             {@link Aggregation#geoNear(NearQuery, String)} instead.
	 */
	@Deprecated
	<T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass, String collectionName);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity class to a single instance of an object of the
	 * specified type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned list.
	 * @return the converted object.
	 */
	@Nullable
	<T> T findOne(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a single instance of an object of the specified
	 * type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned list.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @return the converted object.
	 */
	@Nullable
	<T> T findOne(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Determine result of given {@link Query} contains at least one element. <br />
	 * <strong>NOTE:</strong> Any additional support for query/field mapping, etc. is not available due to the lack of
	 * domain type information. Use {@link #exists(Query, Class, String)} to get full type specific support.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find a record.
	 * @param collectionName name of the collection to check for objects.
	 * @return {@literal true} if the query yields a result.
	 */
	boolean exists(Query query, String collectionName);

	/**
	 * Determine result of given {@link Query} contains at least one element.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find a record.
	 * @param entityClass the parametrized type.
	 * @return {@literal true} if the query yields a result.
	 */
	boolean exists(Query query, Class<?> entityClass);

	/**
	 * Determine result of given {@link Query} contains at least one element.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find a record.
	 * @param entityClass the parametrized type. Can be {@literal null}.
	 * @param collectionName name of the collection to check for objects.
	 * @return {@literal true} if the query yields a result.
	 */
	boolean exists(Query query, @Nullable Class<?> entityClass, String collectionName);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity class to a List of the specified type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param entityClass the parametrized type of the returned list. Must not be {@literal null}.
	 * @return the List of converted objects.
	 */
	<T> List<T> find(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a List of the specified type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification. Must not be {@literal null}.
	 * @param entityClass the parametrized type of the returned list. Must not be {@literal null}.
	 * @param collectionName name of the collection to retrieve the objects from. Must not be {@literal null}.
	 * @return the List of converted objects.
	 */
	<T> List<T> find(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Returns a document with the given id mapped onto the given class. The collection the query is ran against will be
	 * derived from the given target class as well.
	 *
	 * @param id the id of the document to return. Must not be {@literal null}.
	 * @param entityClass the type the document shall be converted into. Must not be {@literal null}.
	 * @return the document with the given id mapped onto the given target class.
	 */
	@Nullable
	<T> T findById(Object id, Class<T> entityClass);

	/**
	 * Returns the document with the given id from the given collection mapped onto the given target class.
	 *
	 * @param id the id of the document to return.
	 * @param entityClass the type to convert the document to.
	 * @param collectionName the collection to query for the document.
	 * @return he converted object or {@literal null} if document does not exist.
	 */
	@Nullable
	<T> T findById(Object id, Class<T> entityClass, String collectionName);

	/**
	 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view and
	 * returns the results in a {@link List}.
	 *
	 * @param field the name of the field to inspect for distinct values. Must not be {@literal null}.
	 * @param entityClass the domain type used for determining the actual {@link MongoCollection}. Must not be
	 *          {@literal null}.
	 * @param resultClass the result type. Must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	default <T> List<T> findDistinct(String field, Class<?> entityClass, Class<T> resultClass) {
		return findDistinct(new Query(), field, entityClass, resultClass);
	}

	/**
	 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view and
	 * returns the results in a {@link List}.
	 *
	 * @param query filter {@link Query} to restrict search. Must not be {@literal null}.
	 * @param field the name of the field to inspect for distinct values. Must not be {@literal null}.
	 * @param entityClass the domain type used for determining the actual {@link MongoCollection} and mapping the
	 *          {@link Query} to the domain type fields. Must not be {@literal null}.
	 * @param resultClass the result type. Must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	<T> List<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass);

	/**
	 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view and
	 * returns the results in a {@link List}.
	 *
	 * @param query filter {@link Query} to restrict search. Must not be {@literal null}.
	 * @param field the name of the field to inspect for distinct values. Must not be {@literal null}.
	 * @param collectionName the explicit name of the actual {@link MongoCollection}. Must not be {@literal null}.
	 * @param entityClass the domain type used for mapping the {@link Query} to the domain type fields.
	 * @param resultClass the result type. Must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	<T> List<T> findDistinct(Query query, String field, String collectionName, Class<?> entityClass,
			Class<T> resultClass);

	/**
	 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view and
	 * returns the results in a {@link List}.
	 *
	 * @param query filter {@link Query} to restrict search. Must not be {@literal null}.
	 * @param field the name of the field to inspect for distinct values. Must not be {@literal null}.
	 * @param collection the explicit name of the actual {@link MongoCollection}. Must not be {@literal null}.
	 * @param resultClass the result type. Must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	default <T> List<T> findDistinct(Query query, String field, String collection, Class<T> resultClass) {
		return findDistinct(query, field, collection, Object.class, resultClass);
	}

	/**
	 * Triggers <a href="https://docs.mongodb.org/manual/reference/method/db.collection.findAndModify/">findAndModify <a/>
	 * to apply provided {@link Update} on documents matching {@link Criteria} of given {@link Query}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} to apply on matching documents. Must not be {@literal null}.
	 * @param entityClass the parametrized type. Must not be {@literal null}.
	 * @return the converted object that was updated before it was updated or {@literal null}, if not found.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	@Nullable
	<T> T findAndModify(Query query, UpdateDefinition update, Class<T> entityClass);

	/**
	 * Triggers <a href="https://docs.mongodb.org/manual/reference/method/db.collection.findAndModify/">findAndModify <a/>
	 * to apply provided {@link Update} on documents matching {@link Criteria} of given {@link Query}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} to apply on matching documents. Must not be {@literal null}.
	 * @param entityClass the parametrized type. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the converted object that was updated before it was updated or {@literal null}, if not found.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	@Nullable
	<T> T findAndModify(Query query, UpdateDefinition update, Class<T> entityClass, String collectionName);

	/**
	 * Triggers <a href="https://docs.mongodb.org/manual/reference/method/db.collection.findAndModify/">findAndModify <a/>
	 * to apply provided {@link Update} on documents matching {@link Criteria} of given {@link Query} taking
	 * {@link FindAndModifyOptions} into account.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification.
	 * @param update the {@link UpdateDefinition} to apply on matching documents.
	 * @param options the {@link FindAndModifyOptions} holding additional information.
	 * @param entityClass the parametrized type.
	 * @return the converted object that was updated or {@literal null}, if not found. Depending on the value of
	 *         {@link FindAndModifyOptions#isReturnNew()} this will either be the object as it was before the update or as
	 *         it is after the update.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	@Nullable
	<T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass);

	/**
	 * Triggers <a href="https://docs.mongodb.org/manual/reference/method/db.collection.findAndModify/">findAndModify <a/>
	 * to apply provided {@link Update} on documents matching {@link Criteria} of given {@link Query} taking
	 * {@link FindAndModifyOptions} into account.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param update the {@link UpdateDefinition} to apply on matching documents. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @param entityClass the parametrized type. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the converted object that was updated or {@literal null}, if not found. Depending on the value of
	 *         {@link FindAndModifyOptions#isReturnNew()} this will either be the object as it was before the update or as
	 *         it is after the update.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	@Nullable
	<T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass,
			String collectionName);

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement}
	 * document. <br />
	 * The collection name is derived from the {@literal replacement} type. <br />
	 * Options are defaulted to {@link FindAndReplaceOptions#empty()}. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @return the converted object that was updated or {@literal null}, if not found.
	 * @since 2.1
	 */
	@Nullable
	default <T> T findAndReplace(Query query, T replacement) {
		return findAndReplace(query, replacement, FindAndReplaceOptions.empty());
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement}
	 * document.<br />
	 * Options are defaulted to {@link FindAndReplaceOptions#empty()}. <br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the converted object that was updated or {@literal null}, if not found.
	 * @since 2.1
	 */
	@Nullable
	default <T> T findAndReplace(Query query, T replacement, String collectionName) {
		return findAndReplace(query, replacement, FindAndReplaceOptions.empty(), collectionName);
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account.<br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @return the converted object that was updated or {@literal null}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @since 2.1
	 */
	@Nullable
	default <T> T findAndReplace(Query query, T replacement, FindAndReplaceOptions options) {
		return findAndReplace(query, replacement, options, getCollectionName(ClassUtils.getUserClass(replacement)));
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account.<br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @return the converted object that was updated or {@literal null}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @since 2.1
	 */
	@Nullable
	default <T> T findAndReplace(Query query, T replacement, FindAndReplaceOptions options, String collectionName) {

		Assert.notNull(replacement, "Replacement must not be null!");
		return findAndReplace(query, replacement, options, (Class<T>) ClassUtils.getUserClass(replacement), collectionName);
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account.<br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @param entityType the parametrized type. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @return the converted object that was updated or {@literal null}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @since 2.1
	 */
	@Nullable
	default <T> T findAndReplace(Query query, T replacement, FindAndReplaceOptions options, Class<T> entityType,
			String collectionName) {

		return findAndReplace(query, replacement, options, entityType, collectionName, entityType);
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account.<br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @param entityType the type used for mapping the {@link Query} to domain type fields and deriving the collection
	 *          from. Must not be {@literal null}.
	 * @param resultType the parametrized type projection return type. Must not be {@literal null}, use the domain type of
	 *          {@code Object.class} instead.
	 * @return the converted object that was updated or {@literal null}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @since 2.1
	 */
	@Nullable
	default <S, T> T findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType,
			Class<T> resultType) {

		return findAndReplace(query, replacement, options, entityType,
				getCollectionName(ClassUtils.getUserClass(entityType)), resultType);
	}

	/**
	 * Triggers
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>
	 * to replace a single document matching {@link Criteria} of given {@link Query} with the {@code replacement} document
	 * taking {@link FindAndReplaceOptions} into account.<br />
	 * <strong>NOTE:</strong> The replacement entity must not hold an {@literal id}.
	 *
	 * @param query the {@link Query} class that specifies the {@link Criteria} used to find a record and also an optional
	 *          fields specification. Must not be {@literal null}.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @param entityType the type used for mapping the {@link Query} to domain type fields. Must not be {@literal null}.
	 * @param collectionName the collection to query. Must not be {@literal null}.
	 * @param resultType the parametrized type projection return type. Must not be {@literal null}, use the domain type of
	 *          {@code Object.class} instead.
	 * @return the converted object that was updated or {@literal null}, if not found. Depending on the value of
	 *         {@link FindAndReplaceOptions#isReturnNew()} this will either be the object as it was before the update or
	 *         as it is after the update.
	 * @since 2.1
	 */
	@Nullable
	<S, T> T findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType,
			String collectionName, Class<T> resultType);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity type to a single instance of an object of the
	 * specified type. The first document that matches the query is returned and also removed from the collection in the
	 * database.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned list.
	 * @return the converted object
	 */
	@Nullable
	<T> T findAndRemove(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a single instance of an object of the specified
	 * type. The first document that matches the query is returned and also removed from the collection in the database.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 *
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification.
	 * @param entityClass the parametrized type of the returned list.
	 * @param collectionName name of the collection to retrieve the objects from.
	 * @return the converted object.
	 */
	@Nullable
	<T> T findAndRemove(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Returns the number of documents for the given {@link Query} by querying the collection of the given entity class.
	 * <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents. Must not be
	 *          {@literal null}.
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @return the count of matching documents.
	 */
	long count(Query query, Class<?> entityClass);

	/**
	 * Returns the number of documents for the given {@link Query} querying the given collection. The given {@link Query}
	 * must solely consist of document field references as we lack type information to map potential property references
	 * onto document fields. Use {@link #count(Query, Class, String)} to get full type specific support. <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents.
	 * @param collectionName must not be {@literal null} or empty.
	 * @return the count of matching documents.
	 * @see #count(Query, Class, String)
	 */
	long count(Query query, String collectionName);

	/**
	 * Returns the number of documents for the given {@link Query} by querying the given collection using the given entity
	 * class to map the given {@link Query}. <br />
	 * <strong>NOTE:</strong> Query {@link Query#getSkip() offset} and {@link Query#getLimit() limit} can have direct
	 * influence on the resulting number of documents found as those values are passed on to the server and potentially
	 * limit the range and order within which the server performs the count operation. Use an {@literal unpaged} query to
	 * count all matches.
	 *
	 * @param query the {@link Query} class that specifies the criteria used to find documents. Must not be
	 *          {@literal null}.
	 * @param entityClass the parametrized type. Can be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 * @return the count of matching documents.
	 */
	long count(Query query, @Nullable Class<?> entityClass, String collectionName);

	/**
	 * Insert the object into the collection for the entity type of the object to save.
	 * <p/>
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}.
	 * <p/>
	 * If your object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Type Conversion API. See
	 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation" > Spring's
	 * Type Conversion"</a> for more details.
	 * <p/>
	 * <p/>
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @return the inserted object.
	 */
	<T> T insert(T objectToSave);

	/**
	 * Insert the object into the specified collection.
	 * <p/>
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @param collectionName name of the collection to store the object in. Must not be {@literal null}.
	 * @return the inserted object.
	 */
	<T> T insert(T objectToSave, String collectionName);

	/**
	 * Insert a Collection of objects into a collection in a single batch write to the database.
	 *
	 * @param batchToSave the batch of objects to save. Must not be {@literal null}.
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @return the inserted objects that.
	 */
	<T> Collection<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass);

	/**
	 * Insert a batch of objects into the specified collection in a single batch write to the database.
	 *
	 * @param batchToSave the list of objects to save. Must not be {@literal null}.
	 * @param collectionName name of the collection to store the object in. Must not be {@literal null}.
	 * @return the inserted objects that.
	 */
	<T> Collection<T> insert(Collection<? extends T> batchToSave, String collectionName);

	/**
	 * Insert a mixed Collection of objects into a database collection determining the collection name to use based on the
	 * class.
	 *
	 * @param objectsToSave the list of objects to save. Must not be {@literal null}.
	 * @return the inserted objects.
	 */
	<T> Collection<T> insertAll(Collection<? extends T> objectsToSave);

	/**
	 * Save the object to the collection for the entity type of the object to save. This will perform an insert if the
	 * object is not already present, that is an 'upsert'.
	 * <p/>
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * If your object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Type Conversion API. See
	 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation" > Spring's
	 * Type Conversion"</a> for more details.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @return the saved object.
	 */
	<T> T save(T objectToSave);

	/**
	 * Save the object to the specified collection. This will perform an insert if the object is not already present, that
	 * is an 'upsert'.
	 * <p/>
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of {@link MappingMongoConverter} will be used.
	 * <p/>
	 * If your object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Type Conversion API. See <a
	 * https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#validation">Spring's Type
	 * Conversion"</a> for more details.
	 *
	 * @param objectToSave the object to store in the collection. Must not be {@literal null}.
	 * @param collectionName name of the collection to store the object in. Must not be {@literal null}.
	 * @return the saved object.
	 */
	<T> T save(T objectToSave, String collectionName);

	/**
	 * Performs an upsert. If no document is found that matches the query, a new document is created and inserted by
	 * combining the query document and the update document. <br />
	 * <strong>NOTE:</strong> {@link Query#getSortObject() sorting} is not supported by {@code db.collection.updateOne}.
	 * Use {@link #findAndModify(Query, UpdateDefinition, FindAndModifyOptions, Class, String)} instead.
	 *
	 * @param query the query document that specifies the criteria used to select a record to be upserted. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing object. Must not be {@literal null}.
	 * @param entityClass class that determines the collection to use. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	UpdateResult upsert(Query query, UpdateDefinition update, Class<?> entityClass);

	/**
	 * Performs an upsert. If no document is found that matches the query, a new document is created and inserted by
	 * combining the query document and the update document. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping, versions, etc. is not available due to the lack of
	 * domain type information. Use {@link #upsert(Query, UpdateDefinition, Class, String)} to get full type specific
	 * support. <br />
	 * <strong>NOTE:</strong> {@link Query#getSortObject() sorting} is not supported by {@code db.collection.updateOne}.
	 * Use {@link #findAndModify(Query, UpdateDefinition, FindAndModifyOptions, Class, String)} instead.
	 *
	 * @param query the query document that specifies the criteria used to select a record to be upserted. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing object. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	UpdateResult upsert(Query query, UpdateDefinition update, String collectionName);

	/**
	 * Performs an upsert. If no document is found that matches the query, a new document is created and inserted by
	 * combining the query document and the update document.
	 *
	 * @param query the query document that specifies the criteria used to select a record to be upserted. Must not be
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
	UpdateResult upsert(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName);

	/**
	 * Updates the first object that is found in the collection of the entity class that matches the query document with
	 * the provided update document.
	 *
	 * @param query the query document that specifies the criteria used to select a record to be updated. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param entityClass class that determines the collection to use.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> entityClass);

	/**
	 * Updates the first object that is found in the specified collection that matches the query document criteria with
	 * the provided updated document. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping, versions, etc. is not available due to the lack of
	 * domain type information. Use {@link #updateFirst(Query, UpdateDefinition, Class, String)} to get full type specific
	 * support. <br />
	 * <strong>NOTE:</strong> {@link Query#getSortObject() sorting} is not supported by {@code db.collection.updateOne}.
	 * Use {@link #findAndModify(Query, UpdateDefinition, Class, String)} instead.
	 *
	 * @param query the query document that specifies the criteria used to select a record to be updated. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	UpdateResult updateFirst(Query query, UpdateDefinition update, String collectionName);

	/**
	 * Updates the first object that is found in the specified collection that matches the query document criteria with
	 * the provided updated document. <br />
	 *
	 * @param query the query document that specifies the criteria used to select a record to be updated. Must not be
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
	UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName);

	/**
	 * Updates all objects that are found in the collection for the entity class that matches the query document criteria
	 * with the provided updated document.
	 *
	 * @param query the query document that specifies the criteria used to select a record to be updated. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param entityClass class of the pojo to be operated on. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	UpdateResult updateMulti(Query query, UpdateDefinition update, Class<?> entityClass);

	/**
	 * Updates all objects that are found in the specified collection that matches the query document criteria with the
	 * provided updated document. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping, versions, etc. is not available due to the lack of
	 * domain type information. Use {@link #updateMulti(Query, UpdateDefinition, Class, String)} to get full type specific
	 * support.
	 *
	 * @param query the query document that specifies the criteria used to select a record to be updated. Must not be
	 *          {@literal null}.
	 * @param update the {@link UpdateDefinition} that contains the updated object or {@code $} operators to manipulate
	 *          the existing. Must not be {@literal null}.
	 * @param collectionName name of the collection to update the object in. Must not be {@literal null}.
	 * @return the {@link UpdateResult} which lets you access the results of the previous write.
	 * @since 3.0
	 * @see Update
	 * @see AggregationUpdate
	 */
	UpdateResult updateMulti(Query query, UpdateDefinition update, String collectionName);

	/**
	 * Updates all objects that are found in the collection for the entity class that matches the query document criteria
	 * with the provided updated document.
	 *
	 * @param query the query document that specifies the criteria used to select a record to be updated. Must not be
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
	UpdateResult updateMulti(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName);

	/**
	 * Remove the given object from the collection by {@literal id} and (if applicable) its
	 * {@link org.springframework.data.annotation.Version}. <br />
	 * Use {@link DeleteResult#getDeletedCount()} for insight whether an {@link DeleteResult#wasAcknowledged()
	 * acknowledged} remove operation was successful or not.
	 *
	 * @param object must not be {@literal null}.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 */
	DeleteResult remove(Object object);

	/**
	 * Removes the given object from the given collection by {@literal id} and (if applicable) its
	 * {@link org.springframework.data.annotation.Version}. <br />
	 * Use {@link DeleteResult#getDeletedCount()} for insight whether an {@link DeleteResult#wasAcknowledged()
	 * acknowledged} remove operation was successful or not.
	 *
	 * @param object must not be {@literal null}.
	 * @param collectionName name of the collection where the objects will removed, must not be {@literal null} or empty.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 */
	DeleteResult remove(Object object, String collectionName);

	/**
	 * Remove all documents that match the provided query document criteria from the the collection used to store the
	 * entityClass. The Class parameter is also used to help convert the Id of the object if it is present in the query.
	 *
	 * @param query the query document that specifies the criteria used to remove a record.
	 * @param entityClass class that determines the collection to use.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 * @throws IllegalArgumentException when {@literal query} or {@literal entityClass} is {@literal null}.
	 */
	DeleteResult remove(Query query, Class<?> entityClass);

	/**
	 * Remove all documents that match the provided query document criteria from the the collection used to store the
	 * entityClass. The Class parameter is also used to help convert the Id of the object if it is present in the query.
	 *
	 * @param query the query document that specifies the criteria used to remove a record.
	 * @param entityClass class of the pojo to be operated on. Can be {@literal null}.
	 * @param collectionName name of the collection where the objects will removed, must not be {@literal null} or empty.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 * @throws IllegalArgumentException when {@literal query}, {@literal entityClass} or {@literal collectionName} is
	 *           {@literal null}.
	 */
	DeleteResult remove(Query query, Class<?> entityClass, String collectionName);

	/**
	 * Remove all documents from the specified collection that match the provided query document criteria. There is no
	 * conversion/mapping done for any criteria using the id field. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping is not available due to the lack of domain type
	 * information. Use {@link #remove(Query, Class, String)} to get full type specific support.
	 *
	 * @param query the query document that specifies the criteria used to remove a record.
	 * @param collectionName name of the collection where the objects will removed, must not be {@literal null} or empty.
	 * @return the {@link DeleteResult} which lets you access the results of the previous delete.
	 * @throws IllegalArgumentException when {@literal query} or {@literal collectionName} is {@literal null}.
	 */
	DeleteResult remove(Query query, String collectionName);

	/**
	 * Returns and removes all documents form the specified collection that match the provided query. <br />
	 * <strong>NOTE:</strong> Any additional support for field mapping is not available due to the lack of domain type
	 * information. Use {@link #findAllAndRemove(Query, Class, String)} to get full type specific support.
	 *
	 * @param query the query document that specifies the criteria used to find and remove documents.
	 * @param collectionName name of the collection where the objects will removed, must not be {@literal null} or empty.
	 * @return the {@link List} converted objects deleted by this operation.
	 * @since 1.5
	 */
	<T> List<T> findAllAndRemove(Query query, String collectionName);

	/**
	 * Returns and removes all documents matching the given query form the collection used to store the entityClass.
	 *
	 * @param query the query document that specifies the criteria used to find and remove documents.
	 * @param entityClass class of the pojo to be operated on.
	 * @return the {@link List} converted objects deleted by this operation.
	 * @since 1.5
	 */
	<T> List<T> findAllAndRemove(Query query, Class<T> entityClass);

	/**
	 * Returns and removes all documents that match the provided query document criteria from the the collection used to
	 * store the entityClass. The Class parameter is also used to help convert the Id of the object if it is present in
	 * the query.
	 *
	 * @param query the query document that specifies the criteria used to find and remove documents.
	 * @param entityClass class of the pojo to be operated on.
	 * @param collectionName name of the collection where the objects will removed, must not be {@literal null} or empty.
	 * @return the {@link List} converted objects deleted by this operation.
	 * @since 1.5
	 */
	<T> List<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Returns the underlying {@link MongoConverter}.
	 *
	 * @return
	 */
	MongoConverter getConverter();
}
