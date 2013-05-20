/*
 * Copyright 2010-2013 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.geo.GeoResult;
import org.springframework.data.mongodb.core.geo.GeoResults;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

/**
 * Interface that specifies a basic set of MongoDB operations. Implemented by {@link MongoTemplate}. Not often used but
 * a useful option for extensibility and testability (as it can be easily mocked, stubbed, or be the target of a JDK
 * proxy).
 * 
 * @author Thomas Risberg
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public interface MongoOperations {

	/**
	 * The collection name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getCollectionName(Class<?> entityClass);

	/**
	 * Execute the a MongoDB command expressed as a JSON string. This will call the method JSON.parse that is part of the
	 * MongoDB driver to convert the JSON string to a DBObject. Any errors that result from executing this command will be
	 * converted into Spring's DAO exception hierarchy.
	 * 
	 * @param jsonCommand a MongoDB command expressed as a JSON string.
	 */
	CommandResult executeCommand(String jsonCommand);

	/**
	 * Execute a MongoDB command. Any errors that result from executing this command will be converted into Spring's DAO
	 * exception hierarchy.
	 * 
	 * @param command a MongoDB command
	 */
	CommandResult executeCommand(DBObject command);

	/**
	 * Execute a MongoDB command. Any errors that result from executing this command will be converted into Spring's DAO
	 * exception hierarchy.
	 * 
	 * @param command a MongoDB command
	 * @param options query options to use
	 */
	CommandResult executeCommand(DBObject command, int options);

	/**
	 * Execute a MongoDB query and iterate over the query results on a per-document basis with a DocumentCallbackHandler.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param dch the handler that will extract results, one document at a time
	 */
	void executeQuery(Query query, String collectionName, DocumentCallbackHandler dch);

	/**
	 * Executes a {@link DbCallback} translating any exceptions as necessary.
	 * <p/>
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * 
	 * @param <T> return type
	 * @param action callback object that specifies the MongoDB actions to perform on the passed in DB instance.
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(DbCallback<T> action);

	/**
	 * Executes the given {@link CollectionCallback} on the entity collection of the specified class.
	 * <p/>
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * 
	 * @param entityClass class that determines the collection to use
	 * @param <T> return type
	 * @param action callback object that specifies the MongoDB action
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(Class<?> entityClass, CollectionCallback<T> action);

	/**
	 * Executes the given {@link CollectionCallback} on the collection of the given name.
	 * <p/>
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * 
	 * @param <T> return type
	 * @param collectionName the name of the collection that specifies which DBCollection instance will be passed into
	 * @param action callback object that specifies the MongoDB action the callback action.
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(String collectionName, CollectionCallback<T> action);

	/**
	 * Executes the given {@link DbCallback} within the same connection to the database so as to ensure consistency in a
	 * write heavy environment where you may read the data that you wrote. See the comments on {@see <a
	 * href=http://www.mongodb.org/display/DOCS/Java+Driver+Concurrency>Java Driver Concurrency</a>}
	 * <p/>
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * 
	 * @param <T> return type
	 * @param action callback that specified the MongoDB actions to perform on the DB instance
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T executeInSession(DbCallback<T> action);

	/**
	 * Create an uncapped collection with a name based on the provided entity class.
	 * 
	 * @param entityClass class that determines the collection to create
	 * @return the created collection
	 */
	<T> DBCollection createCollection(Class<T> entityClass);

	/**
	 * Create a collect with a name based on the provided entity class using the options.
	 * 
	 * @param entityClass class that determines the collection to create
	 * @param collectionOptions options to use when creating the collection.
	 * @return the created collection
	 */
	<T> DBCollection createCollection(Class<T> entityClass, CollectionOptions collectionOptions);

	/**
	 * Create an uncapped collection with the provided name.
	 * 
	 * @param collectionName name of the collection
	 * @return the created collection
	 */
	DBCollection createCollection(String collectionName);

	/**
	 * Create a collect with the provided name and options.
	 * 
	 * @param collectionName name of the collection
	 * @param collectionOptions options to use when creating the collection.
	 * @return the created collection
	 */
	DBCollection createCollection(String collectionName, CollectionOptions collectionOptions);

	/**
	 * A set of collection names.
	 * 
	 * @return list of collection names
	 */
	Set<String> getCollectionNames();

	/**
	 * Get a collection by name, creating it if it doesn't exist.
	 * <p/>
	 * Translate any exceptions as necessary.
	 * 
	 * @param collectionName name of the collection
	 * @return an existing collection or a newly created one.
	 */
	DBCollection getCollection(String collectionName);

	/**
	 * Check to see if a collection with a name indicated by the entity class exists.
	 * <p/>
	 * Translate any exceptions as necessary.
	 * 
	 * @param entityClass class that determines the name of the collection
	 * @return true if a collection with the given name is found, false otherwise.
	 */
	<T> boolean collectionExists(Class<T> entityClass);

	/**
	 * Check to see if a collection with a given name exists.
	 * <p/>
	 * Translate any exceptions as necessary.
	 * 
	 * @param collectionName name of the collection
	 * @return true if a collection with the given name is found, false otherwise.
	 */
	boolean collectionExists(String collectionName);

	/**
	 * Drop the collection with the name indicated by the entity class.
	 * <p/>
	 * Translate any exceptions as necessary.
	 * 
	 * @param entityClass class that determines the collection to drop/delete.
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
	 * Query for a list of objects of type T from the collection used by the entity class.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient way
	 * to map objects since the test for class type is done in the client and not on the server.
	 * 
	 * @param entityClass the parameterized type of the returned list
	 * @return the converted collection
	 */
	<T> List<T> findAll(Class<T> entityClass);

	/**
	 * Query for a list of objects of type T from the specified collection.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient way
	 * to map objects since the test for class type is done in the client and not on the server.
	 * 
	 * @param entityClass the parameterized type of the returned list.
	 * @param collectionName name of the collection to retrieve the objects from
	 * @return the converted collection
	 */
	<T> List<T> findAll(Class<T> entityClass, String collectionName);

	/**
	 * Execute a group operation over the entire collection. The group operation entity class should match the 'shape' of
	 * the returned object that takes int account the initial document structure as well as any finalize functions.
	 * 
	 * @param criteria The criteria that restricts the row that are considered for grouping. If not specified all rows are
	 *          considered.
	 * @param inputCollectionName the collection where the group operation will read from
	 * @param groupBy the conditions under which the group operation will be performed, e.g. keys, initial document,
	 *          reduce function.
	 * @param entityClass The parameterized type of the returned list
	 * @return The results of the group operation
	 */
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
	 * @param entityClass The parameterized type of the returned list
	 * @return The results of the group operation
	 */
	<T> GroupByResults<T> group(Criteria criteria, String inputCollectionName, GroupBy groupBy, Class<T> entityClass);

	/**
	 * Execute a map-reduce operation. The map-reduce operation will be formed with an output type of INLINE
	 * 
	 * @param inputCollectionName the collection where the map-reduce will read from
	 * @param mapFunction The JavaScript map function
	 * @param reduceFunction The JavaScript reduce function
	 * @param mapReduceOptions Options that specify detailed map-reduce behavior
	 * @param entityClass The parameterized type of the returned list
	 * @return The results of the map reduce operation
	 */
	<T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			Class<T> entityClass);

	/**
	 * Execute a map-reduce operation that takes additional map-reduce options.
	 * 
	 * @param inputCollectionName the collection where the map-reduce will read from
	 * @param mapFunction The JavaScript map function
	 * @param reduceFunction The JavaScript reduce function
	 * @param mapReduceOptions Options that specify detailed map-reduce behavior
	 * @param entityClass The parameterized type of the returned list
	 * @return The results of the map reduce operation
	 */
	<T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			MapReduceOptions mapReduceOptions, Class<T> entityClass);

	/**
	 * Execute a map-reduce operation that takes a query. The map-reduce operation will be formed with an output type of
	 * INLINE
	 * 
	 * @param query The query to use to select the data for the map phase
	 * @param inputCollectionName the collection where the map-reduce will read from
	 * @param mapFunction The JavaScript map function
	 * @param reduceFunction The JavaScript reduce function
	 * @param mapReduceOptions Options that specify detailed map-reduce behavior
	 * @param entityClass The parameterized type of the returned list
	 * @return The results of the map reduce operation
	 */
	<T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction, String reduceFunction,
			Class<T> entityClass);

	/**
	 * Execute a map-reduce operation that takes a query and additional map-reduce options
	 * 
	 * @param query The query to use to select the data for the map phase
	 * @param inputCollectionName the collection where the map-reduce will read from
	 * @param mapFunction The JavaScript map function
	 * @param reduceFunction The JavaScript reduce function
	 * @param mapReduceOptions Options that specify detailed map-reduce behavior
	 * @param entityClass The parameterized type of the returned list
	 * @return The results of the map reduce operation
	 */
	<T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction, String reduceFunction,
			MapReduceOptions mapReduceOptions, Class<T> entityClass);

	/**
	 * Returns {@link GeoResult} for all entities matching the given {@link NearQuery}. Will consider entity mapping
	 * information to determine the collection the query is ran against.
	 * 
	 * @param near must not be {@literal null}.
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	<T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass);

	/**
	 * Returns {@link GeoResult} for all entities matching the given {@link NearQuery}.
	 * 
	 * @param near must not be {@literal null}.
	 * @param entityClass must not be {@literal null}.
	 * @param collectionName the collection to trigger the query against. If no collection name is given the entity class
	 *          will be inspected.
	 * @return
	 */
	<T> GeoResults<T> geoNear(NearQuery near, Class<T> entityClass, String collectionName);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity class to a single instance of an object of the
	 * specified type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification
	 * @param entityClass the parameterized type of the returned list.
	 * @return the converted object
	 */
	<T> T findOne(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a single instance of an object of the specified
	 * type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification
	 * @param entityClass the parameterized type of the returned list.
	 * @param collectionName name of the collection to retrieve the objects from
	 * @return the converted object
	 */
	<T> T findOne(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Map the results of an ad-hoc query on the collection for the entity class to a List of the specified type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification
	 * @param entityClass the parameterized type of the returned list.
	 * @return the List of converted objects
	 */
	<T> List<T> find(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a List of the specified type.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification
	 * @param entityClass the parameterized type of the returned list.
	 * @param collectionName name of the collection to retrieve the objects from
	 * @return the List of converted objects
	 */
	<T> List<T> find(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Returns a document with the given id mapped onto the given class. The collection the query is ran against will be
	 * derived from the given target class as well.
	 * 
	 * @param <T>
	 * @param id the id of the document to return.
	 * @param entityClass the type the document shall be converted into.
	 * @return the document with the given id mapped onto the given target class.
	 */
	<T> T findById(Object id, Class<T> entityClass);

	/**
	 * Returns the document with the given id from the given collection mapped onto the given target class.
	 * 
	 * @param id the id of the document to return
	 * @param entityClass the type to convert the document to
	 * @param collectionName the collection to query for the document
	 * @param <T>
	 * @return
	 */
	<T> T findById(Object id, Class<T> entityClass, String collectionName);

	<T> T findAndModify(Query query, Update update, Class<T> entityClass);

	<T> T findAndModify(Query query, Update update, Class<T> entityClass, String collectionName);

	<T> T findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass);

	<T> T findAndModify(Query query, Update update, FindAndModifyOptions options, Class<T> entityClass,
			String collectionName);

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
	 *          specification
	 * @param entityClass the parameterized type of the returned list.
	 * @return the converted object
	 */
	<T> T findAndRemove(Query query, Class<T> entityClass);

	/**
	 * Map the results of an ad-hoc query on the specified collection to a single instance of an object of the specified
	 * type. The first document that matches the query is returned and also removed from the collection in the database.
	 * <p/>
	 * The object is converted from the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * The query is specified as a {@link Query} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields
	 *          specification
	 * @param entityClass the parameterized type of the returned list.
	 * @param collectionName name of the collection to retrieve the objects from
	 * @return the converted object
	 */
	<T> T findAndRemove(Query query, Class<T> entityClass, String collectionName);

	/**
	 * Returns the number of documents for the given {@link Query} by querying the collection of the given entity class.
	 * 
	 * @param query
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	long count(Query query, Class<?> entityClass);

	/**
	 * Returns the number of documents for the given {@link Query} querying the given collection.
	 * 
	 * @param query
	 * @param collectionName must not be {@literal null} or empty.
	 * @return
	 */
	long count(Query query, String collectionName);

	/**
	 * Insert the object into the collection for the entity type of the object to save.
	 * <p/>
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}.
	 * <p/>
	 * If you object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Spring 3.0's new Type Conversion API.
	 * See <a href="http://static.springsource.org/spring/docs/3.0.x/reference/validation.html#core-convert">Spring 3 Type
	 * Conversion"</a> for more details.
	 * <p/>
	 * <p/>
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 * 
	 * @param objectToSave the object to store in the collection.
	 */
	void insert(Object objectToSave);

	/**
	 * Insert the object into the specified collection.
	 * <p/>
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * Insert is used to initially store the object into the database. To update an existing object use the save method.
	 * 
	 * @param objectToSave the object to store in the collection
	 * @param collectionName name of the collection to store the object in
	 */
	void insert(Object objectToSave, String collectionName);

	/**
	 * Insert a Collection of objects into a collection in a single batch write to the database.
	 * 
	 * @param batchToSave the list of objects to save.
	 * @param entityClass class that determines the collection to use
	 */
	void insert(Collection<? extends Object> batchToSave, Class<?> entityClass);

	/**
	 * Insert a list of objects into the specified collection in a single batch write to the database.
	 * 
	 * @param batchToSave the list of objects to save.
	 * @param collectionName name of the collection to store the object in
	 */
	void insert(Collection<? extends Object> batchToSave, String collectionName);

	/**
	 * Insert a mixed Collection of objects into a database collection determining the collection name to use based on the
	 * class.
	 * 
	 * @param collectionToSave the list of objects to save.
	 */
	void insertAll(Collection<? extends Object> objectsToSave);

	/**
	 * Save the object to the collection for the entity type of the object to save. This will perform an insert if the
	 * object is not already present, that is an 'upsert'.
	 * <p/>
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * If you object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Spring 3.0's new Type Conversion API.
	 * See <a href="http://static.springsource.org/spring/docs/3.0.x/reference/validation.html#core-convert">Spring 3 Type
	 * Conversion"</a> for more details.
	 * 
	 * @param objectToSave the object to store in the collection
	 */
	void save(Object objectToSave);

	/**
	 * Save the object to the specified collection. This will perform an insert if the object is not already present, that
	 * is an 'upsert'.
	 * <p/>
	 * The object is converted to the MongoDB native representation using an instance of {@see MongoConverter}. Unless
	 * configured otherwise, an instance of SimpleMongoConverter will be used.
	 * <p/>
	 * If you object has an "Id' property, it will be set with the generated Id from MongoDB. If your Id property is a
	 * String then MongoDB ObjectId will be used to populate that string. Otherwise, the conversion from ObjectId to your
	 * property type will be handled by Spring's BeanWrapper class that leverages Spring 3.0's new Type Cobnversion API.
	 * See <a href="http://static.springsource.org/spring/docs/3.0.x/reference/validation.html#core-convert">Spring 3 Type
	 * Conversion"</a> for more details.
	 * 
	 * @param objectToSave the object to store in the collection
	 * @param collectionName name of the collection to store the object in
	 */
	void save(Object objectToSave, String collectionName);

	/**
	 * Performs an upsert. If no document is found that matches the query, a new document is created and inserted by
	 * combining the query document and the update document.
	 * 
	 * @param query the query document that specifies the criteria used to select a record to be upserted
	 * @param update the update document that contains the updated object or $ operators to manipulate the existing object
	 * @param entityClass class that determines the collection to use
	 * @return the WriteResult which lets you access the results of the previous write.
	 */
	WriteResult upsert(Query query, Update update, Class<?> entityClass);

	/**
	 * Performs an upsert. If no document is found that matches the query, a new document is created and inserted by
	 * combining the query document and the update document.
	 * 
	 * @param query the query document that specifies the criteria used to select a record to be updated
	 * @param update the update document that contains the updated object or $ operators to manipulate the existing
	 *          object.
	 * @param collectionName name of the collection to update the object in
	 * @return the WriteResult which lets you access the results of the previous write.
	 */
	WriteResult upsert(Query query, Update update, String collectionName);

	/**
	 * Updates the first object that is found in the collection of the entity class that matches the query document with
	 * the provided update document.
	 * 
	 * @param query the query document that specifies the criteria used to select a record to be updated
	 * @param update the update document that contains the updated object or $ operators to manipulate the existing
	 *          object.
	 * @param entityClass class that determines the collection to use
	 * @return the WriteResult which lets you access the results of the previous write.
	 */
	WriteResult updateFirst(Query query, Update update, Class<?> entityClass);

	/**
	 * Updates the first object that is found in the specified collection that matches the query document criteria with
	 * the provided updated document.
	 * 
	 * @param query the query document that specifies the criteria used to select a record to be updated
	 * @param update the update document that contains the updated object or $ operators to manipulate the existing
	 *          object.
	 * @param collectionName name of the collection to update the object in
	 * @return the WriteResult which lets you access the results of the previous write.
	 */
	WriteResult updateFirst(Query query, Update update, String collectionName);

	/**
	 * Updates all objects that are found in the collection for the entity class that matches the query document criteria
	 * with the provided updated document.
	 * 
	 * @param query the query document that specifies the criteria used to select a record to be updated
	 * @param update the update document that contains the updated object or $ operators to manipulate the existing
	 *          object.
	 * @param entityClass class that determines the collection to use
	 * @return the WriteResult which lets you access the results of the previous write.
	 */
	WriteResult updateMulti(Query query, Update update, Class<?> entityClass);

	/**
	 * Updates all objects that are found in the specified collection that matches the query document criteria with the
	 * provided updated document.
	 * 
	 * @param query the query document that specifies the criteria used to select a record to be updated
	 * @param update the update document that contains the updated object or $ operators to manipulate the existing
	 *          object.
	 * @param collectionName name of the collection to update the object in
	 * @return the WriteResult which lets you access the results of the previous write.
	 */
	WriteResult updateMulti(Query query, Update update, String collectionName);

	/**
	 * Remove the given object from the collection by id.
	 * 
	 * @param object
	 */
	void remove(Object object);

	/**
	 * Removes the given object from the given collection.
	 * 
	 * @param object
	 * @param collection must not be {@literal null} or empty.
	 */
	void remove(Object object, String collection);

	/**
	 * Remove all documents that match the provided query document criteria from the the collection used to store the
	 * entityClass. The Class parameter is also used to help convert the Id of the object if it is present in the query.
	 * 
	 * @param query
	 * @param entityClass
	 */
	void remove(Query query, Class<?> entityClass);

	void remove(Query query, Class<?> entityClass, String collectionName);

	/**
	 * Remove all documents from the specified collection that match the provided query document criteria. There is no
	 * conversion/mapping done for any criteria using the id field.
	 * 
	 * @param query the query document that specifies the criteria used to remove a record
	 * @param collectionName name of the collection where the objects will removed
	 */
	void remove(Query query, String collectionName);

	/**
	 * Returns the underlying {@link MongoConverter}.
	 * 
	 * @return
	 */
	MongoConverter getConverter();
}
