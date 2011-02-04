/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.document.mongodb;

import java.util.List;
import java.util.Set;

import org.springframework.data.document.mongodb.builder.QueryDefinition;
import org.springframework.data.document.mongodb.builder.UpdateDefinition;

import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Interface that specifies a basic set of MongoDB operations.  Implemented by {@link MongoTemplate}.
 * Not often used but a useful option for extensibility and testability (as it can be easily mocked, stubbed, or be
 * the target of a JDK proxy).
 * 
 * @author Thomas Risberg
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public interface MongoOperations {

	/**
	 * The default collection name used by this template.
	 * @return
	 */
	String getDefaultCollectionName();

	/**
	 * The default collection used by this template.
	 * @return The default collection used by this template
	 */
	DBCollection getDefaultCollection();
	
	/**
	 * Execute the a MongoDB command expressed as a JSON string.  This will call the method 
	 * JSON.parse that is part of the MongoDB driver to convert the JSON string to a DBObject.  
	 * Any errors that result from executing this command will be converted into Spring's DAO
	 * exception hierarchy.
	 * @param jsonCommand a MongoDB command expressed as a JSON string. 
	 */
	CommandResult executeCommand(String jsonCommand);

	/**
	 * Execute a MongoDB command.  Any errors that result from executing this command will be converted
	 * into Spring's DAO exception hierarchy.
	 * @param command a MongoDB command
	 */
	CommandResult executeCommand(DBObject command);

	/**
	 * Executes a {@link DbCallback} translating any exceptions as necessary.
	 * 
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * 
	 * @param <T> return type
	 * @param action callback object that specifies the MongoDB actions to perform on the passed in DB instance.
	 *
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(DbCallback<T> action);

	/**
	 * Executes the given {@link CollectionCallback} on the default collection.
	 * 
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * 
	 * @param <T> return type
	 * @param action callback object that specifies the MongoDB action  
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(CollectionCallback<T> action);

	/**
	 * Executes the given {@link CollectionCallback} on the collection of the given name.
	 * 
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * 
	 * @param <T> return type
	 * @param action callback object that specifies the MongoDB action  
	 * @param collectionName the name of the collection that specifies which DBCollection instance will be passed into 
	 * the callback action.
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(CollectionCallback<T> action, String collectionName);

	/**
	 * Executes the given {@link DbCallback} within the same connection to the database so as to ensure 
	 * consistency in a write heavy environment where you may read the data that you wrote.  See the 
	 * comments on {@see <a href=http://www.mongodb.org/display/DOCS/Java+Driver+Concurrency>Java Driver Concurrency</a>}
	 * 
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * 
	 * @param <T> return type
	 * @param action callback that specified the MongoDB actions to perform on the DB instance
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T executeInSession(DbCallback<T> action);

	/**
	 * Create an uncapped collection with the provided name.
	 * @param collectionName name of the collection
	 * @return the created collection
	 */
	DBCollection createCollection(String collectionName);

	/**
	 * Create a collect with the provided name and options.
	 * @param collectionName name of the collection
	 * @param collectionOptions options to use when creating the collection.
	 */
	void createCollection(String collectionName, CollectionOptions collectionOptions);

	/**
	 * A set of collection names.
	 * @return list of collection names
	 */
	Set<String> getCollectionNames();
	
	/**
	 * Get a collection by name, creating it if it doesn't exist.
	 * 
	 * Translate any exceptions as necessary.
	 * 
	 * @param collectionName name of the collection
	 * @return an existing collection or a newly created one.
	 */
	DBCollection getCollection(String collectionName);

	/**
	 * Check to see if a collection with a given name exists.
	 * 
	 * Translate any exceptions as necessary.
	 *  
	 * @param collectionName name of the collection
	 * @return true if a collection with the given name is found, false otherwise. 
	 */
	boolean collectionExists(String collectionName);

	/**
	 * Drop the collection with the given name.
	 * 
	 * Translate any exceptions as necessary.
	 * 
	 * @param collectionName name of the collection to drop/delete.
	 */
	void dropCollection(String collectionName);

	/**
	 * Query for a list of objects of type T from the default collection.  
	 * 
	 * The object is converted from the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.  
	 * 
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient
	 * way to map objects since the test for class type is done in the client and not on the server.
	 * 
	 * @param targetClass the parameterized type of the returned list
	 * @return the converted collection
	 */
	<T> List<T> getCollection(Class<T> targetClass);

	/**
	 * Query for a list of objects of type T from the specified collection.  
	 * 
	 * The object is converted from the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.  
	 * 
	 * If your collection does not contain a homogeneous collection of types, this operation will not be an efficient
	 * way to map objects since the test for class type is done in the client and not on the server.
	 * @param collectionName name of the collection to retrieve the objects from 
	 * @param targetClass the parameterized type of the returned list.
	 * @return the converted collection
	 */
	<T> List<T> getCollection(String collectionName, Class<T> targetClass);

	/**
	 * Query for a list of objects of type T from the specified collection, mapping the DBObject using
	 * the provided MongoReader.
	 * 
	 * @param collectionName name of the collection to retrieve the objects from 
	 * @param targetClass the parameterized type of the returned list.
	 * @param reader the MongoReader to convert from DBObject to an object.
	 * @return the converted collection
	 */
	<T> List<T> getCollection(String collectionName, Class<T> targetClass,
			MongoReader<T> reader);

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type.
	 *
	 * The object is converted from the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.   
	 * 
	 * The query is specified as a {@link QueryDefinition} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields specification 
	 * @param targetClass the parameterized type of the returned list.
	 * @return the List of converted objects
	 */
	<T> List<T> find(QueryDefinition query, Class<T> targetClass);

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type.
	 *
	 * The object is converted from the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.   
	 * 
	 * The query is specified as a {@link QueryDefinition} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields specification 
	 * @param targetClass the parameterized type of the returned list.
	 * @param reader the MongoReader to convert from DBObject to an object.
	 * @return the List of converted objects
	 */
	<T> List<T> find(QueryDefinition query, Class<T> targetClass,
			MongoReader<T> reader);

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type.
	 *
	 * The object is converted from the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.   
	 * 
	 * The query is specified as a {@link QueryDefinition} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param collectionName name of the collection to retrieve the objects from	 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields specification 
	 * @param targetClass the parameterized type of the returned list.
	 * @return the List of converted objects
	 */
	<T> List<T> find(String collectionName, QueryDefinition query,
			Class<T> targetClass);

	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type.
	 *
	 * The object is converted from the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.   
	 * 
	 * The query is specified as a {@link QueryDefinition} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param collectionName name of the collection to retrieve the objects from	 
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields specification 
	 * @param targetClass the parameterized type of the returned list.
	 * @param reader the MongoReader to convert from DBObject to an object.
	 * @return the List of converted objects
	 */
	<T> List<T> find(String collectionName, QueryDefinition query,
			Class<T> targetClass, MongoReader<T> reader);


	/**
	 * Map the results of an ad-hoc query on the default MongoDB collection to a List of the specified type.
	 * 
	 * The object is converted from the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.   
	 * 
	 * The query is specified as a {@link QueryDefinition} which can be created either using the {@link BasicQuery} or the more
	 * feature rich {@link Query}.
	 * 
	 * @param collectionName name of the collection to retrieve the objects from
	 * @param query the query class that specifies the criteria used to find a record and also an optional fields specification 
	 * @param targetClass the parameterized type of the returned list.
	 * @param preparer allows for customization of the DBCursor used when iterating over the result set,
	 * (apply limits, skips and so on).
	 * @return the List of converted objects.
	 */
	<T> List<T> find(String collectionName, QueryDefinition query, Class<T> targetClass, CursorPreparer preparer);

	/**
	 * Insert the object into the default collection.  
	 * 
	 * The object is converted to the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.  
	 * 
	 * If you object has an "Id' property, it will be set with the generated Id from MongoDB.  If your Id property
	 * is a String then MongoDB ObjectId will be used to populate that string.  Otherwise, the conversion from
	 * ObjectId to your property type will be handled by Spring's BeanWrapper class that leverages Spring 3.0's
	 * new Type Conversion API.  
	 * See <a href="http://static.springsource.org/spring/docs/3.0.x/reference/validation.html#core-convert">Spring 3 Type Conversion"</a>
	 * for more details.
	 *   
	 * 
	 * Insert is used to initially store the object into the database.  
	 * To update an existing object use the save method.
	 * 
	 * @param objectToSave the object to store in the collection.
	 */
	void insert(Object objectToSave);

	/**
	 * Insert the object into the specified collection.  
	 * 
	 * The object is converted to the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used. 
	 * 
	 * Insert is used to initially store the object into the 
	 * database.  To update an existing object use the save method.
	 * 
	 * @param collectionName name of the collection to store the object in
	 * @param objectToSave the object to store in the collection
	 */
	void insert(String collectionName, Object objectToSave);

	/**
	 * Insert the object into the specified collection.  
	 * 
	 * The object is converted to the MongoDB native representation using an instance of 
	 * {@see MongoWriter} 
	 * 
	 * Insert is used to initially store the object into the 
	 * database.  To update an existing object use the save method.
	 * 
	 * @param <T> the type of the object to insert
	 * @param collectionName name of the collection to store the object in 
	 * @param objectToSave  the object to store in the collection
	 * @param writer the writer to convert the object to save into a DBObject
	 */
	<T> void insert(String collectionName, T objectToSave, MongoWriter<T> writer);

	/**
	 * Insert a list of objects into the default collection in a single batch write to the database.
	 * 
	 * @param listToSave the list of objects to save.
	 */
	void insertList(List<? extends Object> listToSave);

	/**
	 * Insert a list of objects into the specified collection in a single batch write to the database.
	 * @param collectionName name of the collection to store the object in 
	 * @param listToSave the list of objects to save.
	 */
	void insertList(String collectionName, List<? extends Object> listToSave);

	/**
	 * Insert a list of objects into the specified collection using the provided MongoWriter instance
	 * 
	 * @param <T> the type of object being saved
	 * @param collectionName name of the collection to store the object in 
	 * @param listToSave the list of objects to save.
	 * @param writer the writer to convert the object to save into a DBObject
	 */
	<T> void insertList(String collectionName, List<? extends T> listToSave, MongoWriter<T> writer);

	/**
	 * Save the object to the default collection.  This will perform an insert if the object is not already 
	 * present, that is an 'upsert'.
	 * 
	 * The object is converted to the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.  
	 * 
	 * If you object has an "Id' property, it will be set with the generated Id from MongoDB.  If your Id property
	 * is a String then MongoDB ObjectId will be used to populate that string.  Otherwise, the conversion from
	 * ObjectId to your property type will be handled by Spring's BeanWrapper class that leverages Spring 3.0's
	 * new Type Conversion API.  
	 * See <a href="http://static.springsource.org/spring/docs/3.0.x/reference/validation.html#core-convert">Spring 3 Type Conversion"</a>
	 * for more details.
	 * 
	 * @param objectToSave the object to store in the collection
	 */
	void save(Object objectToSave);

	/**
	 * Save the object to the specified collection.  This will perform an insert if the object is not already 
	 * present, that is an 'upsert'.
	 * 
	 * The object is converted to the MongoDB native representation using an instance of 
	 * {@see MongoConverter}.  Unless configured otherwise, an
	 * instance of SimpleMongoConverter will be used.  
	 * 
	 * If you object has an "Id' property, it will be set with the generated Id from MongoDB.  If your Id property
	 * is a String then MongoDB ObjectId will be used to populate that string.  Otherwise, the conversion from
	 * ObjectId to your property type will be handled by Spring's BeanWrapper class that leverages Spring 3.0's
	 * new Type Cobnversion API.  
	 * See <a href="http://static.springsource.org/spring/docs/3.0.x/reference/validation.html#core-convert">Spring 3 Type Conversion"</a>
	 * for more details.
	 * 
	 * @param collectionName name of the collection to store the object in 
	 * @param objectToSave the object to store in the collection
	 */
	void save(String collectionName, Object objectToSave);

	/**
	 * Save the object into the specified collection. This will perform an insert if the object is not already 
	 * present, that is an 'upsert'.  
	 * 
	 * The object is converted to the MongoDB native representation using an instance of 
	 * {@see MongoWriter}
	 *  
	 * @param <T> the type of the object to insert
	 * @param collectionName name of the collection to store the object in 
	 * @param objectToSave  the object to store in the collection
	 * @param writer the writer to convert the object to save into a DBObject
	 */
	<T> void save(String collectionName, T objectToSave, MongoWriter<T> writer);

	/**
	 * Updates the first object that is found in the default collection that matches the query document 
	 * with the provided updated document.
	 * 
	 * @param queryDoc the query document that specifies the criteria used to select a record to be updated
	 * @param updateDoc the update document that contains the updated object or $ operators to manipulate the
	 * existing object. 
	 */
	void updateFirst(QueryDefinition query, UpdateDefinition update);

	/**
	 * Updates the first object that is found in the specified collection that matches the query document criteria
	 * with the provided updated document.
	 * 
	 * @param collectionName name of the collection to update the object in 
	 * @param queryDoc the query document that specifies the criteria used to select a record to be updated
	 * @param updateDoc the update document that contains the updated object or $ operators to manipulate the
	 * existing object. 
	 */
	void updateFirst(String collectionName, QueryDefinition query,
			UpdateDefinition update);

	/**
	 * Updates all objects that are found in the default collection that matches the query document criteria
	 * with the provided updated document.
	 * 
	 * @param queryDoc the query document that specifies the criteria used to select a record to be updated
	 * @param updateDoc the update document that contains the updated object or $ operators to manipulate the
	 * existing object. 
	 */
	void updateMulti(QueryDefinition query, UpdateDefinition update);

	/**
	 * Updates all objects that are found in the specified collection that matches the query document criteria
	 * with the provided updated document.
	 * 
	 * @param collectionName name of the collection to update the object in 
	 * @param queryDoc the query document that specifies the criteria used to select a record to be updated
	 * @param updateDoc the update document that contains the updated object or $ operators to manipulate the
	 * existing object. 
	 */
	void updateMulti(String collectionName, QueryDefinition query,
			UpdateDefinition update);

	/**
	 * Remove all documents from the default collection that match the provide query document criteria.
	 * @param queryDoc the query document that specifies the criteria used to remove a record 
	 */
	void remove(QueryDefinition query);

	/**
	 * Remove all documents from the specified collection that match the provide query document criteria.
	 * @param collectionName name of the collection where the objects will removed
	 * @param queryDoc the query document that specifies the criteria used to remove a record 
	 */
	void remove(String collectionName, QueryDefinition query);

}