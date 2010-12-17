package org.springframework.data.document.mongodb;

import java.util.List;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public interface MongoOperations {

	String getDefaultCollectionName();

	/**
	 * @return The default collection used by this template
	 */
	DBCollection getDefaultCollection();

	void executeCommand(String jsonCommand);

	void executeCommand(DBObject command);

	/**
	 * Executes a {@link DBCallback} translating any exceptions as necessary
	 * 
	 * @param <T> The return type
	 * @param action The action to execute
	 * 
	 * @return The return value of the {@link DBCallback}
	 */
	<T> T execute(DBCallback<T> action);

	/**
	 * Executes the given {@link CollectionCallback} on the default collection.
	 * 
	 * @param <T>
	 * @param callback
	 * @return
	 */
	<T> T execute(CollectionCallback<T> callback);

	/**
	 * Executes the given {@link CollectionCallback} on the collection of the given name.
	 * 
	 * @param <T>
	 * @param callback
	 * @param collectionName
	 * @return
	 */
	<T> T execute(CollectionCallback<T> callback, String collectionName);

	<T> T executeInSession(DBCallback<T> action);

	DBCollection createCollection(String collectionName);

	void createCollection(String collectionName,
			CollectionOptions collectionOptions);

	List<String> getCollectionNames();
	
	DBCollection getCollection(String collectionName);

	boolean collectionExists(String collectionName);

	void dropCollection(String collectionName);

	void insert(Object objectToSave);

	void insert(String collectionName, Object objectToSave);

	<T> void insert(String collectionName, T objectToSave, MongoWriter<T> writer);

	void insertList(List<Object> listToSave);

	void insertList(String collectionName, List<Object> listToSave);

	<T> void insertList(String collectionName, List<T> listToSave,
			MongoWriter<T> writer);

	void save(Object objectToSave);

	void save(String collectionName, Object objectToSave);

	<T> void save(String collectionName, T objectToSave, MongoWriter<T> writer);

	void updateFirst(DBObject queryDoc, DBObject updateDoc);

	void updateFirst(String collectionName, DBObject queryDoc,
			DBObject updateDoc);

	void updateMulti(DBObject queryDoc, DBObject updateDoc);

	void updateMulti(String collectionName, DBObject queryDoc,
			DBObject updateDoc);

	void remove(DBObject queryDoc);

	void remove(String collectionName, DBObject queryDoc);

	<T> List<T> getCollection(Class<T> targetClass);

	<T> List<T> getCollection(String collectionName, Class<T> targetClass);

	<T> List<T> getCollection(String collectionName, Class<T> targetClass,
			MongoReader<T> reader);

	<T> List<T> queryUsingJavaScript(String query, Class<T> targetClass);

	<T> List<T> queryUsingJavaScript(String query, Class<T> targetClass,
			MongoReader<T> reader);

	<T> List<T> queryUsingJavaScript(String collectionName, String query,
			Class<T> targetClass);

	<T> List<T> queryUsingJavaScript(String collectionName, String query,
			Class<T> targetClass, MongoReader<T> reader);

	<T> List<T> query(DBObject query, Class<T> targetClass);

	<T> List<T> query(DBObject query, Class<T> targetClass,
			CursorPreparer preparer);

	<T> List<T> query(DBObject query, Class<T> targetClass,
			MongoReader<T> reader);

	<T> List<T> query(String collectionName, DBObject query,
			Class<T> targetClass);

	<T> List<T> query(String collectionName, DBObject query,
			Class<T> targetClass, CursorPreparer preparer);

	<T> List<T> query(String collectionName, DBObject query,
			Class<T> targetClass, MongoReader<T> reader);

}