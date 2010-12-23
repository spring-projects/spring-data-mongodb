/*
 * Copyright 2010 the original author or authors.
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


import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class MongoTemplate implements InitializingBean, MongoOperations {

	private String defaultCollectionName;
	
	private MongoConverter mongoConverter;
	
	//TODO expose configuration...
	private CollectionOptions defaultCollectionOptions;

	private Mongo mongo;

	private String databaseName;
	
	private String username;
	
	private char[] password;
	
	
	public MongoTemplate(Mongo mongo, String databaseName) {
		this(mongo, databaseName, null, null);
	}
	
	public MongoTemplate(Mongo mongo, String databaseName, String defaultCollectionName) {
		this(mongo, databaseName, defaultCollectionName, null);
	}
	
	public MongoTemplate(Mongo mongo, String databaseName, MongoConverter mongoConverter) {
		this(mongo, databaseName, null, mongoConverter);
	}
	
	public MongoTemplate(Mongo mongo, String databaseName, String defaultCollectionName, MongoConverter mongoConverter) {
		this.mongoConverter = mongoConverter;
		this.defaultCollectionName = defaultCollectionName;
		this.mongo = mongo;
		this.databaseName = databaseName;
	}
	
	
	/**
	 * Sets the username to use to connect to the Mongo database
	 * 
	 * @param username The username to use
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Sets the password to use to authenticate with the Mongo database
	 * 
	 * @param password The password to use
	 */
	public void setPassword(char[] password) {
		this.password = password;
	}

	public void setDefaultCollectionName(String defaultCollectionName) {
		this.defaultCollectionName = defaultCollectionName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getDefaultCollectionName()
	 */
	public String getDefaultCollectionName() {
		return defaultCollectionName;
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getDefaultCollection()
	 */
	public DBCollection getDefaultCollection() {
		return getDb().getCollection(getDefaultCollectionName());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#executeCommand(java.lang.String)
	 */
	public void executeCommand(String jsonCommand) {
		executeCommand((DBObject)JSON.parse(jsonCommand));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#executeCommand(com.mongodb.DBObject)
	 */
	public void executeCommand(DBObject command) {
		CommandResult cr = getDb().command(command);
		String err = cr.getErrorMessage();
		if (err != null) {
			throw new InvalidDataAccessApiUsageException("Command execution of " + 
					command.toString() + " failed: " + err);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.DBCallback)
	 */
	public <T> T execute(DBCallback<T> action) {
		DB db = getDb();

		try {
			return action.doInDB(db);
		} catch (MongoException e) {
			throw MongoDbUtils.translateMongoExceptionIfPossible(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.CollectionCallback)
	 */
	public <T> T execute(CollectionCallback<T> action) {
		return execute(action, defaultCollectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.CollectionCallback, java.lang.String)
	 */
	public <T> T execute(CollectionCallback<T> callback, String collectionName) {

		try {
			return callback.doInCollection(getCollection(collectionName));
		} catch (MongoException e) {
			throw MongoDbUtils.translateMongoExceptionIfPossible(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#executeInSession(org.springframework.data.document.mongodb.DBCallback)
	 */
	public <T> T executeInSession(DBCallback<T> action) {
		DB db = getDb();
		db.requestStart();
		try {
			return action.doInDB(db);
		} catch (MongoException e) {
			throw MongoDbUtils.translateMongoExceptionIfPossible(e);
		} finally {
			db.requestDone();
		}
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#createCollection(java.lang.String)
	 */
	public DBCollection createCollection(String collectionName) {
		try {
			return getDb().createCollection(collectionName, new BasicDBObject());
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}
		
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#createCollection(java.lang.String, org.springframework.data.document.mongodb.CollectionOptions)
	 */
	public void createCollection(String collectionName, CollectionOptions collectionOptions) {
		try {
			getDb().createCollection(collectionName, convertToDbObject(collectionOptions));
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.String)
	 */
	public DBCollection getCollection(String collectionName) {
		try {
			return getDb().getCollection(collectionName);
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}
		

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#collectionExists(java.lang.String)
	 */
	public boolean collectionExists(String collectionName) {
		try {
			return getDb().collectionExists(collectionName);
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#dropCollection(java.lang.String)
	 */
	public void dropCollection(String collectionName) {
		getDb().getCollection(collectionName)
			.drop();
	}


	private String getRequiredDefaultCollectionName() {
		String name = getDefaultCollectionName();
		if (name == null) {
			throw new IllegalStateException(
					"No 'defaultCollection' or 'defaultCollectionName' specified. Check configuration of MongoTemplate.");
		}
		return name;
	}

	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insert(java.lang.Object)
	 */
	public void insert(Object objectToSave) {
		insert(getRequiredDefaultCollectionName(), objectToSave);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insert(java.lang.String, java.lang.Object)
	 */
	public void insert(String collectionName, Object objectToSave) {
		insert(collectionName, objectToSave, this.mongoConverter);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insert(java.lang.String, T, org.springframework.data.document.mongodb.MongoWriter)
	 */
	public <T> void insert(String collectionName, T objectToSave, MongoWriter<T> writer) {
		BasicDBObject dbDoc = new BasicDBObject();
		writer.write(objectToSave, dbDoc);
		Object _id = insertDBObject(collectionName, dbDoc);
		populateIdIfNecessary(objectToSave, _id);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.util.List)
	 */
	public void insertList(List<Object> listToSave) {
		insertList(getRequiredDefaultCollectionName(), listToSave);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.lang.String, java.util.List)
	 */
	public void insertList(String collectionName, List<Object> listToSave) {
		insertList(collectionName, listToSave, this.mongoConverter);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.lang.String, java.util.List, org.springframework.data.document.mongodb.MongoWriter)
	 */
	public <T> void insertList(String collectionName, List<T> listToSave, MongoWriter<T> writer) {
		List<DBObject> dbObjectList = new ArrayList<DBObject>();
		for (T o : listToSave) {
			BasicDBObject dbDoc = new BasicDBObject();
			writer.write(o, dbDoc);
			dbObjectList.add(dbDoc);
		}
		List<Object> _ids = insertDBObjectList(collectionName, dbObjectList);
		for (int i = 0; i < listToSave.size(); i++) {
			if (i < _ids.size())
				populateIdIfNecessary(listToSave.get(i), _ids.get(i));
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#save(java.lang.Object)
	 */
	public void save(Object objectToSave) {
		save(getRequiredDefaultCollectionName(), objectToSave);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#save(java.lang.String, java.lang.Object)
	 */
	public void save(String collectionName, Object objectToSave) {
		save(collectionName, objectToSave, this.mongoConverter);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#save(java.lang.String, T, org.springframework.data.document.mongodb.MongoWriter)
	 */
	public <T> void save(String collectionName, T objectToSave, MongoWriter<T> writer) {
		BasicDBObject dbDoc = new BasicDBObject();
		writer.write(objectToSave, dbDoc);
		Object _id = saveDBObject(collectionName, dbDoc);
		populateIdIfNecessary(objectToSave, _id);
	}


	protected Object insertDBObject(String collectionName, DBObject dbDoc) {
		if (dbDoc.keySet().size() > 0 ) {
			WriteResult wr = null;
			try {
				wr = getDb().getCollection(collectionName).insert(dbDoc);
				return dbDoc.get("_id");
			} catch (MongoException e) {
				throw new DataRetrievalFailureException(wr.getLastError().getErrorMessage(), e);
			}
		}
		else {
			return null;
		}
	}

	protected List<Object> insertDBObjectList(String collectionName, List<DBObject> dbDocList) {
		if (dbDocList.size() > 0 ) {
			List<Object> ids = new ArrayList<Object>();
			WriteResult wr = null;
			try {
				wr = getDb().getCollection(collectionName).insert(dbDocList);
				for (DBObject dbo : dbDocList) {
					ids.add(dbo.get("_id"));
				}
				return ids;
			} catch (MongoException e) {
				throw new DataRetrievalFailureException(wr.getLastError().getErrorMessage(), e);
			}
		}
		else {
			return null;
		}
	}

	protected Object saveDBObject(String collectionName, DBObject dbDoc) {
		if (dbDoc.keySet().size() > 0 ) {
			WriteResult wr = null;
			try {
				wr = getDb().getCollection(collectionName).save(dbDoc);
				return dbDoc.get("_id");
			} catch (MongoException e) {
				throw new DataRetrievalFailureException(wr.getLastError().getErrorMessage(), e);
			}
		}
		else {
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#updateFirst(com.mongodb.DBObject, com.mongodb.DBObject)
	 */
	public void updateFirst(DBObject queryDoc, DBObject updateDoc) {
		updateFirst(getRequiredDefaultCollectionName(), queryDoc, updateDoc);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#updateFirst(java.lang.String, com.mongodb.DBObject, com.mongodb.DBObject)
	 */
	public void updateFirst(String collectionName, DBObject queryDoc, DBObject updateDoc) {
		WriteResult wr = null;
		try {
			wr = getDb().getCollection(collectionName).update(queryDoc, updateDoc);
		} catch (MongoException e) {
			throw new DataRetrievalFailureException("Error during update using " + queryDoc + ", " + updateDoc + ": " + wr.getLastError().getErrorMessage(), e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#updateMulti(com.mongodb.DBObject, com.mongodb.DBObject)
	 */
	public void updateMulti(DBObject queryDoc, DBObject updateDoc) {
		updateMulti(getRequiredDefaultCollectionName(), queryDoc, updateDoc);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#updateMulti(java.lang.String, com.mongodb.DBObject, com.mongodb.DBObject)
	 */
	public void updateMulti(String collectionName, DBObject queryDoc, DBObject updateDoc) {
		WriteResult wr = null;
		try {
			wr = getDb().getCollection(collectionName).updateMulti(queryDoc, updateDoc);
		} catch (MongoException e) {
			throw new DataRetrievalFailureException("Error during updateMulti using " + queryDoc + ", " + updateDoc + ": " + wr.getLastError().getErrorMessage(), e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#remove(com.mongodb.DBObject)
	 */
	public void remove(DBObject queryDoc) {
		remove(getRequiredDefaultCollectionName(), queryDoc);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#remove(java.lang.String, com.mongodb.DBObject)
	 */
	public void remove(String collectionName, DBObject queryDoc) {
		WriteResult wr = null;
		try {
			wr = getDb().getCollection(collectionName).remove(queryDoc);
		} catch (MongoException e) {
			throw new DataRetrievalFailureException("Error during remove using "  + queryDoc + ": " + wr.getLastError().getErrorMessage(), e);
		}
	}
	

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.Class)
	 */
	public <T> List<T> getCollection(Class<T> targetClass) {
		
		List<T> results = new ArrayList<T>();
		DBCollection collection = getDb().getCollection(getDefaultCollectionName());
		for (DBObject dbo : collection.find()) {
			Object obj = mongoConverter.read(targetClass, dbo);
			//effectively acts as a query on the collection restricting it to elements of a specific type
			if (targetClass.isInstance(obj)) {
				results.add(targetClass.cast(obj));
			}
		}
		return results;
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.String, java.lang.Class)
	 */
	public <T> List<T> getCollection(String collectionName, Class<T> targetClass) {
		
		List<T> results = new ArrayList<T>();
		DBCollection collection = getDb().getCollection(collectionName);
		for (DBObject dbo : collection.find()) {
			Object obj = mongoConverter.read(targetClass, dbo);
			//effectively acts as a query on the collection restricting it to elements of a specific type
			if (targetClass.isInstance(obj)) {
				results.add(targetClass.cast(obj));
			}
		}
		return results;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollectionNames()
	 */
	public List<String> getCollectionNames() {
		return new ArrayList<String>(getDb().getCollectionNames());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.String, java.lang.Class, org.springframework.data.document.mongodb.MongoReader)
	 */
	public <T> List<T> getCollection(String collectionName, Class<T> targetClass, MongoReader<T> reader) { 
		List<T> results = new ArrayList<T>();
		DBCollection collection = getDb().getCollection(collectionName);
		for (DBObject dbo : collection.find()) {
			results.add(reader.read(targetClass, dbo));
		}
		return results;
	}
	
	// Queries that take JavaScript to express the query.
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#queryUsingJavaScript(java.lang.String, java.lang.Class)
	 */
	public <T> List<T> queryUsingJavaScript(String query, Class<T> targetClass) {
		return query(getDefaultCollectionName(), (DBObject)JSON.parse(query), targetClass); //
	}	
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#queryUsingJavaScript(java.lang.String, java.lang.Class, org.springframework.data.document.mongodb.MongoReader)
	 */
	public <T> List<T> queryUsingJavaScript(String query, Class<T> targetClass, MongoReader<T> reader) {
		return query(getDefaultCollectionName(), (DBObject)JSON.parse(query), targetClass, reader);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#queryUsingJavaScript(java.lang.String, java.lang.String, java.lang.Class)
	 */
	public <T> List<T> queryUsingJavaScript(String collectionName, String query, Class<T> targetClass) {
		return query(collectionName, (DBObject)JSON.parse(query), targetClass); //
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#queryUsingJavaScript(java.lang.String, java.lang.String, java.lang.Class, org.springframework.data.document.mongodb.MongoReader)
	 */
	public <T> List<T> queryUsingJavaScript(String collectionName, String query, Class<T> targetClass, MongoReader<T> reader) {
		return query(collectionName, (DBObject)JSON.parse(query), targetClass, reader);
	}

	
	// Queries that take DBObject to express the query
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#query(com.mongodb.DBObject, java.lang.Class)
	 */
	public <T> List<T> query(DBObject query, Class<T> targetClass) {
		return query(getDefaultCollectionName(), query, targetClass); //
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#query(com.mongodb.DBObject, java.lang.Class, org.springframework.data.document.mongodb.CursorPreparer)
	 */
	public <T> List<T> query(DBObject query, Class<T> targetClass, CursorPreparer preparer) {
		return query(getDefaultCollectionName(), query, targetClass, preparer); //
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#query(com.mongodb.DBObject, java.lang.Class, org.springframework.data.document.mongodb.MongoReader)
	 */
	public <T> List<T> query(DBObject query, Class<T> targetClass, MongoReader<T> reader) {
		return query(getDefaultCollectionName(), query, targetClass, reader);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#query(java.lang.String, com.mongodb.DBObject, java.lang.Class)
	 */
	public <T> List<T> query(String collectionName, DBObject query, Class<T> targetClass) {	
		return query(collectionName, query, targetClass, (CursorPreparer) null);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#query(java.lang.String, com.mongodb.DBObject, java.lang.Class, org.springframework.data.document.mongodb.CursorPreparer)
	 */
	public <T> List<T> query(String collectionName, DBObject query, Class<T> targetClass, CursorPreparer preparer) {
		DBCollection collection = getDb().getCollection(collectionName);
		List<T> results = new ArrayList<T>();
		DBCursor cursor = collection.find(query);
		if (preparer != null) {
			preparer.prepare(cursor);
		}
		for (DBObject dbo : cursor) {
			Object obj = mongoConverter.read(targetClass,dbo);
			//effectively acts as a query on the collection restricting it to elements of a specific type
			if (targetClass.isInstance(obj)) {
				results.add(targetClass.cast(obj));
			}
		}
		return results;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#query(java.lang.String, com.mongodb.DBObject, java.lang.Class, org.springframework.data.document.mongodb.MongoReader)
	 */
	public <T> List<T> query(String collectionName, DBObject query, Class<T> targetClass, MongoReader<T> reader) {
		DBCollection collection = getDb().getCollection(collectionName);
		List<T> results = new ArrayList<T>();
		for (DBObject dbo : collection.find(query)) {
			results.add(reader.read(targetClass, dbo));
		}
		return results;
	}

	public RuntimeException convertMongoAccessException(RuntimeException ex) {
		return MongoDbUtils.translateMongoExceptionIfPossible(ex);
	}

	public DB getDb() {
		if(username != null && password != null) {
			return MongoDbUtils.getDB(mongo, databaseName, username, password);
		}
		return MongoDbUtils.getDB(mongo, databaseName);
	}

	
	protected DBObject convertToDbObject(CollectionOptions collectionOptions) {
		DBObject dbo = new BasicDBObject();
		if (collectionOptions != null) {
			if (collectionOptions.getCapped() != null) {
				dbo.put("capped", collectionOptions.getCapped().booleanValue());			
			}
			if (collectionOptions.getSize() != null) {
				dbo.put("size", collectionOptions.getSize().intValue());
			}
			if (collectionOptions.getMaxDocuments() != null ) {
				dbo.put("max", collectionOptions.getMaxDocuments().intValue());
			}
		}
		return dbo;
	}

	protected void populateIdIfNecessary(Object savedObject, Object id) {
		//TODO Needs proper conversion support and should be integrated with reader implementation somehow
		BeanWrapper bw = PropertyAccessorFactory.forBeanPropertyAccess(savedObject);
		PropertyDescriptor idPd = BeanUtils.getPropertyDescriptor(savedObject.getClass(), "id");
		if (idPd == null) {
			idPd = BeanUtils.getPropertyDescriptor(savedObject.getClass(), "_id");
		}
		if (idPd != null) {
			Object v = bw.getPropertyValue(idPd.getName());
			if (v == null) {
				if (id instanceof ObjectId) {
					bw.setPropertyValue(idPd.getName(), id.toString());
				}
				else if (id.getClass().isAssignableFrom(idPd.getPropertyType())) {
					bw.setPropertyValue(idPd.getName(), id);
				}
			}
		}
		
	}

	public void afterPropertiesSet() throws Exception {
		if (this.getDefaultCollectionName() != null) {
			DB db = getDb();
			if (! db.collectionExists(getDefaultCollectionName())) {
				db.createCollection(getDefaultCollectionName(), null);
			}
		}
		if (this.mongoConverter == null) {
			mongoConverter = new SimpleMongoConverter();
		}
		
	}
}
