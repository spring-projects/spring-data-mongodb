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
import org.springframework.data.document.AbstractDocumentStoreTemplate;
import org.springframework.data.document.mongodb.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class MongoTemplate implements InitializingBean {

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

	public String getDefaultCollectionName() {
		return defaultCollectionName;
	}
	
	/**
	 * @return The default collection used by this template
	 */
	public DBCollection getDefaultCollection() {
		return getDb().getCollection(getDefaultCollectionName());
	}

	public void executeCommand(String jsonCommand) {
		executeCommand((DBObject)JSON.parse(jsonCommand));
	}

	public void executeCommand(DBObject command) {
		CommandResult cr = getDb().command(command);
		String err = cr.getErrorMessage();
		if (err != null) {
			throw new InvalidDataAccessApiUsageException("Command execution of " + 
					command.toString() + " failed: " + err);
		}
	}
	
	/**
	 * Executes a {@link DBCallback} translating any exceptions as necessary
	 * 
	 * @param <T> The return type
	 * @param action The action to execute
	 * 
	 * @return The return value of the {@link DBCallback}
	 */
	public <T> T execute(DBCallback<T> action) {
		DB db = getDb();

		try {
			return action.doInDB(db);
		} catch (MongoException e) {
			throw MongoDbUtils.translateMongoExceptionIfPossible(e);
		} 
	}
	
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
	
	public DBCollection createCollection(String collectionName) {
		try {
			return getDb().createCollection(collectionName, null);
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}
		
	public void createCollection(String collectionName, CollectionOptions collectionOptions) {
		try {
			getDb().createCollection(collectionName, convertToDbObject(collectionOptions));
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}
	
	public DBCollection getCollection(String collectionName) {
		try {
			return getDb().getCollection(collectionName);
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}
		

	public boolean collectionExists(String collectionName) {
		try {
			return getDb().collectionExists(collectionName);
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}

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

	
	public void save(Object objectToSave) {
		save(getRequiredDefaultCollectionName(), objectToSave);
	}
	
	public void save(String collectionName, Object objectToSave) {
		save(collectionName, objectToSave, this.mongoConverter);
	}
	
	public <T> void save(String collectionName, T objectToSave, MongoWriter<T> writer) {
		BasicDBObject dbDoc = new BasicDBObject();
		writer.write(objectToSave, dbDoc);
		Object _id = saveDBObject(collectionName, dbDoc);
		populateIdIfNecessary(objectToSave, _id);
	}


	protected Object saveDBObject(String collectionName, BasicDBObject dbDoc) {
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

	public <T> List<T> queryForCollection(Class<T> targetClass) {
		
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
	
	public <T> List<T> queryForCollection(String collectionName, Class<T> targetClass) {
		
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

	public <T> List<T> queryForCollection(String collectionName, Class<T> targetClass, MongoReader<T> reader) { 
		List<T> results = new ArrayList<T>();
		DBCollection collection = getDb().getCollection(collectionName);
		for (DBObject dbo : collection.find()) {
			results.add(reader.read(targetClass, dbo));
		}
		return results;
	}	

	public <T> List<T> queryForList(String collectionName, String query, Class<T> targetClass) {
		return queryForList(collectionName, (DBObject)JSON.parse(query), targetClass);
	}

	public <T> List<T> queryForList(String collectionName, String query, Class<T> targetClass, MongoReader<T> reader) {
		return queryForList(collectionName, (DBObject)JSON.parse(query), targetClass, reader);
	}

	
	//
	
	public <T> List<T> queryForList(String collectionName, DBObject query, Class<T> targetClass) {	
		DBCollection collection = getDb().getCollection(collectionName);
		List<T> results = new ArrayList<T>();
		for (DBObject dbo : collection.find(query)) {
			Object obj = mongoConverter.read(targetClass,dbo);
			//effectively acts as a query on the collection restricting it to elements of a specific type
			if (targetClass.isInstance(obj)) {
				results.add(targetClass.cast(obj));
			}
		}
		return results;
	}

	public <T> List<T> queryForList(String collectionName, DBObject query, Class<T> targetClass, MongoReader<T> reader) {
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

	private void populateIdIfNecessary(Object savedObject, Object id) {
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
