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

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jca.cci.core.ConnectionCallback;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

/**
 * Primary implementation of {@link MongoOperations}.
 *
 * @author Thomas Risberg
 * @author Graeme Rocher
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class MongoTemplate implements InitializingBean, MongoOperations {
	
	private static final String ID = "_id";

	private final MongoConverter mongoConverter;
	private final Mongo mongo;
	private final MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	
	private String defaultCollectionName;
	private String databaseName;
	private String username;
	private String password;
	
	
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
		
		Assert.notNull(mongo);
		Assert.notNull(databaseName);
		
		this.mongoConverter = mongoConverter == null ? new SimpleMongoConverter() : mongoConverter;
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
	 * Sets the password to use to authenticate with the Mongo database.
	 * 
	 * @param password The password to use
	 */
	public void setPassword(String password) {
		
		this.password = password;
	}

	/**
	 * Sets the name of the default collection to be used.
	 * 
	 * @param defaultCollectionName
	 */
	public void setDefaultCollectionName(String defaultCollectionName) {
		this.defaultCollectionName = defaultCollectionName;
	}

	/**
	 * Sets the database name to be used.
	 * 
	 * @param databaseName
	 */
	public void setDatabaseName(String databaseName) {
		Assert.notNull(databaseName);
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
		
		return execute(new DBCallback<DBCollection>() {
			public DBCollection doInDB(DB db) throws MongoException, DataAccessException {
				return db.getCollection(getDefaultCollectionName());
			}
		});
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
	public void executeCommand(final DBObject command) {
		
		CommandResult result = execute(new DBCallback<CommandResult>() {
			public CommandResult doInDB(DB db) throws MongoException, DataAccessException {
				return db.command(command);
			}
		});
		
		String error = result.getErrorMessage();
		if (error != null) {
			throw new InvalidDataAccessApiUsageException("Command execution of " +
					command.toString() + " failed: " + error);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.DBCallback)
	 */
	public <T> T execute(DbCallback<T> action) {
		
		Assert.notNull(action);
		
		try {
			DB db = getDb();
			return action.doInDB(db);
		} catch (MongoException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.CollectionCallback)
	 */
	public <T> T execute(CollectionCallback<T> callback) {
		return execute(callback, defaultCollectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#execute(org.springframework.data.document.mongodb.CollectionCallback, java.lang.String)
	 */
	public <T> T execute(CollectionCallback<T> callback, String collectionName) {

		Assert.notNull(callback);
		
		try {
			DBCollection collection = getDb().getCollection(collectionName);
			return callback.doInCollection(collection);
		} catch (MongoException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}
	
	/**
	 * Central callback executing method to do queries against the datastore that requires reading a collection of
	 * objects. It will take the following steps <ol> <li>Execute the given {@link ConnectionCallback} for a
	 * {@link DBCursor}.</li> <li>Prepare that {@link DBCursor} with the given {@link CursorPreparer} (will be skipped
	 * if {@link CursorPreparer} is {@literal null}</li> <li>Iterate over the {@link DBCursor} and applies the given
	 * {@link DbObjectCallback} to each of the {@link DBObject}s collecting the actual result {@link List}.</li> <ol>
	 * 
	 * @param <T>
	 * @param collectionCallback the callback to retrieve the {@link DBCursor} with
	 * @param preparer the {@link CursorPreparer} to potentially modify the {@link DBCursor} before ireating over it
	 * @param objectCallback the {@link DbObjectCallback} to transform {@link DBObject}s into the actual domain type
	 * @param collectionName the collection to be queried
	 * @return
	 */
	private <T> List<T> executeEach(CollectionCallback<DBCursor> collectionCallback, CursorPreparer preparer,
			DbObjectCallback<T> objectCallback, String collectionName) {
		
		try {
			DBCursor cursor = collectionCallback.doInCollection(getCollection(collectionName));
			
			if (preparer != null) {
				preparer.prepare(cursor);
			}
			
			List<T> result = new ArrayList<T>();
			
			for (DBObject object : cursor) {
				result.add(objectCallback.doWith(object));
			}
			
			return result;
		} catch (MongoException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#executeInSession(org.springframework.data.document.mongodb.DBCallback)
	 */
	public <T> T executeInSession(final DBCallback<T> action) {
		
		return execute(new DBCallback<T>() {
			public T doInDB(DB db) throws MongoException, DataAccessException {
				try {
					db.requestStart();
					return action.doInDB(db);
				} finally {
					db.requestDone();
				}
			}
		});
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#createCollection(java.lang.String)
	 */
	public DBCollection createCollection(final String collectionName) {
		return execute(new DBCallback<DBCollection>() {
			public DBCollection doInDB(DB db) throws MongoException, DataAccessException {
				return db.createCollection(collectionName, new BasicDBObject());
			}
		});
	}
		
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#createCollection(java.lang.String, org.springframework.data.document.mongodb.CollectionOptions)
	 */
	public void createCollection(final String collectionName, final CollectionOptions collectionOptions) {
		execute(new DBCallback<Void>() {
			public Void doInDB(DB db) throws MongoException, DataAccessException {
				db.createCollection(collectionName, convertToDbObject(collectionOptions));
				return null;
			}
		});
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.String)
	 */
	public DBCollection getCollection(final String collectionName) {
		return execute(new DBCallback<DBCollection>() {
			public DBCollection doInDB(DB db) throws MongoException, DataAccessException {
				return db.getCollection(collectionName);
			}
		});
	}
		

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#collectionExists(java.lang.String)
	 */
	public boolean collectionExists(final String collectionName) {
		return execute(new DBCallback<Boolean>() {
			public Boolean doInDB(DB db) throws MongoException, DataAccessException {
				return db.collectionExists(collectionName);
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#dropCollection(java.lang.String)
	 */
	public void dropCollection(String collectionName) {
		
		execute(new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.drop();
				return null;
			}
		}, collectionName);
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
		Object id = insertDBObject(collectionName, dbDoc);
		populateIdIfNecessary(objectToSave, id);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.util.List)
	 */
	public void insertList(List<? extends Object> listToSave) {
		insertList(getRequiredDefaultCollectionName(), listToSave);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.lang.String, java.util.List)
	 */
	public void insertList(String collectionName, List<? extends Object> listToSave) {
		insertList(collectionName, listToSave, this.mongoConverter);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#insertList(java.lang.String, java.util.List, org.springframework.data.document.mongodb.MongoWriter)
	 */
	public <T> void insertList(String collectionName, List<? extends T> listToSave, MongoWriter<T> writer) {
		
		Assert.notNull(writer);
		
		List<DBObject> dbObjectList = new ArrayList<DBObject>();
		for (T o : listToSave) {
			BasicDBObject dbDoc = new BasicDBObject();
			writer.write(o, dbDoc);
			dbObjectList.add(dbDoc);
		}
		List<Object> ids = insertDBObjectList(collectionName, dbObjectList);
		for (int i = 0; i < listToSave.size(); i++) {
			if (i < ids.size()) {
				populateIdIfNecessary(listToSave.get(i), ids.get(i));
			}
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
		Object id = saveDBObject(collectionName, dbDoc);
		populateIdIfNecessary(objectToSave, id);
	}


	protected Object insertDBObject(String collectionName, final DBObject dbDoc) {

		if (dbDoc.keySet().isEmpty()) {
			return null;
		}
		
		return execute(new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.insert(dbDoc);
				return dbDoc.get(ID);
			}
		}, collectionName);
	}
	
	
	

	protected List<Object> insertDBObjectList(String collectionName, final List<DBObject> dbDocList) {
		
		if (dbDocList.isEmpty()) {
			return Collections.emptyList();
		}

		execute(new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.insert(dbDocList);
				return null;
			}
		}, collectionName);

		List<Object> ids = new ArrayList<Object>();
		for (DBObject dbo : dbDocList) {
			ids.add(dbo.get(ID));
		}
		return ids;
	}

	protected Object saveDBObject(String collectionName, final DBObject dbDoc) {
		
		if (dbDoc.keySet().isEmpty()) {
			return null;
		}
		
		return execute(new CollectionCallback<Object>() {

			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.save(dbDoc);
				return dbDoc.get(ID);
			}
		}, collectionName);
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
	public void updateFirst(String collectionName, final DBObject queryDoc, final DBObject updateDoc) {
		execute(new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.update(queryDoc, updateDoc);
				return null;
			}
		}, collectionName);
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
	public void updateMulti(String collectionName, final DBObject queryDoc, final DBObject updateDoc) {
		execute(new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.updateMulti(queryDoc, updateDoc);
				return null;
			}
		}, collectionName);
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
	public void remove(String collectionName, final DBObject queryDoc) {
		execute(new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.remove(queryDoc);
				return null;
			}
		}, collectionName);
	}
	

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.Class)
	 */
	public <T> List<T> getCollection(Class<T> targetClass) {
		return executeEach(new FindCallback(null), null, new ReadDbObjectCallback<T>(mongoConverter, targetClass),
				getDefaultCollectionName());
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.String, java.lang.Class)
	 */
	public <T> List<T> getCollection(String collectionName, Class<T> targetClass) {
		return executeEach(new FindCallback(null), null, new ReadDbObjectCallback<T>(mongoConverter, targetClass),
				collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollectionNames()
	 */
	public Set<String> getCollectionNames() {
		return execute(new DBCallback<Set<String>>() {
			public Set<String> doInDB(DB db) throws MongoException, DataAccessException {
				return db.getCollectionNames();
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#getCollection(java.lang.String, java.lang.Class, org.springframework.data.document.mongodb.MongoReader)
	 */
	public <T> List<T> getCollection(String collectionName, Class<T> targetClass, MongoReader<T> reader) {
		return executeEach(new FindCallback(null), null, new ReadDbObjectCallback<T>(reader, targetClass),
				collectionName);
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
		return executeEach(new FindCallback(query), preparer, new ReadDbObjectCallback<T>(mongoConverter, targetClass),
				collectionName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoOperations#query(java.lang.String, com.mongodb.DBObject, java.lang.Class, org.springframework.data.document.mongodb.MongoReader)
	 */
	public <T> List<T> query(String collectionName, DBObject query, Class<T> targetClass, MongoReader<T> reader) {
		return executeEach(new FindCallback(query), null, new ReadDbObjectCallback<T>(reader, targetClass),
				collectionName);
	}


	public DB getDb() {
		return MongoDbUtils.getDB(mongo, databaseName, username, password == null ? null : password.toCharArray());
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
			idPd = BeanUtils.getPropertyDescriptor(savedObject.getClass(), ID);
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
	
	/**
	 * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
	 * exception if the conversation failed. Thus allows safe rethrowing of the return value.
	 * 
	 * @param ex
	 * @return
	 */
	private RuntimeException potentiallyConvertRuntimeException(RuntimeException ex) {

		RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(ex);
    	return resolved == null ? ex : resolved;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {
		if (this.getDefaultCollectionName() != null) {
			if (!collectionExists(getDefaultCollectionName())) {
				createCollection(getDefaultCollectionName(), null);
			}
		}
	}
	
	
	/**
	 * Simple {@link CollectionCallback} that takes a query {@link DBObject} and executes that against the
	 * {@link DBCollection}.
	 * 
	 * @author Oliver Gierke
	 */
	private static class FindCallback implements CollectionCallback<DBCursor> {
		
		private final DBObject query;
		
		public FindCallback(DBObject query) {
			this.query = query;
		}

		public DBCursor doInCollection(DBCollection collection) throws MongoException, DataAccessException {
			return collection.find(query);
		}
	}
	
	/**
	 * Simple internal callback to allow operations on a {@link DBObject}.
	 *
	 * @author Oliver Gierke
	 */
	
	private interface DbObjectCallback<T> {
		
		T doWith(DBObject object);
	}
	
	/**
	 * Simple {@link DbObjectCallback} that will transform {@link DBObject} into the given target type using the given
	 * {@link MongoReader}.
	 * 
	 * @author Oliver Gierke
	 */
	private static class ReadDbObjectCallback<T> implements DbObjectCallback<T> {
		
		private final MongoReader<? super T> reader;
		private final Class<T> type;
		
		public ReadDbObjectCallback(MongoReader<? super T> reader, Class<T> type) {
			this.reader = reader;
			this.type = type;
		}
		
		@SuppressWarnings("unchecked")
		public T doWith(DBObject object) {
			return (T) reader.read(type, object);
		}
	}
}
