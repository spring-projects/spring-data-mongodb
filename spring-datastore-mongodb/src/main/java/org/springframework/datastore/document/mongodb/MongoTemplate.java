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

package org.springframework.datastore.document.mongodb;


import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.datastore.document.AbstractDocumentStoreTemplate;
import org.springframework.datastore.document.DocumentMapper;
import org.springframework.datastore.document.DocumentSource;
import org.springframework.datastore.document.mongodb.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class MongoTemplate extends AbstractDocumentStoreTemplate<DB> implements InitializingBean {

	private DB db;
	
	private String defaultCollectionName;
	
	//TODO expose configuration...
	private CollectionOptions defaultCollectionOptions;
	
//	public MongoTemplate() {
//		super();
//	}
	
	public MongoTemplate(DB db) {
		super();
		this.db = db;
	}
	
	

	public String getDefaultCollectionName() {
		return defaultCollectionName;
	}

	//TODO would one ever consider passing in a DBCollection object?

	public void setDefaultCollectionName(String defaultCollection) {
		this.defaultCollectionName = defaultCollection;
	}



	public void execute(String command) {
		execute((DBObject)JSON.parse(command));
	}
	
	public void execute(DocumentSource<DBObject> command) {
		execute(command.getDocument());
	}
	
	public void execute(DBObject command) {
		CommandResult cr = getConnection().command(command);
		String err = cr.getErrorMessage();
		if (err != null) {
			throw new InvalidDataAccessApiUsageException("Command execution of " + 
					command.toString() + " failed: " + err);
		}
	}

	public DBCollection createCollection(String collectionName) {
		try {
			return getConnection().createCollection(collectionName, null);
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}
		
	public void createCollection(String collectionName, CollectionOptions collectionOptions) {
		try {
			getConnection().createCollection(collectionName, convertToDbObject(collectionOptions));
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}

	public boolean collectionExists(String collectionName) {
		try {
			return getConnection().collectionExists(collectionName);
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}

	public void dropCollection(String collectionName) {
		getConnection().getCollection(collectionName)
			.drop();
	}
	
	public void saveObject(Object object) {
		saveObject(getRequiredDefaultCollectionName(), object);
	}

	private String getRequiredDefaultCollectionName() {
		String name = getDefaultCollectionName();
		if (name == null) {
			throw new IllegalStateException(
					"No 'defaultCollection' or 'defaultCollectionName' specified. Check configuration of MongoTemplate.");
		}
		return name;
	}


	public void saveObject(String collectionName, Object source) {
		MongoBeanPropertyDocumentSource docSrc = new MongoBeanPropertyDocumentSource(source);
		save(collectionName, docSrc);
	}
	
	public void save(String collectionName, DocumentSource<DBObject> documentSource) {
		DBObject dbDoc = documentSource.getDocument();		
		WriteResult wr = null;
		try {
			wr = getConnection().getCollection(collectionName).save(dbDoc);			
		} catch (MongoException e) {
			throw new DataRetrievalFailureException(wr.getLastError().getErrorMessage(), e);
		}
	}
	
	public <T> List<T> queryForCollection(String collectionName, Class<T> targetClass) {
		DocumentMapper<DBObject, T> mapper = MongoBeanPropertyDocumentMapper.newInstance(targetClass);
		return queryForCollection(collectionName, mapper);
	}

	public <T> List<T> queryForCollection(String collectionName, DocumentMapper<DBObject, T> mapper) {
		List<T> results = new ArrayList<T>();
		DBCollection collection = getConnection().getCollection(collectionName);
		for (DBObject dbo : collection.find()) {
			results.add(mapper.mapDocument(dbo));
		}
		return results;
	}
	
	public <T> List<T> queryForList(String collectionName, Query query, Class<T> targetClass) {
		DocumentMapper<DBObject, T> mapper = MongoBeanPropertyDocumentMapper.newInstance(targetClass);
		return queryForList(collectionName, query, mapper);
	}

	public <T> List<T> queryForList(String collectionName, Query query, DocumentMapper<DBObject, T> mapper) {
		return queryForList(collectionName, query.getQueryObject(), mapper);
	}

	public <T> List<T> queryForList(String collectionName, String query, Class<T> targetClass) {
		DocumentMapper<DBObject, T> mapper = MongoBeanPropertyDocumentMapper.newInstance(targetClass);
		return queryForList(collectionName, query, mapper);
	}

	public <T> List<T> queryForList(String collectionName, String query, DocumentMapper<DBObject, T> mapper) {
		return queryForList(collectionName, (DBObject)JSON.parse(query), mapper);
	}

	public <T> List<T> queryForList(String collectionName, DBObject query, Class<T> targetClass) {
		DocumentMapper<DBObject, T> mapper = MongoBeanPropertyDocumentMapper.newInstance(targetClass);
		return queryForList(collectionName, query, mapper);
	}

	public <T> List<T> queryForList(String collectionName, DBObject query, DocumentMapper<DBObject, T> mapper) {
		DBCollection collection = getConnection().getCollection(collectionName);
		List<T> results = new ArrayList<T>();
		for (DBObject dbo : collection.find(query)) {
			results.add(mapper.mapDocument(dbo));
		}
		return results;
	}

	public RuntimeException translateIfNecessary(RuntimeException ex) {
		return MongoDbUtils.translateMongoExceptionIfPossible(ex);
	}

	@Override
	public DB getConnection() {
		return db;
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



	public void afterPropertiesSet() throws Exception {
		if (this.getDefaultCollectionName() != null) {
			if (! db.collectionExists(getDefaultCollectionName())) {
				db.createCollection(getDefaultCollectionName(), null);
			}
		}
		
	}
}
