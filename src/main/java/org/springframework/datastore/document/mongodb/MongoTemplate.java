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

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.datastore.document.AbstractDocumentStoreTemplate;
import org.springframework.datastore.document.DocumentMapper;
import org.springframework.datastore.document.DocumentSource;
import org.springframework.datastore.document.InvalidDocumentStoreApiUageException;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class MongoTemplate extends AbstractDocumentStoreTemplate<DB> {

	private DB db;
	
//	public MongoTemplate() {
//		super();
//	}
	
	public MongoTemplate(DB db) {
		super();
		this.db = db;
	}

	public void execute(String command) {
		CommandResult cr = getConnection().command(command);
		String err = cr.getErrorMessage();
		if (err != null) {
			throw new InvalidDataAccessApiUsageException("Command execution of " + 
					command + " failed: " + err);
		}
	}
	
	public void execute(DocumentSource<DBObject> command) {
		CommandResult cr = getConnection().command(command.getDocument());
		String err = cr.getErrorMessage();
		if (err != null) {
			throw new InvalidDataAccessApiUsageException("Command execution of " + 
					command.getDocument().toString() + " failed: " + err);
		}
	}
	
	public void createCollection(String collectionName, DocumentSource<DBObject> documentSource) {
		try {
			getConnection().createCollection(collectionName, documentSource.getDocument());
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}

	public void dropCollection(String collectionName) {
		getConnection().getCollection(collectionName)
			.drop();
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
	
	public <T> List<T> queryForList(String collectionName, String fieldName, String[] operands, Object[] values, Class<T> targetClass) {
		DocumentMapper<DBObject, T> mapper = MongoBeanPropertyDocumentMapper.newInstance(targetClass);
		return queryForList(collectionName, fieldName, operands, values, mapper);
	}

	public <T> List<T> queryForList(String collectionName, String fieldName, String[] operands, Object[] values, DocumentMapper<DBObject, T> mapper) {
		if (operands.length != values.length) {
			throw new InvalidDocumentStoreApiUageException("The number of operands and values must match");
		}
		List<T> results = new ArrayList<T>();
		DBObject query = new BasicDBObject();
		DBObject condition = new BasicDBObject();
		for (int i = 0; i < operands.length; i++) {
			condition.put(operands[i], values[i]);
		}
		query.put(fieldName, condition);
		System.out.println("--> " + query);
		DBCollection collection = getConnection().getCollection(collectionName);
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

}
