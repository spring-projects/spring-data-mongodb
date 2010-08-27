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
import org.springframework.datastore.document.DocumentStoreConnectionFactory;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class MongoTemplate extends AbstractDocumentStoreTemplate<DB> {

	private DocumentStoreConnectionFactory<DB> connectionFactory;
	
	public MongoTemplate() {
		super();
	}
	
	public MongoTemplate(Mongo mongo, String databaseName) {
		super();
		connectionFactory = new MongoDbConnectionFactory(mongo, databaseName);
	}

	public MongoTemplate(MongoDbConnectionFactory mcf) {
		super();
		connectionFactory = mcf;
	}

	@Override
	public DocumentStoreConnectionFactory<DB> getDocumentStoreConnectionFactory() {
		return connectionFactory;
	}

	
	public void execute(DocumentSource<DBObject> command) {
		CommandResult cr = getDocumentStoreConnectionFactory()
			.getConnection()
			.command(command.getDocument());
		System.out.println("! " + cr.getErrorMessage());
	}
	
	public void createCollection(String collectionName, DocumentSource<DBObject> documentSource) {
		DB db =  getDocumentStoreConnectionFactory().getConnection();
		try {
			db.createCollection(collectionName, documentSource.getDocument());
		} catch (MongoException e) {
			throw new InvalidDataAccessApiUsageException("Error creating collection " + collectionName + ": " + e.getMessage(), e);
		}
	}

	public void dropCollection(String collectionName) {
		getDocumentStoreConnectionFactory()
			.getConnection()
			.getCollection(collectionName)
			.drop();
	}

	public void saveObject(String collectionName, Object source) {
		MongoBeanPropertyDocumentSource docSrc = new MongoBeanPropertyDocumentSource(source);
		save(collectionName, docSrc);
	}
	
	public void save(String collectionName, DocumentSource<DBObject> documentSource) {
		DBObject dbDoc = documentSource.getDocument();		
		DB db =  getDocumentStoreConnectionFactory().getConnection();
		WriteResult wr = null;
		try {
			wr = db.getCollection(collectionName).save(dbDoc);
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
		DB db =  getDocumentStoreConnectionFactory().getConnection();
		DBCollection collection = db.getCollection(collectionName);
		for (DBObject dbo : collection.find()) {
			results.add(mapper.mapDocument(dbo));
		}
		return results;
	}
	
}
