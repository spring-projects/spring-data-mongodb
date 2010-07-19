package org.springframework.datastore.document.mongodb;


import java.util.List;

import org.springframework.data.core.DataMapper;
import org.springframework.data.core.QueryDefinition;
import org.springframework.datastore.core.AbstractDatastoreTemplate;

import com.mongodb.DB;
import com.mongodb.Mongo;

public class MongoDatastoreTemplate extends AbstractDatastoreTemplate<DB> {

	private Mongo mongo;
	
	
	public MongoDatastoreTemplate() {
		super();
	}
	
	public MongoDatastoreTemplate(Mongo mongo, String databaseName) {
		super();
		this.mongo = mongo;
		setDatastoreConnectionFactory(new MongoConnectionFactory(mongo, databaseName));
	}


	public Mongo getMongo() {
		return mongo;
	}

	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}


	@Override
	public <S, T> List<T> query(QueryDefinition arg0, DataMapper<S, T> arg1) {
		return null;
	}

	
	
}
