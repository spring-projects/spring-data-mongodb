package org.springframework.data.document.mongodb;

import org.springframework.dao.DataAccessException;

import com.mongodb.DB;

public interface MongoDbFactory {

	DB getDb() throws DataAccessException;
	
	DB getDb(String dbName) throws DataAccessException;
	
}
