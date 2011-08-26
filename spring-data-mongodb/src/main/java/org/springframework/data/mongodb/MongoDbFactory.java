package org.springframework.data.mongodb;

import org.springframework.dao.DataAccessException;

import com.mongodb.DB;

/**
 * Interface for factories creating {@link DB} instances.
 * 
 * @author Mark Pollack
 */
public interface MongoDbFactory {

	/**
	 * Creates a default {@link DB} instance.
	 * 
	 * @return
	 * @throws DataAccessException
	 */
	DB getDb() throws DataAccessException;

	/**
	 * Creates a {@link DB} instance to access the database with the given name.
	 * 
	 * @param dbName must not be {@literal null} or empty.
	 * @return
	 * @throws DataAccessException
	 */
	DB getDb(String dbName) throws DataAccessException;
}
