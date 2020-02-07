/*
 * Copyright 2011-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb;

import org.springframework.dao.DataAccessException;

import com.mongodb.client.MongoDatabase;

/**
 * Interface for factories creating {@link MongoDatabase} instances.
 *
 * @author Mark Pollack
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @deprecated since 3.0, use {@link MongoDatabaseFactory} instead.
 */
@Deprecated
public interface MongoDbFactory extends MongoDatabaseFactory {

	/**
	 * Creates a default {@link MongoDatabase} instance.
	 *
	 * @return
	 * @throws DataAccessException
	 * @deprecated since 3.0. Use {@link #getMongoDatabase()} instead.
	 */
	@Deprecated
	default MongoDatabase getDb() throws DataAccessException {
		return getMongoDatabase();
	}

	/**
	 * Obtain a {@link MongoDatabase} instance to access the database with the given name.
	 *
	 * @param dbName must not be {@literal null} or empty.
	 * @return
	 * @throws DataAccessException
	 * @deprecated since 3.0. Use {@link #getMongoDatabase(String)} instead.
	 */
	@Deprecated
	default MongoDatabase getDb(String dbName) throws DataAccessException {
		return getMongoDatabase(dbName);
	}
}
