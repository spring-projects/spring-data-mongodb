/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.core;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Helper class featuring helper methods for internal MongoDb classes. Mainly intended for internal use within the
 * framework.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public abstract class ReactiveMongoDbUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ReactiveMongoDbUtils() {}

	/**
	 * Obtains a {@link MongoDatabase} connection for the given {@link MongoClient} instance and database name
	 *
	 * @param mongo the {@link MongoClient} instance, must not be {@literal null}.
	 * @param databaseName the database name, must not be {@literal null} or empty.
	 * @return the {@link MongoDatabase} connection
	 */
	public static MongoDatabase getMongoDatabase(MongoClient mongo, String databaseName) {
		return doGetMongoDatabase(mongo, databaseName, true);
	}

	private static MongoDatabase doGetMongoDatabase(MongoClient mongo, String databaseName, boolean allowCreate) {
		return mongo.getDatabase(databaseName);
	}

}
