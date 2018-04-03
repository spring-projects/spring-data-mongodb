/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author Christoph Strobl
 */
public class MongoTestUtils {

	/**
	 * Create a {@link com.mongodb.client.MongoCollection} if it does not exist, or drop and recreate it if it does.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static MongoCollection<Document> createOrReplaceCollection(String dbName, String collectionName,
			MongoClient client) {

		MongoDatabase database = client.getDatabase(dbName);

		boolean collectionExists = database.listCollections().filter(new Document("name", collectionName)).first() != null;

		if (collectionExists) {
			database.getCollection(collectionName).drop();
		}

		database.createCollection(collectionName);

		try {
			Thread.sleep(10); // server replication time
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return database.getCollection(collectionName);
	}

	/**
	 * Create a new {@link MongoClient} with defaults suitable for replica set usage.
	 *
	 * @return new instance of {@link MongoClient}.
	 */
	public static MongoClient replSetClient() {

		return new MongoClient("localhost",
				MongoClientOptions.builder().requiredReplicaSetName("rs0").build());
	}

}
