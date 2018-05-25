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

import reactor.core.publisher.Mono;

import org.bson.Document;

import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.Success;

/**
 * @author Christoph Strobl
 */
public class MongoTestUtils {

	public static final String CONNECTION_STRING = "mongodb://localhost:27017/?replicaSet=rs0"; // &readPreference=primary&w=majority

	/**
	 * Create a {@link com.mongodb.client.MongoCollection} if it does not exist, or drop and recreate it if it does.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static MongoCollection<Document> createOrReplaceCollection(String dbName, String collectionName,
			com.mongodb.MongoClient client) {

		MongoDatabase database = client.getDatabase(dbName).withWriteConcern(WriteConcern.MAJORITY)
				.withReadPreference(ReadPreference.primary());

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
	 * Create a {@link com.mongodb.client.MongoCollection} if it does not exist, or drop and recreate it if it does.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static Mono<Success> createOrReplaceCollection(String dbName, String collectionName,
			com.mongodb.reactivestreams.client.MongoClient client) {

		com.mongodb.reactivestreams.client.MongoDatabase database = client.getDatabase(dbName)
				.withWriteConcern(WriteConcern.MAJORITY).withReadPreference(ReadPreference.primary());

		return Mono.from(database.getCollection(collectionName).drop())
				.then(Mono.from(database.createCollection(collectionName)));
	}

	/**
	 * Create a new {@link com.mongodb.MongoClient} with defaults suitable for replica set usage.
	 *
	 * @return new instance of {@link com.mongodb.MongoClient}.
	 */
	public static com.mongodb.MongoClient replSetClient() {

		return new com.mongodb.MongoClient(new MongoClientURI(CONNECTION_STRING));
	}

	/**
	 * Create a new {@link com.mongodb.reactivestreams.client.MongoClient} with defaults suitable for replica set usage.
	 *
	 * @return new instance of {@link com.mongodb.reactivestreams.client.MongoClient}.
	 */
	public static com.mongodb.reactivestreams.client.MongoClient reactiveReplSetClient() {
		return MongoClients.create(CONNECTION_STRING);
	}

}
