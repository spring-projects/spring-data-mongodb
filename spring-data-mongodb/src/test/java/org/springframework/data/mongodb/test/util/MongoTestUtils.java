/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;

import org.bson.Document;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.mongodb.SpringDataMongoDB;
import org.springframework.data.util.Version;
import org.springframework.util.ObjectUtils;

import com.mongodb.ConnectionString;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Utility to create (and reuse) imperative and reactive {@code MongoClient} instances.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoTestUtils {

	private static final Environment ENV = new StandardEnvironment();
	private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(10);

	public static final String CONNECTION_STRING = "mongodb://127.0.0.1:27017/?replicaSet=rs0&w=majority&uuidrepresentation=javaLegacy";

	private static final String CONNECTION_STRING_PATTERN = "mongodb://%s:%s/?w=majority&uuidrepresentation=javaLegacy";

	private static final Version ANY = new Version(9999, 9999, 9999);

	/**
	 * Create a new {@link com.mongodb.client.MongoClient} with defaults.
	 *
	 * @return new instance of {@link com.mongodb.client.MongoClient}.
	 */
	public static MongoClient client() {
		return client("127.0.0.1", 27017);
	}

	public static MongoClient client(String host, int port) {

		ConnectionString connectionString = new ConnectionString(String.format(CONNECTION_STRING_PATTERN, host, port));
		return com.mongodb.client.MongoClients.create(connectionString, SpringDataMongoDB.driverInformation());
	}

	/**
	 * Create a new {@link com.mongodb.reactivestreams.client.MongoClient} with defaults.
	 *
	 * @return new instance of {@link com.mongodb.reactivestreams.client.MongoClient}.
	 */
	public static com.mongodb.reactivestreams.client.MongoClient reactiveClient() {
		return reactiveClient("127.0.0.1", 27017);
	}

	public static com.mongodb.reactivestreams.client.MongoClient reactiveClient(String host, int port) {

		ConnectionString connectionString = new ConnectionString(String.format(CONNECTION_STRING_PATTERN, host, port));
		return MongoClients.create(connectionString, SpringDataMongoDB.driverInformation());
	}

	/**
	 * Create a {@link com.mongodb.client.MongoCollection} if it does not exist, or drop and recreate it if it does.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static MongoCollection<Document> createOrReplaceCollection(String dbName, String collectionName,
			com.mongodb.client.MongoClient client) {

		MongoDatabase database = client.getDatabase(dbName).withWriteConcern(WriteConcern.MAJORITY)
				.withReadPreference(ReadPreference.primary());

		boolean collectionExists = database.listCollections().filter(new Document("name", collectionName)).first() != null;

		if (collectionExists) {

			database.getCollection(collectionName).drop();
			giveTheServerALittleTimeToThink();
		}

		database.createCollection(collectionName);
		giveTheServerALittleTimeToThink();

		return database.getCollection(collectionName);
	}

	/**
	 * Create a {@link com.mongodb.client.MongoCollection} if it does not exist, or drop and recreate it if it does.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static Mono<Void> createOrReplaceCollection(String dbName, String collectionName,
			com.mongodb.reactivestreams.client.MongoClient client) {

		com.mongodb.reactivestreams.client.MongoDatabase database = client.getDatabase(dbName)
				.withWriteConcern(WriteConcern.MAJORITY).withReadPreference(ReadPreference.primary());

		return Mono.from(database.getCollection(collectionName).drop()) //
				.delayElement(getTimeout()) // server replication time
				.then(Mono.from(database.createCollection(collectionName))) //
				.delayElement(getTimeout()); // server replication time
	}

	/**
	 * Create a {@link com.mongodb.client.MongoCollection} if it does not exist, or drop and recreate it if it does and
	 * verify operation result.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static void createOrReplaceCollectionNow(String dbName, String collectionName,
			com.mongodb.reactivestreams.client.MongoClient client) {

		createOrReplaceCollection(dbName, collectionName, client) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	/**
	 * Create a {@link com.mongodb.client.MongoCollection} if it does not exist, or drop and recreate it if it does and
	 * verify operation result.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static void dropCollectionNow(String dbName, String collectionName,
			com.mongodb.reactivestreams.client.MongoClient client) {

		com.mongodb.reactivestreams.client.MongoDatabase database = client.getDatabase(dbName)
				.withWriteConcern(WriteConcern.MAJORITY).withReadPreference(ReadPreference.primary());

		Mono.from(database.getCollection(collectionName).drop()) //
				.delayElement(getTimeout()).retryWhen(Retry.backoff(3, Duration.ofMillis(250))) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	/**
	 * Create a {@link com.mongodb.client.MongoCollection} if it does not exist, or drop and recreate it if it does and
	 * verify operation result.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static void dropCollectionNow(String dbName, String collectionName,
			com.mongodb.client.MongoClient client) {

		com.mongodb.client.MongoDatabase database = client.getDatabase(dbName)
				.withWriteConcern(WriteConcern.MAJORITY).withReadPreference(ReadPreference.primary());

		database.getCollection(collectionName).drop();
	}

	/**
	 * Remove all documents from the {@link MongoCollection} with given name in the according {@link MongoDatabase
	 * database}.
	 *
	 * @param dbName must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 */
	public static void flushCollection(String dbName, String collectionName,
			com.mongodb.reactivestreams.client.MongoClient client) {

		com.mongodb.reactivestreams.client.MongoDatabase database = client.getDatabase(dbName)
				.withWriteConcern(WriteConcern.MAJORITY).withReadPreference(ReadPreference.primary());

		Mono.from(database.getCollection(collectionName).deleteMany(new Document())) //
				.delayElement(getTimeout()).then() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	public static void flushCollection(String dbName, String collectionName,
			com.mongodb.client.MongoClient client) {

		com.mongodb.client.MongoDatabase database = client.getDatabase(dbName)
				.withWriteConcern(WriteConcern.MAJORITY).withReadPreference(ReadPreference.primary());

		database.getCollection(collectionName).deleteMany(new Document());
	}

	/**
	 * Create a new {@link com.mongodb.client.MongoClient} with defaults suitable for replica set usage.
	 *
	 * @return new instance of {@link com.mongodb.client.MongoClient}.
	 */
	public static com.mongodb.client.MongoClient replSetClient() {
		return com.mongodb.client.MongoClients.create(CONNECTION_STRING);
	}

	/**
	 * Create a new {@link com.mongodb.reactivestreams.client.MongoClient} with defaults suitable for replica set usage.
	 *
	 * @return new instance of {@link com.mongodb.reactivestreams.client.MongoClient}.
	 */
	public static com.mongodb.reactivestreams.client.MongoClient reactiveReplSetClient() {
		return MongoClients.create(CONNECTION_STRING);
	}

	/**
	 * @return the server version extracted from buildInfo.
	 * @since 3.0
	 */
	public static Version serverVersion() {

		try (MongoClient client = client()) {

			MongoDatabase database = client.getDatabase("test");
			Document result = database.runCommand(new Document("buildInfo", 1));

			return Version.parse(result.get("version", String.class));
		} catch (Exception e) {
			return ANY;
		}
	}

	/**
	 * @return check if the server is running as part of a replica set.
	 * @since 3.0
	 */
	public static boolean serverIsReplSet() {

		try (MongoClient client = MongoTestUtils.client()) {

			return client.getDatabase("admin").runCommand(new Document("getCmdLineOpts", "1")).get("argv", List.class)
					.contains("--replSet");
		} catch (Exception e) {
			return false;
		}
	}

	public static Duration getTimeout() {

		return ObjectUtils.nullSafeEquals("jenkins", ENV.getProperty("user.name")) ? Duration.ofMillis(100)
				: DEFAULT_TIMEOUT;
	}

	private static void giveTheServerALittleTimeToThink() {

		try {
			Thread.sleep(getTimeout().toMillis()); // server replication time
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static CollectionInfo readCollectionInfo(MongoDatabase db, String collectionName) {

		List<Document> list = db.runCommand(new Document().append("listCollections", 1).append("filter", new Document("name", collectionName)))
				.get("cursor", Document.class).get("firstBatch", List.class);

		if(list.isEmpty()) {
			throw new IllegalStateException(String.format("Collection %s not found.", collectionName));
		}
		return CollectionInfo.from(list.get(0));
	}
}
