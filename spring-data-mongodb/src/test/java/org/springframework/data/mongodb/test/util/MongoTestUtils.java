/*
 * Copyright 2018-2019 the original author or authors.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.Success;

/**
 * Utility to create (and reuse) imperative and reactive {@code MongoClient} instances.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoTestUtils {

	public static final String CONNECTION_STRING = "mongodb://localhost:27017/?replicaSet=rs0"; // &readPreference=primary&w=majority

	private static final String CONNECTION_STRING_PATTERN = "mongodb://%s:%s/";
	private static final Map<String, com.mongodb.client.MongoClient> CLIENT_CACHE = new HashMap<>();
	private static final Map<String, com.mongodb.reactivestreams.client.MongoClient> REACTIVE_CLIENT_CACHE = new HashMap<>();

	/**
	 * Create a new {@link com.mongodb.client.MongoClient} with defaults.
	 *
	 * @return new instance of {@link com.mongodb.client.MongoClient}.
	 */
	public static MongoClient client() {
		return client("localhost", 27017);
	}

	public static MongoClient client(String host, int port) {
		return getOrCreate(String.format(CONNECTION_STRING_PATTERN, host, port));
	}

	/**
	 * Create a new {@link com.mongodb.reactivestreams.client.MongoClient} with defaults.
	 *
	 * @return new instance of {@link com.mongodb.reactivestreams.client.MongoClient}.
	 */
	public static com.mongodb.reactivestreams.client.MongoClient reactiveClient() {
		return reactiveClient("localhost", 27017);
	}

	public static com.mongodb.reactivestreams.client.MongoClient reactiveClient(String host, int port) {
		return getOrCreateReactive(String.format(CONNECTION_STRING_PATTERN, host, port));
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

		return Mono.from(database.getCollection(collectionName).drop()) //
				.delayElement(Duration.ofMillis(10)) // server replication time
				.then(Mono.from(database.createCollection(collectionName))) //
				.delayElement(Duration.ofMillis(10)); // server replication time
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
				.expectNext(Success.SUCCESS) //
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
				.retryBackoff(3, Duration.ofMillis(250)) //
				.as(StepVerifier::create) //
				.expectNext(Success.SUCCESS) //
				.verifyComplete();
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
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	/**
	 * Create a new {@link com.mongodb.MongoClient} with defaults suitable for replica set usage.
	 *
	 * @return new instance of {@link com.mongodb.MongoClient}.
	 */
	public static com.mongodb.client.MongoClient replSetClient() {
		return getOrCreate(CONNECTION_STRING);
	}

	/**
	 * Create a new {@link com.mongodb.reactivestreams.client.MongoClient} with defaults suitable for replica set usage.
	 *
	 * @return new instance of {@link com.mongodb.reactivestreams.client.MongoClient}.
	 */
	public static com.mongodb.reactivestreams.client.MongoClient reactiveReplSetClient() {
		return getOrCreateReactive(CONNECTION_STRING);
	}

	private static com.mongodb.client.MongoClient getOrCreate(String connectionString) {
		return CLIENT_CACHE.computeIfAbsent(connectionString,
				key -> wrapClient(com.mongodb.client.MongoClients.create(key)));
	}

	private static com.mongodb.reactivestreams.client.MongoClient getOrCreateReactive(String connectionString) {
		return REACTIVE_CLIENT_CACHE.computeIfAbsent(connectionString, key -> wrapClient(MongoClients.create(key)));
	}

	@SuppressWarnings("unchecked")
	private static <T> T wrapClient(T client) {

		CloseSurpressingInvocationHandler ih = new CloseSurpressingInvocationHandler(client);
		Object proxy = Proxy.newProxyInstance(MongoTestUtils.class.getClassLoader(), ClassUtils.getAllInterfaces(client),
				ih);
		return (T) proxy;

	}

	static class CloseSurpressingInvocationHandler implements InvocationHandler {

		private final Object mongoClient;
		private final Map<Method, Method> methodCache = new HashMap<>();

		public CloseSurpressingInvocationHandler(Object mongoClient) {
			this.mongoClient = mongoClient;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

			if (method.getName().equals("close")) {
				throw new UnsupportedOperationException("Cannot close cached MongoClient. See MongoTestUtils.");
			}

			Method toInvoke = methodCache.computeIfAbsent(method, key -> {
				return ReflectionUtils.findMethod(mongoClient.getClass(), key.getName(), key.getParameterTypes());
			});
			return toInvoke.invoke(mongoClient, args);
		}
	}

}
