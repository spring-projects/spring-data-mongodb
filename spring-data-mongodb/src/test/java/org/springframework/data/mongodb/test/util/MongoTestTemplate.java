/*
 * Copyright 2020-2025 the original author or authors.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.util.MongoCompatibilityAdapter;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * A {@link MongoTemplate} with configuration hooks and extension suitable for tests.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
public class MongoTestTemplate extends MongoTemplate {

	private final MongoTestTemplateConfiguration cfg;

	public MongoTestTemplate(MongoClient client, String database, Class<?>... initialEntities) {
		this(cfg -> {
			cfg.configureDatabaseFactory(it -> {

				it.client(client);
				it.defaultDb(database);
			});

			cfg.configureMappingContext(it -> {

				it.autocreateIndex(false);
				it.initialEntitySet(initialEntities);
			});
		});
	}

	public MongoTestTemplate(Consumer<MongoTestTemplateConfiguration> cfg) {

		this(() -> {

			MongoTestTemplateConfiguration config = new MongoTestTemplateConfiguration();
			cfg.accept(config);
			return config;
		});
	}

	public MongoTestTemplate(Supplier<MongoTestTemplateConfiguration> config) {
		this(config.get());
	}

	public MongoTestTemplate(MongoTestTemplateConfiguration config) {
		super(config.databaseFactory(), config.mongoConverter());

		ApplicationContext applicationContext = config.getApplicationContext();
		EntityCallbacks callbacks = config.getEntityCallbacks();
		if (callbacks != null) {
			setEntityCallbacks(callbacks);
		}
		if (applicationContext != null) {
			setApplicationContext(applicationContext);
		}

		this.cfg = config;
	}

	public void flush() {
		flush(PersistentEntities.of(getConverter().getMappingContext()).stream().map(it -> getCollectionName(it.getType()))
				.collect(Collectors.toList()));
	}

	public void flushDatabase() {
		flush(MongoCompatibilityAdapter.mongoDatabaseAdapter().forDb(getDb()).listCollectionNames());
	}

	public void flush(Iterable<String> collections) {

		for (String collection : collections) {
			MongoCollection<Document> mongoCollection = getCollection(collection);
			try {
				mongoCollection.deleteMany(new Document());
			} catch (MongoWriteException e) {
				mongoCollection.drop();
			}
		}
	}

	public void flush(Class<?>... entities) {
		flush(Arrays.stream(entities).map(this::getCollectionName).collect(Collectors.toList()));
	}

	public void flush(String... collections) {
		flush(Arrays.asList(collections));
	}

	public void flush(Object... objects) {

		flush(Arrays.stream(objects).map(it -> {

			if (it instanceof String) {
				return (String) it;
			}
			if (it instanceof Class) {
				return getCollectionName((Class<?>) it);
			}
			return it.toString();
		}).collect(Collectors.toList()));
	}

	public void createCollectionIfNotExists(Class<?> type) {
		createCollectionIfNotExists(getCollectionName(type));
	}

	public void createCollectionIfNotExists(String collectionName) {

		MongoDatabase database = getDb().withWriteConcern(WriteConcern.MAJORITY)
				.withReadPreference(ReadPreference.primary());

		boolean collectionExists = database.listCollections().filter(new Document("name", collectionName)).first() != null;
		if (!collectionExists) {
			createCollection(collectionName);
		}
	}

	public void dropDatabase() {
		getDb().drop();
	}

	public void dropIndexes(String... collections) {
		for (String collection : collections) {
			getCollection(collection).dropIndexes();
		}
	}

	public void dropIndexes(Class<?>... entities) {
		for (Class<?> entity : entities) {
			getCollection(getCollectionName(entity)).dropIndexes();
		}
	}

	public void doInCollection(Class<?> entityClass, Consumer<MongoCollection<Document>> callback) {
		execute(entityClass, (collection -> {
			callback.accept(collection);
			return null;
		}));
	}

	public void awaitSearchIndexCreation(Class<?> type, String indexName) {
		awaitSearchIndexCreation(getCollectionName(type), indexName, Duration.ofSeconds(30));
	}

	public void awaitSearchIndexCreation(String collectionName, String indexName, Duration timeout) {

		Awaitility.await().atMost(timeout).pollInterval(Duration.ofMillis(200)).until(() -> {

			List<Document> execute = this.execute(collectionName,
					coll -> coll
							.aggregate(List.of(Document.parse("{'$listSearchIndexes': { 'name' : '%s'}}".formatted(indexName))))
							.into(new ArrayList<>()));
			for (Document doc : execute) {
				if (doc.getString("name").equals(indexName)) {
					return doc.getString("status").equals("READY");
				}
			}
			return false;
		});
	}

	public void awaitIndexDeletion(String collectionName, String indexName, Duration timeout) {

		Awaitility.await().atMost(timeout).pollInterval(Duration.ofMillis(200)).until(() -> {

			List<Document> execute = this.execute(collectionName,
					coll -> coll
							.aggregate(List.of(Document.parse("{'$listSearchIndexes': { 'name' : '%s'}}".formatted(indexName))))
							.into(new ArrayList<>()));
			for (Document doc : execute) {
				if (doc.getString("name").equals(indexName)) {
					return false;
				}
			}
			return true;
		});
	}

	public void awaitNoSearchIndexAvailable(String collectionName, Duration timeout) {

		Awaitility.await().atMost(timeout).pollInterval(Duration.ofMillis(200)).until(() -> {

			return this.execute(collectionName, coll -> coll.aggregate(List.of(Document.parse("{'$listSearchIndexes': {}}")))
					.into(new ArrayList<>()).isEmpty());

		});
	}

	public void awaitNoSearchIndexAvailable(Class<?> type, Duration timeout) {
		awaitNoSearchIndexAvailable(getCollectionName(type), timeout);
	}
}
