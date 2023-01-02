/*
 * Copyright 2020-2023 the original author or authors.
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

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

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

		this(new Supplier<MongoTestTemplateConfiguration>() {
			@Override
			public MongoTestTemplateConfiguration get() {

				MongoTestTemplateConfiguration config = new MongoTestTemplateConfiguration();
				cfg.accept(config);
				return config;
			}
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
		flush(getDb().listCollectionNames());
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
		flush(Arrays.asList(entities).stream().map(this::getCollectionName).collect(Collectors.toList()));
	}

	public void flush(String... collections) {
		flush(Arrays.asList(collections));
	}

	public void flush(Object... objects) {

		flush(Arrays.asList(objects).stream().map(it -> {

			if (it instanceof String) {
				return (String) it;
			}
			if (it instanceof Class) {
				return getCollectionName((Class<?>) it);
			}
			return it.toString();
		}).collect(Collectors.toList()));
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
}
