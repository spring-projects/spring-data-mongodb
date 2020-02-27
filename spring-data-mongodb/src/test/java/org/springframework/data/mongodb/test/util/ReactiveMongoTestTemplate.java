/*
 * Copyright 2020 the original author or authors.
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

import com.mongodb.reactivestreams.client.MongoClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.bson.Document;
import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

/**
 * A {@link ReactiveMongoTemplate} with configuration hooks and extension suitable for tests.
 * 
 * @author Christoph Strobl
 * @since 3.0
 */
public class ReactiveMongoTestTemplate extends ReactiveMongoTemplate {

	private final MongoTestTemplateConfiguration cfg;

	public ReactiveMongoTestTemplate(MongoClient client, String database, Class<?>... initialEntities) {
		this(cfg -> {
			cfg.configureDatabaseFactory(it -> {

				it.client(client);
				it.defaultDb(database);
			});

			cfg.configureMappingContext(it -> {

				it.autocreateIndex(false);
				it.intitalEntitySet(initialEntities);
			});
		});
	}

	public ReactiveMongoTestTemplate(Consumer<MongoTestTemplateConfiguration> cfg) {

		this(new Supplier<MongoTestTemplateConfiguration>() {
			@Override
			public MongoTestTemplateConfiguration get() {

				MongoTestTemplateConfiguration config = new MongoTestTemplateConfiguration();
				cfg.accept(config);
				return config;
			}
		});
	}

	public ReactiveMongoTestTemplate(Supplier<MongoTestTemplateConfiguration> config) {
		this(config.get());
	}

	public ReactiveMongoTestTemplate(MongoTestTemplateConfiguration config) {
		super(config.reactiveDatabaseFactory(), config.mongoConverter());

		ApplicationContext applicationContext = config.getApplicationContext();
		if (applicationContext != null) {
			setApplicationContext(applicationContext);
		}

		this.cfg = config;
	}

	public ReactiveMongoDatabaseFactory getDatabaseFactory() {
		return cfg.reactiveDatabaseFactory();
	}

	public Mono<Void> flush() {
		return flush(Flux.fromStream(
				PersistentEntities.of(getConverter().getMappingContext()).stream().map(it -> getCollectionName(it.getType()))));
	}

	public Mono<Void> flushDatabase() {
		return flush(getMongoDatabase().listCollectionNames());
	}

	public Mono<Void> flush(Class<?>... entities) {
		return flush(Flux.fromStream(Arrays.asList(entities).stream().map(this::getCollectionName)));
	}

	public Mono<Void> flush(String... collections) {
		return flush(Flux.fromArray(collections));
	}

	public Mono<Void> flush(Publisher<String> collectionNames) {

		return Flux.from(collectionNames)
				.flatMap(collection -> Mono.from(getCollection(collection).deleteMany(new Document())).then()
						.onErrorResume(it -> Mono.from(getCollection(collection).drop()).then()))
				.then();
	}

	public Mono<Void> flush(Object... objects) {

		return flush(Flux.fromStream(Arrays.asList(objects).stream().map(it -> {

			if (it instanceof String) {
				return (String) it;
			}
			if (it instanceof Class) {
				return getCollectionName((Class<?>) it);
			}
			return it.toString();
		})));
	}

	public Mono<Void> dropDatabase() {
		return Mono.from(getMongoDatabase().drop()).then();
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
