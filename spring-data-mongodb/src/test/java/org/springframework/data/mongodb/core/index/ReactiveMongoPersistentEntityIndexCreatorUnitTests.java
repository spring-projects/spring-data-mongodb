/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.MongoException;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Unit tests for {@link ReactiveMongoPersistentEntityIndexCreator}.
 *
 * @author Mark Paluch
 * @author Mathieu Ouellet
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ReactiveMongoPersistentEntityIndexCreatorUnitTests {

	private ReactiveIndexOperations indexOperations;

	@Mock ReactiveMongoDatabaseFactory factory;
	@Mock MongoDatabase db;
	@Mock MongoCollection<org.bson.Document> collection;

	private ArgumentCaptor<org.bson.Document> keysCaptor;
	private ArgumentCaptor<IndexOptions> optionsCaptor;
	private ArgumentCaptor<String> collectionCaptor;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() {

		when(factory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		when(factory.getMongoDatabase()).thenReturn(Mono.just(db));
		when(db.getCollection(any(), any(Class.class))).thenReturn(collection);

		indexOperations = new ReactiveMongoTemplate(factory).indexOps("foo");

		keysCaptor = ArgumentCaptor.forClass(org.bson.Document.class);
		optionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);
		collectionCaptor = ArgumentCaptor.forClass(String.class);

		when(collection.createIndex(keysCaptor.capture(), optionsCaptor.capture())).thenReturn(Mono.just("OK"));
	}

	@Test // DATAMONGO-1928
	void buildsIndexDefinitionUsingFieldName() {

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		Mono<Void> publisher = checkForIndexes(mappingContext);

		verifyNoInteractions(collection);

		publisher.as(StepVerifier::create).verifyComplete();

		assertThat(keysCaptor.getValue()).isNotNull().containsKey("fieldname");
		assertThat(optionsCaptor.getValue().getName()).isEqualTo("indexName");
		assertThat(optionsCaptor.getValue().isBackground()).isFalse();
		assertThat(optionsCaptor.getValue().getExpireAfter(TimeUnit.SECONDS)).isNull();
	}

	@Test // DATAMONGO-1928
	void createIndexShouldUsePersistenceExceptionTranslatorForNonDataIntegrityConcerns() {

		when(collection.createIndex(any(org.bson.Document.class), any(IndexOptions.class)))
				.thenReturn(Mono.error(new MongoException(6, "HostUnreachable")));

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		Mono<Void> publisher = checkForIndexes(mappingContext);

		publisher.as(StepVerifier::create).expectError(DataAccessResourceFailureException.class).verify();
	}

	@Test // DATAMONGO-1928
	void createIndexShouldNotConvertUnknownExceptionTypes() {

		when(collection.createIndex(any(org.bson.Document.class), any(IndexOptions.class)))
				.thenReturn(Mono.error(new ClassCastException("o_O")));

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		Mono<Void> publisher = checkForIndexes(mappingContext);

		publisher.as(StepVerifier::create).expectError(ClassCastException.class).verify();
	}

	private static MongoMappingContext prepareMappingContext(Class<?> type) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(Collections.singleton(type));
		mappingContext.initialize();

		return mappingContext;
	}

	private Mono<Void> checkForIndexes(MongoMappingContext mappingContext) {

		return new ReactiveMongoPersistentEntityIndexCreator(mappingContext, it -> indexOperations)
				.checkForIndexes(mappingContext.getRequiredPersistentEntity(Person.class));
	}

	@Document
	static class Person {

		@Indexed(name = "indexName") //
		@Field("fieldname") //
		String field;

	}
}
