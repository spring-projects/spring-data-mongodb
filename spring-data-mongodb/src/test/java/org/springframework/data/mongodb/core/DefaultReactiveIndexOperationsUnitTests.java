/*
 * Copyright 2019-present the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.BiConsumer;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Collation;

import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Unit tests for {@link DefaultReactiveIndexOperations}.
 *
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
public class DefaultReactiveIndexOperationsUnitTests {

	private ReactiveMongoTemplate template;

	@Mock ReactiveMongoDatabaseFactory factory;
	@Mock MongoDatabase db;
	@Mock MongoCollection<Document> collection;
	@Mock Publisher publisher;

	private MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;

	@BeforeEach
	void setUp() {

		when(factory.getMongoDatabase()).thenReturn(Mono.just(db));
		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(db.getCollection(any(), any(Class.class))).thenReturn(collection);
		when(collection.createIndexes(anyList(), any(CreateIndexOptions.class))).thenReturn(publisher);

		this.mappingContext = new MongoMappingContext();
		this.converter = spy(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext));
		this.template = new ReactiveMongoTemplate(factory, converter);
	}

	@Test // DATAMONGO-1854
	void ensureIndexDoesNotSetCollectionIfNoDefaultDefined() {

		indexOpsFor(Jedi.class).ensureIndex(new Index("firstname", Direction.DESC)).subscribe();

		verifyCreateIndex((keys, options) -> assertThat(options.getCollation()).isNull());
	}

	@Test // DATAMONGO-1854
	void ensureIndexUsesDefaultCollationIfNoneDefinedInOptions() {

		indexOpsFor(Sith.class).ensureIndex(new Index("firstname", Direction.DESC)).subscribe();

		verifyCreateIndex((keys, options) -> assertThat(options.getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void ensureIndexDoesNotUseDefaultCollationIfExplicitlySpecifiedInTheIndex() {

		indexOpsFor(Sith.class).ensureIndex(new Index("firstname", Direction.DESC).collation(Collation.of("en_US")))
				.subscribe();


		verifyCreateIndex((keys, options) -> assertThat(options.getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("en_US").build()));
	}

	private void verifyCreateIndex(BiConsumer<Bson, IndexOptions> consumer) {

		ArgumentCaptor<List<IndexModel>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection).createIndexes(captor.capture(), any());

		IndexModel indexModel = captor.getValue().get(0);
		consumer.accept(indexModel.getKeys(), indexModel.getOptions());
	}

	private DefaultReactiveIndexOperations indexOpsFor(Class<?> type) {
		return new DefaultReactiveIndexOperations(template, template.getCollectionName(type),
				new QueryMapper(template.getConverter()), type);
	}

	static class Jedi {

		@Field("firstname") String name;

		public Jedi() {}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String toString() {
			return "DefaultReactiveIndexOperationsUnitTests.Jedi(name=" + this.getName() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document(collation = "de_AT")
	static class Sith {
		@Field("firstname") String name;
	}

}
