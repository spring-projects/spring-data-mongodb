/*
 * Copyright 2019 the original author or authors.
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

import lombok.Data;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.index.HashedIndex;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Collation;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

/**
 * Unit tests for {@link DefaultIndexOperations}.
 *
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultIndexOperationsUnitTests {

	MongoTemplate template;

	@Mock MongoDbFactory factory;
	@Mock MongoDatabase db;
	@Mock MongoCollection<Document> collection;

	MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	MappingMongoConverter converter;
	MongoMappingContext mappingContext;

	@Before
	public void setUp() {

		when(factory.getMongoDatabase()).thenReturn(db);
		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(db.getCollection(any(), any(Class.class))).thenReturn(collection);
		when(collection.createIndex(any(), any(IndexOptions.class))).thenReturn("OK");

		this.mappingContext = new MongoMappingContext();
		this.converter = spy(new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext));
		this.template = new MongoTemplate(factory, converter);
	}

	@Test // DATAMONGO-1183
	public void indexOperationsMapFieldNameCorrectly() {

		indexOpsFor(Jedi.class).ensureIndex(new Index("name", Direction.DESC));

		verify(collection).createIndex(eq(new Document("firstname", -1)), any());
	}

	@Test // DATAMONGO-1854
	public void ensureIndexDoesNotSetCollectionIfNoDefaultDefined() {

		indexOpsFor(Jedi.class).ensureIndex(new Index("firstname", Direction.DESC));

		ArgumentCaptor<IndexOptions> options = ArgumentCaptor.forClass(IndexOptions.class);
		verify(collection).createIndex(any(), options.capture());

		assertThat(options.getValue().getCollation()).isNull();
	}

	@Test // DATAMONGO-1854
	public void ensureIndexUsesDefaultCollationIfNoneDefinedInOptions() {

		indexOpsFor(Sith.class).ensureIndex(new Index("firstname", Direction.DESC));

		ArgumentCaptor<IndexOptions> options = ArgumentCaptor.forClass(IndexOptions.class);
		verify(collection).createIndex(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	public void ensureIndexDoesNotUseDefaultCollationIfExplicitlySpecifiedInTheIndex() {

		indexOpsFor(Sith.class).ensureIndex(new Index("firstname", Direction.DESC).collation(Collation.of("en_US")));

		ArgumentCaptor<IndexOptions> options = ArgumentCaptor.forClass(IndexOptions.class);
		verify(collection).createIndex(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("en_US").build());
	}

	@Test // DATAMONGO-1183
	public void shouldCreateHashedIndexCorrectly() {

		indexOpsFor(Jedi.class).ensureIndex(HashedIndex.hashed("name"));

		verify(collection).createIndex(eq(new Document("firstname", "hashed")), any());
	}

	private DefaultIndexOperations indexOpsFor(Class<?> type) {
		return new DefaultIndexOperations(template, template.getCollectionName(type), type);
	}

	@Data
	static class Jedi {
		@Field("firstname") String name;
	}

	@org.springframework.data.mongodb.core.mapping.Document(collation = "de_AT")
	static class Sith {
		@Field("firstname") String name;
	}

}
