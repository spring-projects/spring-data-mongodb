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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.index.HashedIndex;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Collation;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;

/**
 * Unit tests for {@link DefaultIndexOperations}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
public class DefaultIndexOperationsUnitTests {

	private MongoTemplate template;

	@Mock MongoDatabaseFactory factory;
	@Mock MongoDatabase db;
	@Mock MongoCollection<Document> collection;

	private MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;

	@BeforeEach
	void setUp() {

		when(factory.getMongoDatabase()).thenReturn(db);
		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(db.getCollection(any(), any(Class.class))).thenReturn(collection);
		when(collection.createIndexes(anyList(), any(CreateIndexOptions.class))).thenReturn(List.of("OK"));

		this.mappingContext = new MongoMappingContext();
		this.converter = spy(new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext));
		this.template = new MongoTemplate(factory, converter);
	}

	@Test // DATAMONGO-1183
	void indexOperationsMapFieldNameCorrectly() {

		indexOpsFor(Jedi.class).ensureIndex(new Index("name", Direction.DESC));

		ArgumentCaptor<List<IndexModel>> captor = ArgumentCaptor.forClass(List.class);
		verify(collection).createIndexes(captor.capture(), any(CreateIndexOptions.class));
		assertThat(captor.getValue().get(0).getKeys()).isEqualTo(new Document("firstname", -1));
	}

	@Test // DATAMONGO-1854
	void ensureIndexDoesNotSetCollectionIfNoDefaultDefined() {

		indexOpsFor(Jedi.class).ensureIndex(new Index("firstname", Direction.DESC));

		ArgumentCaptor<List<IndexModel>> captor = ArgumentCaptor.forClass(List.class);
		verify(collection).createIndexes(captor.capture(), any(CreateIndexOptions.class));

		assertThat(captor.getValue().get(0).getOptions().getCollation()).isNull();
	}

	@Test // DATAMONGO-1854
	void ensureIndexUsesDefaultCollationIfNoneDefinedInOptions() {

		indexOpsFor(Sith.class).ensureIndex(new Index("firstname", Direction.DESC));

		ArgumentCaptor<List<IndexModel>> captor = ArgumentCaptor.forClass(List.class);
		verify(collection).createIndexes(captor.capture(), any(CreateIndexOptions.class));

		assertThat(captor.getValue().get(0).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void ensureIndexDoesNotUseDefaultCollationIfExplicitlySpecifiedInTheIndex() {

		indexOpsFor(Sith.class).ensureIndex(new Index("firstname", Direction.DESC).collation(Collation.of("en_US")));

		ArgumentCaptor<List<IndexModel>> captor = ArgumentCaptor.forClass(List.class);
		verify(collection).createIndexes(captor.capture(), any(CreateIndexOptions.class));

		assertThat(captor.getValue().get(0).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("en_US").build());
	}

	@Test // DATAMONGO-1183
	void shouldCreateHashedIndexCorrectly() {

		indexOpsFor(Jedi.class).ensureIndex(HashedIndex.hashed("name"));

		ArgumentCaptor<List<IndexModel>> captor = ArgumentCaptor.forClass(List.class);
		verify(collection).createIndexes(captor.capture(), any(CreateIndexOptions.class));
		assertThat(captor.getValue().get(0).getKeys()).isEqualTo(new Document("firstname", "hashed"));
	}

	@Test // GH-4698
	void shouldConsiderGivenCollectionName() {

		DefaultIndexOperations operations = new DefaultIndexOperations(template, "foo", Jedi.class);

		operations.ensureIndex(HashedIndex.hashed("name"));
		verify(db).getCollection(eq("foo"), any(Class.class));
	}

	@Test // GH-4422
	void shouldPassCreateIndexOptionsToDriver() {

		CreateIndexOptions createIndexOptions = new CreateIndexOptions()
				.commitQuorum(com.mongodb.CreateIndexCommitQuorum.MAJORITY);

		DefaultIndexOperations operations = new DefaultIndexOperations(template, template.getCollectionName(Jedi.class),
				Jedi.class, createIndexOptions);

		operations.createIndex(new Index("name", Direction.DESC));

		ArgumentCaptor<CreateIndexOptions> optionsCaptor = ArgumentCaptor.forClass(CreateIndexOptions.class);
		verify(collection).createIndexes(anyList(), optionsCaptor.capture());
		assertThat(optionsCaptor.getValue()).isSameAs(createIndexOptions);
	}

	private DefaultIndexOperations indexOpsFor(Class<?> type) {
		return new DefaultIndexOperations(template, template.getCollectionName(type), type);
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
			return "DefaultIndexOperationsUnitTests.Jedi(name=" + this.getName() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document(collation = "de_AT")
	static class Sith {
		@Field("firstname") String name;
	}

}
