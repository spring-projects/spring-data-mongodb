/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.IndexOperationsProvider;
import org.springframework.data.mongodb.repository.query.MongoEntityMetadata;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.mongodb.repository.query.PartTreeMongoQuery;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.Streamable;

/**
 * Unit tests for {@link IndexEnsuringQueryCreationListener}.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IndexEnsuringQueryCreationListenerUnitTests {

	private IndexEnsuringQueryCreationListener listener;

	@Mock IndexOperationsProvider provider;
	@Mock PartTree partTree;
	@Mock PartTreeMongoQuery partTreeQuery;
	@Mock MongoQueryMethod queryMethod;
	@Mock IndexOperations indexOperations;
	@Mock MongoEntityMetadata entityInformation;

	@BeforeEach
	void setUp() {

		this.listener = new IndexEnsuringQueryCreationListener(provider);

		partTreeQuery = mock(PartTreeMongoQuery.class, Answers.RETURNS_MOCKS);
		when(partTreeQuery.getTree()).thenReturn(partTree);
		when(provider.indexOps(anyString(), any())).thenReturn(indexOperations);
		when(queryMethod.getEntityInformation()).thenReturn(entityInformation);
		when(entityInformation.getCollectionName()).thenReturn("persons");
	}

	@Test // DATAMONGO-1753
	void skipsQueryCreationForMethodWithoutPredicate() {

		when(partTree.hasPredicate()).thenReturn(false);

		listener.onCreation(partTreeQuery);

		verify(provider, times(0)).indexOps(any());
	}

	@Test // DATAMONGO-1854
	void usesCollationWhenPresentAndFixedValue() {

		when(partTree.hasPredicate()).thenReturn(true);
		when(partTree.getParts()).thenReturn(Streamable.empty());
		when(partTree.getSort()).thenReturn(Sort.unsorted());
		when(partTreeQuery.getQueryMethod()).thenReturn(queryMethod);
		when(queryMethod.hasAnnotatedCollation()).thenReturn(true);
		when(queryMethod.getAnnotatedCollation()).thenReturn("en_US");

		listener.onCreation(partTreeQuery);

		ArgumentCaptor<IndexDefinition> indexArgumentCaptor = ArgumentCaptor.forClass(IndexDefinition.class);
		verify(indexOperations).ensureIndex(indexArgumentCaptor.capture());

		IndexDefinition indexDefinition = indexArgumentCaptor.getValue();
		assertThat(indexDefinition.getIndexOptions()).isEqualTo(new Document("collation", new Document("locale", "en_US")));
	}

	@Test // DATAMONGO-1854
	void usesCollationWhenPresentAndFixedDocumentValue() {

		when(partTree.hasPredicate()).thenReturn(true);
		when(partTree.getParts()).thenReturn(Streamable.empty());
		when(partTree.getSort()).thenReturn(Sort.unsorted());
		when(partTreeQuery.getQueryMethod()).thenReturn(queryMethod);
		when(queryMethod.hasAnnotatedCollation()).thenReturn(true);
		when(queryMethod.getAnnotatedCollation()).thenReturn("{ 'locale' : 'en_US' }");

		listener.onCreation(partTreeQuery);

		ArgumentCaptor<IndexDefinition> indexArgumentCaptor = ArgumentCaptor.forClass(IndexDefinition.class);
		verify(indexOperations).ensureIndex(indexArgumentCaptor.capture());

		IndexDefinition indexDefinition = indexArgumentCaptor.getValue();
		assertThat(indexDefinition.getIndexOptions()).isEqualTo(new Document("collation", new Document("locale", "en_US")));
	}

	@Test // DATAMONGO-1854
	void skipsCollationWhenPresentButDynamic() {

		when(partTree.hasPredicate()).thenReturn(true);
		when(partTree.getParts()).thenReturn(Streamable.empty());
		when(partTree.getSort()).thenReturn(Sort.unsorted());
		when(partTreeQuery.getQueryMethod()).thenReturn(queryMethod);
		when(queryMethod.hasAnnotatedCollation()).thenReturn(true);
		when(queryMethod.getAnnotatedCollation()).thenReturn("{ 'locale' : '?0' }");

		listener.onCreation(partTreeQuery);

		ArgumentCaptor<IndexDefinition> indexArgumentCaptor = ArgumentCaptor.forClass(IndexDefinition.class);
		verify(indexOperations).ensureIndex(indexArgumentCaptor.capture());

		IndexDefinition indexDefinition = indexArgumentCaptor.getValue();
		assertThat(indexDefinition.getIndexOptions()).isEmpty();
	}

	@Test // DATAMONGO-1854
	void skipsCollationWhenNotPresent() {

		when(partTree.hasPredicate()).thenReturn(true);
		when(partTree.getParts()).thenReturn(Streamable.empty());
		when(partTree.getSort()).thenReturn(Sort.unsorted());
		when(partTreeQuery.getQueryMethod()).thenReturn(queryMethod);
		when(queryMethod.hasAnnotatedCollation()).thenReturn(false);

		listener.onCreation(partTreeQuery);

		ArgumentCaptor<IndexDefinition> indexArgumentCaptor = ArgumentCaptor.forClass(IndexDefinition.class);
		verify(indexOperations).ensureIndex(indexArgumentCaptor.capture());

		IndexDefinition indexDefinition = indexArgumentCaptor.getValue();
		assertThat(indexDefinition.getIndexOptions()).isEmpty();
	}

	interface SampleRepository {

		Object findAllBy();
	}
}
