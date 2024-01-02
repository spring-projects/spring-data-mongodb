/*
 * Copyright 2019-2024 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class SimpleReactiveMongoRepositoryUnitTests {

	private SimpleReactiveMongoRepository<Object, String> repository;
	@Mock Mono mono;
	@Mock Flux flux;
	@Mock ReactiveMongoOperations mongoOperations;
	@Mock MongoEntityInformation<Object, String> entityInformation;

	@BeforeEach
	void setUp() {
		repository = new SimpleReactiveMongoRepository<>(entityInformation, mongoOperations);
	}

	@Test // DATAMONGO-1854
	void shouldAddDefaultCollationToCountForExampleIfPresent() {

		when(mongoOperations.count(any(), any(), any())).thenReturn(mono);

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.count(Example.of(new TestDummy())).subscribe();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).count(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	void shouldAddDefaultCollationToExistsForExampleIfPresent() {

		when(mongoOperations.exists(any(), any(), any())).thenReturn(mono);

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.exists(Example.of(new TestDummy())).subscribe();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).exists(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	void shouldAddDefaultCollationToFindForExampleIfPresent() {

		when(mongoOperations.find(any(), any(), any())).thenReturn(flux);

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.findAll(Example.of(new TestDummy())).subscribe();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	void shouldAddDefaultCollationToFindWithSortForExampleIfPresent() {

		when(mongoOperations.find(any(), any(), any())).thenReturn(flux);

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.findAll(Example.of(new TestDummy()), Sort.by("nothing")).subscribe();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	void shouldAddDefaultCollationToFindOneForExampleIfPresent() {

		when(entityInformation.getCollectionName()).thenReturn("testdummy");
		doReturn(flux).when(mongoOperations).find(any(Query.class), eq(TestDummy.class), eq("testdummy"));
		when(flux.buffer(anyInt())).thenReturn(flux);
		when(flux.map(any())).thenReturn(flux);
		when(flux.next()).thenReturn(mono);

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.findOne(Example.of(new TestDummy())).subscribe();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	private static class TestDummy {

	}

}
