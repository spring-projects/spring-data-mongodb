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
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleMongoRepositoryUnitTests {

	SimpleMongoRepository<Object, Object> repository;
	@Mock MongoOperations mongoOperations;
	@Mock MongoEntityInformation<Object, Object> entityInformation;

	@Before
	public void setUp() {
		repository = new SimpleMongoRepository<>(entityInformation, mongoOperations);
	}

	@Test // DATAMONGO-1854
	public void shouldAddDefaultCollationToCountForExampleIfPresent() {

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.count(Example.of(new TestDummy()));

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).count(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	public void shouldAddDefaultCollationToExistsForExampleIfPresent() {

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.exists(Example.of(new TestDummy()));

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).exists(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	public void shouldAddDefaultCollationToFindForExampleIfPresent() {

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.findAll(Example.of(new TestDummy()));

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	public void shouldAddDefaultCollationToFindWithSortForExampleIfPresent() {

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.findAll(Example.of(new TestDummy()), Sort.by("nothing"));

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	public void shouldAddDefaultCollationToFindWithPageableForExampleIfPresent() {

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.findAll(Example.of(new TestDummy()), PageRequest.of(1, 1, Sort.by("nothing")));

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	public void shouldAddDefaultCollationToFindOneForExampleIfPresent() {

		Collation collation = Collation.of("en_US");

		when(entityInformation.getCollation()).thenReturn(collation);
		repository.findOne(Example.of(new TestDummy()));

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).findOne(query.capture(), any(), any());

		assertThat(query.getValue().getCollation()).contains(collation);
	}

	static class TestDummy {

	}

}
