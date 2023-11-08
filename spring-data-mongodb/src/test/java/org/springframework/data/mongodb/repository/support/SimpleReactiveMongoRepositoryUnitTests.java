/*
 * Copyright 2019-2023 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveFindOperation.ReactiveFind;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.query.FluentQuery;

/**
 * Unit tests for {@link SimpleReactiveMongoRepository}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
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

	@ParameterizedTest // GH-2971
	@MethodSource("findAllCalls")
	void shouldAddReadPreferenceToFindAllMethods(
			Function<SimpleReactiveMongoRepository<Object, String>, Flux<Object>> findCall) {

		repository = new SimpleReactiveMongoRepository<>(entityInformation, mongoOperations);
		repository.setRepositoryMethodMetadata(new CrudMethodMetadata() {
			@Override
			public Optional<com.mongodb.ReadPreference> getReadPreference() {
				return Optional.of(com.mongodb.ReadPreference.secondaryPreferred());
			}
		});
		when(mongoOperations.find(any(), any(), any())).thenReturn(Flux.just("ok"));

		findCall.apply(repository).subscribe();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondaryPreferred());
	}

	@Test // GH-2971
	void shouldAddReadPreferenceToFindOne() {

		repository = new SimpleReactiveMongoRepository<>(entityInformation, mongoOperations);
		repository.setRepositoryMethodMetadata(new CrudMethodMetadata() {
			@Override
			public Optional<com.mongodb.ReadPreference> getReadPreference() {
				return Optional.of(com.mongodb.ReadPreference.secondaryPreferred());
			}
		});
		when(mongoOperations.find(any(), any(), any())).thenReturn(Flux.just("ok"));

		repository.findOne(Example.of(new SimpleMongoRepositoryUnitTests.TestDummy())).subscribe();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondaryPreferred());
	}

	@Test // GH-2971
	void shouldAddReadPreferenceToFluentFetchable() {

		ReactiveFind<Object> finder = mock(ReactiveFind.class);
		when(mongoOperations.query(any())).thenReturn(finder);
		when(finder.inCollection(any())).thenReturn(finder);
		when(finder.matching(any(Query.class))).thenReturn(finder);
		when(finder.as(any())).thenReturn(finder);
		when(finder.all()).thenReturn(Flux.just("ok"));

		repository = new SimpleReactiveMongoRepository<>(entityInformation, mongoOperations);
		repository.setRepositoryMethodMetadata(new CrudMethodMetadata() {
			@Override
			public Optional<com.mongodb.ReadPreference> getReadPreference() {
				return Optional.of(com.mongodb.ReadPreference.secondaryPreferred());
			}
		});

		repository.findBy(Example.of(new TestDummy()), FluentQuery.ReactiveFluentQuery::all).subscribe();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(finder).matching(query.capture());

		assertThat(query.getValue().getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondaryPreferred());
	}

	private static Stream<Arguments> findAllCalls() {

		Function<SimpleReactiveMongoRepository<Object, String>, Flux<Object>> findAll = SimpleReactiveMongoRepository::findAll;
		Function<SimpleReactiveMongoRepository<Object, String>, Flux<Object>> findAllWithSort = repo -> repo
				.findAll(Sort.by("age"));
		Function<SimpleReactiveMongoRepository<Object, String>, Flux<Object>> findAllWithExample = repo -> repo
				.findAll(Example.of(new TestDummy()));
		Function<SimpleReactiveMongoRepository<Object, String>, Flux<Object>> findAllWithExampleAndSort = repo -> repo
				.findAll(Example.of(new TestDummy()), Sort.by("age"));

		return Stream.of(Arguments.of(findAll), //
				Arguments.of(findAllWithSort), //
				Arguments.of(findAllWithExample), //
				Arguments.of(findAllWithExampleAndSort));
	}

	private static class TestDummy {

	}

	@ReadPreference("secondaryPreferred")
	interface TestRepositoryWithReadPreference {

	}

}
