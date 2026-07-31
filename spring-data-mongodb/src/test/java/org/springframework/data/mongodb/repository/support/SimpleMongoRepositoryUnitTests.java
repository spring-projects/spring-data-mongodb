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
package org.springframework.data.mongodb.repository.support;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
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
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.ExecutableFindOperation.ExecutableFind;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkOperation;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.CrudMethodMetadataPostProcessor.DefaultCrudMethodMetadata;
import org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery;

/**
 * Unit tests for {@link SimpleMongoRepository}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Sangyeop Jeong
 */
@ExtendWith(MockitoExtension.class)
public class SimpleMongoRepositoryUnitTests {

	SimpleMongoRepository<Object, Object> repository;
	@Mock MongoOperations mongoOperations;
	@Mock MongoEntityInformation<Object, Object> entityInformation;
	@Mock MongoConverter mongoConverter;
	@Mock MappingContext<MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	@Mock MongoPersistentEntity<?> mongoPersistentEntity;

	@BeforeEach
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

	@ParameterizedTest // GH-2971
	@MethodSource("findAllCalls")
	void shouldAddReadPreferenceToFindAllMethods(Consumer<SimpleMongoRepository<Object, Object>> findCall)
			throws NoSuchMethodException {

		repository = new SimpleMongoRepository<>(entityInformation, mongoOperations);
		repository.setRepositoryMethodMetadata(
				new DefaultCrudMethodMetadata(TestRepositoryWithReadPreference.class, TestRepositoryWithReadPreference.class.getMethod("dummy")));

		findCall.accept(repository);

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).find(query.capture(), any(), any());

		assertThat(query.getValue().getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondaryPreferred());
	}

	@Test // GH-2971
	void shouldAddReadPreferenceToFindOne() throws NoSuchMethodException {

		repository = new SimpleMongoRepository<>(entityInformation, mongoOperations);
		repository.setRepositoryMethodMetadata(
				new DefaultCrudMethodMetadata(TestRepositoryWithReadPreference.class, TestRepositoryWithReadPreference.class.getMethod("dummy")));

		repository.findOne(Example.of(new TestDummy()));

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(mongoOperations).findOne(query.capture(), any(), any());

		assertThat(query.getValue().getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondaryPreferred());
	}

	@Test // GH-2971
	void shouldAddReadPreferenceToFluentFetchable() throws NoSuchMethodException {

		ExecutableFind<Object> finder = mock(ExecutableFind.class);
		when(mongoOperations.query(any())).thenReturn(finder);
		when(finder.inCollection(any())).thenReturn(finder);
		when(finder.matching(any(Query.class))).thenReturn(finder);
		when(finder.as(any())).thenReturn(finder);

		repository = new SimpleMongoRepository<>(entityInformation, mongoOperations);
		repository.setRepositoryMethodMetadata(
				new DefaultCrudMethodMetadata(TestRepositoryWithReadPreferenceMethod.class, TestRepositoryWithReadPreferenceMethod.class.getMethod("dummy")));

		repository.findBy(Example.of(new TestDummy()), FetchableFluentQuery::all);

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(finder).matching(query.capture());

		assertThat(query.getValue().getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondaryPreferred());
	}

	@Test // GH-5220
	void saveAllUsesSingleBulkWriteInsertingNewAndReplacingExistingEntities() {

		Object existing = new Object();
		Object fresh = new Object();

		stubNonVersionedEntityInformation();
		when(entityInformation.isNew(existing)).thenReturn(false);
		when(entityInformation.isNew(fresh)).thenReturn(true);
		when(entityInformation.getId(existing)).thenReturn("id-1");

		repository.saveAll(asList(existing, fresh));

		ArgumentCaptor<Bulk> bulk = ArgumentCaptor.forClass(Bulk.class);
		ArgumentCaptor<BulkWriteOptions> options = ArgumentCaptor.forClass(BulkWriteOptions.class);
		verify(mongoOperations).bulkWrite(bulk.capture(), options.capture());

		assertThat(options.getValue().getOrder()).isEqualTo(BulkWriteOptions.Order.ORDERED);
		assertThat(bulk.getValue().operations()).satisfiesExactly(first -> {

			assertThat(first).isInstanceOf(BulkOperation.Replace.class);
			assertThat(((BulkOperation.Replace) first).replacement()).isSameAs(existing);
		}, second -> {

			assertThat(second).isInstanceOf(BulkOperation.Insert.class);
			assertThat(((BulkOperation.Insert) second).value()).isSameAs(fresh);
		});

		verify(mongoOperations, never()).save(any(), anyString());
		verify(mongoOperations, never()).insert(anyList(), anyString());
	}

	@Test // GH-5220
	void saveAllReturnsEntitiesInSourceOrder() {

		Object existing = new Object();
		Object fresh = new Object();

		stubNonVersionedEntityInformation();
		when(entityInformation.isNew(existing)).thenReturn(false);
		when(entityInformation.isNew(fresh)).thenReturn(true);
		when(entityInformation.getId(existing)).thenReturn("id-1");

		List<Object> saved = repository.saveAll(asList(existing, fresh));

		assertThat(saved).containsExactly(existing, fresh);
	}

	@Test // GH-5220
	void saveAllFallsBackToPerEntitySaveForVersionedEntities() {

		when(entityInformation.getCollectionName()).thenReturn("persons");
		stubEntityWithVersionProperty();

		Object existing = new Object();
		when(entityInformation.isNew(existing)).thenReturn(false);

		repository.saveAll(List.of(existing));

		verify(mongoOperations).save(existing, "persons");
		verify(mongoOperations, never()).bulkWrite(any(Bulk.class), any(BulkWriteOptions.class));
	}

	@Test // GH-5220
	void saveAllDoesNotIssueBulkWriteForEmptyIterable() {

		assertThat(repository.saveAll(List.of())).isEmpty();

		verify(mongoOperations, never()).bulkWrite(any(Bulk.class), any(BulkWriteOptions.class));
	}

	private void stubNonVersionedEntityInformation() {

		when(entityInformation.getJavaType()).thenReturn(Object.class);
		when(entityInformation.getCollectionName()).thenReturn("persons");
		when(entityInformation.getIdAttribute()).thenReturn("id");
		stubEntityWithoutVersionProperty();
	}

	private void stubEntityWithoutVersionProperty() {
		stubMappingContext(null);
	}

	private void stubEntityWithVersionProperty() {

		stubMappingContext(mongoPersistentEntity);
		when(mongoPersistentEntity.hasVersionProperty()).thenReturn(true);
	}

	private void stubMappingContext(@Nullable MongoPersistentEntity<?> persistentEntity) {

		when(mongoOperations.getConverter()).thenReturn(mongoConverter);
		doReturn(mappingContext).when(mongoConverter).getMappingContext();
		doReturn(persistentEntity).when(mappingContext).getPersistentEntity(any(Class.class));
	}

	private static Stream<Arguments> findAllCalls() {

		Consumer<SimpleMongoRepository<Object, Object>> findAll = SimpleMongoRepository::findAll;
		Consumer<SimpleMongoRepository<Object, Object>> findAllWithSort = repo -> repo.findAll(Sort.by("age"));
		Consumer<SimpleMongoRepository<Object, Object>> findAllWithPage = repo -> repo
				.findAll(PageRequest.of(1, 20, Sort.by("age")));
		Consumer<SimpleMongoRepository<Object, Object>> findAllWithExample = repo -> repo
				.findAll(Example.of(new TestDummy()));
		Consumer<SimpleMongoRepository<Object, Object>> findAllWithExampleAndSort = repo -> repo
				.findAll(Example.of(new TestDummy()), Sort.by("age"));
		Consumer<SimpleMongoRepository<Object, Object>> findAllWithExampleAndPage = repo -> repo
				.findAll(Example.of(new TestDummy()), PageRequest.of(1, 20, Sort.by("age")));

		return Stream.of(Arguments.of(findAll), //
				Arguments.of(findAllWithSort), //
				Arguments.of(findAllWithPage), //
				Arguments.of(findAllWithExample), //
				Arguments.of(findAllWithExampleAndSort), //
				Arguments.of(findAllWithExampleAndPage));
	}

	static class TestDummy {

	}

	interface TestRepository {

	}

	@ReadPreference("secondaryPreferred")
	interface TestRepositoryWithReadPreference {

		void dummy();
	}

	interface TestRepositoryWithReadPreferenceMethod {

		@ReadPreference("secondaryPreferred")
		void dummy();
	}

}
