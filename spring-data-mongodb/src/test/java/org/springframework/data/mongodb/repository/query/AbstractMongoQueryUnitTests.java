/*
 * Copyright 2014-2022 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.ExecutableFindOperation.ExecutableFind;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.ExecutableUpdate;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingUpdate;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.UpdateWithQuery;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.UpdateWithUpdate;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * Unit tests for {@link AbstractMongoQuery}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AbstractMongoQueryUnitTests {

	@Mock MongoOperations mongoOperationsMock;
	@Mock ExecutableFind<?> executableFind;
	@Mock FindWithQuery<?> withQueryMock;
	@Mock ExecutableUpdate executableUpdate;
	@Mock UpdateWithQuery updateWithQuery;
	@Mock UpdateWithUpdate updateWithUpdate;
	@Mock TerminatingUpdate terminatingUpdate;
	@Mock BasicMongoPersistentEntity<?> persitentEntityMock;
	@Mock MongoMappingContext mappingContextMock;
	@Mock DeleteResult deleteResultMock;
	@Mock UpdateResult updateResultMock;

	@BeforeEach
	void setUp() {

		doReturn("persons").when(persitentEntityMock).getCollection();
		doReturn(persitentEntityMock).when(mappingContextMock).getPersistentEntity(Mockito.any(Class.class));
		doReturn(persitentEntityMock).when(mappingContextMock).getRequiredPersistentEntity(Mockito.any(Class.class));
		doReturn(Person.class).when(persitentEntityMock).getType();

		MongoDatabaseFactory mongoDbFactory = mock(MongoDatabaseFactory.class);
		when(mongoDbFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		DbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoDbFactory);
		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContextMock);
		converter.afterPropertiesSet();

		doReturn(converter).when(mongoOperationsMock).getConverter();
		doReturn(executableFind).when(mongoOperationsMock).query(any());
		doReturn(withQueryMock).when(executableFind).as(any());
		doReturn(withQueryMock).when(withQueryMock).matching(any(Query.class));
		doReturn(executableUpdate).when(mongoOperationsMock).update(any());
		doReturn(updateWithQuery).when(executableUpdate).matching(any(Query.class));
		doReturn(terminatingUpdate).when(updateWithQuery).apply(any(UpdateDefinition.class));

		when(mongoOperationsMock.remove(any(), any(), anyString())).thenReturn(deleteResultMock);
		when(mongoOperationsMock.updateMulti(any(), any(), any(), anyString())).thenReturn(updateResultMock);
	}

	@Test // DATAMONGO-566
	void testDeleteExecutionCallsRemoveCorrectly() {

		createQueryForMethod("deletePersonByLastname", String.class).setDeleteQuery(true).execute(new Object[] { "booh" });

		verify(mongoOperationsMock, times(1)).remove(any(), eq(Person.class), eq("persons"));
		verify(mongoOperationsMock, times(0)).find(any(), any(), any());
	}

	@Test // DATAMONGO-566, DATAMONGO-1040
	void testDeleteExecutionLoadsListOfRemovedDocumentsWhenReturnTypeIsCollectionLike() {

		createQueryForMethod("deleteByLastname", String.class).setDeleteQuery(true).execute(new Object[] { "booh" });

		verify(mongoOperationsMock, times(1)).findAllAndRemove(any(), eq(Person.class), eq("persons"));
	}

	@Test // DATAMONGO-566
	void testDeleteExecutionReturnsZeroWhenWriteResultIsNull() {

		MongoQueryFake query = createQueryForMethod("deletePersonByLastname", String.class);
		query.setDeleteQuery(true);

		assertThat(query.execute(new Object[] { "fake" })).isEqualTo(0L);
	}

	@Test // DATAMONGO-566, DATAMONGO-978
	void testDeleteExecutionReturnsNrDocumentsDeletedFromWriteResult() {

		when(deleteResultMock.getDeletedCount()).thenReturn(100L);
		when(deleteResultMock.wasAcknowledged()).thenReturn(true);

		MongoQueryFake query = createQueryForMethod("deletePersonByLastname", String.class);
		query.setDeleteQuery(true);

		assertThat(query.execute(new Object[] { "fake" })).isEqualTo(100L);
		verify(mongoOperationsMock, times(1)).remove(any(), eq(Person.class), eq("persons"));
	}

	@Test // DATAMONGO-957
	void metadataShouldNotBeAddedToQueryWhenNotPresent() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class);
		query.execute(new Object[] { "fake" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind).as(Person.class);
		verify(withQueryMock).matching(captor.capture());

		assertThat(captor.getValue().getMeta().getComment()).isNull();
		;
	}

	@Test // DATAMONGO-957
	void metadataShouldBeAddedToQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class, Pageable.class);
		query.execute(new Object[] { "fake", PageRequest.of(0, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind).as(Person.class);
		verify(withQueryMock).matching(captor.capture());

		assertThat(captor.getValue().getMeta().getComment()).isEqualTo("comment");
	}

	@Test // DATAMONGO-957
	void metadataShouldBeAddedToCountQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class, Pageable.class);
		query.execute(new Object[] { "fake", PageRequest.of(1, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind).as(Person.class);
		verify(withQueryMock, atLeast(1)).matching(captor.capture());

		assertThat(captor.getValue().getMeta().getComment()).isEqualTo("comment");
	}

	@Test // DATAMONGO-957, DATAMONGO-1783
	void metadataShouldBeAddedToStringBasedQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByAnnotatedQuery", String.class, Pageable.class);
		query.execute(new Object[] { "fake", PageRequest.of(0, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind).as(Person.class);
		verify(withQueryMock).matching(captor.capture());

		assertThat(captor.getValue().getMeta().getComment()).isEqualTo("comment");
	}

	@Test // DATAMONGO-1057
	void slicedExecutionShouldRetainNrOfElementsToSkip() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = PageRequest.of(0, 10);
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind, times(2)).as(Person.class);
		verify(withQueryMock, times(2)).matching(captor.capture());

		assertThat(captor.getAllValues().get(0).getSkip()).isZero();
		assertThat(captor.getAllValues().get(1).getSkip()).isEqualTo(10);
	}

	@Test // DATAMONGO-1057
	void slicedExecutionShouldIncrementLimitByOne() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = PageRequest.of(0, 10);
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind, times(2)).as(Person.class);
		verify(withQueryMock, times(2)).matching(captor.capture());

		assertThat(captor.getAllValues().get(0).getLimit()).isEqualTo(11);
		assertThat(captor.getAllValues().get(1).getLimit()).isEqualTo(11);
	}

	@Test // DATAMONGO-1057
	void slicedExecutionShouldRetainSort() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = PageRequest.of(0, 10, Sort.Direction.DESC, "bar");
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind, times(2)).as(Person.class);
		verify(withQueryMock, times(2)).matching(captor.capture());

		Document expectedSortObject = new Document().append("bar", -1);
		assertThat(captor.getAllValues().get(0).getSortObject()).isEqualTo(expectedSortObject);
		assertThat(captor.getAllValues().get(1).getSortObject()).isEqualTo(expectedSortObject);
	}

	@Test // DATAMONGO-1080
	void doesNotTryToPostProcessQueryResultIntoWrapperType() {

		Person reference = new Person();

		doReturn(reference).when(withQueryMock).oneValue();

		AbstractMongoQuery query = createQueryForMethod("findByLastname", String.class);

		assertThat(query.execute(new Object[] { "lastname" })).isEqualTo(reference);
	}

	@Test // DATAMONGO-1865
	void limitingSingleEntityQueryCallsFirst() {

		Person reference = new Person();

		doReturn(reference).when(withQueryMock).firstValue();

		AbstractMongoQuery query = createQueryForMethod("findFirstByLastname", String.class).setLimitingQuery(true);

		assertThat(query.execute(new Object[] { "lastname" })).isEqualTo(reference);
	}

	@Test // DATAMONGO-1872
	void doesNotFixCollectionOnPreparation() {

		AbstractMongoQuery query = createQueryForMethod(DynamicallyMappedRepository.class, "findBy");

		query.execute(new Object[0]);

		verify(executableFind, never()).inCollection(anyString());
		verify(executableFind).as(DynamicallyMapped.class);
	}

	@Test // DATAMONGO-1979
	void usesAnnotatedSortWhenPresent() {

		createQueryForMethod("findByAge", Integer.class) //
				.execute(new Object[] { 1000 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getSortObject()).isEqualTo(new Document("age", 1));
	}

	@Test // DATAMONGO-1979
	void usesExplicitSortOverridesAnnotatedSortWhenPresent() {

		createQueryForMethod("findByAge", Integer.class, Sort.class) //
				.execute(new Object[] { 1000, Sort.by(Direction.DESC, "age") });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getSortObject()).isEqualTo(new Document("age", -1));
	}

	@Test // DATAMONGO-1854
	void shouldApplyStaticAnnotatedCollation() {

		createQueryForMethod("findWithCollationUsingSpimpleStringValueByFirstName", String.class) //
				.execute(new Object[] { "dalinar" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyStaticAnnotatedCollationAsDocument() {

		createQueryForMethod("findWithCollationUsingDocumentByFirstName", String.class) //
				.execute(new Object[] { "dalinar" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationAsString() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.execute(new Object[] { "dalinar", "en_US" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationAsDocument() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.execute(new Object[] { "dalinar", new Document("locale", "en_US") });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationAsLocale() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.execute(new Object[] { "dalinar", Locale.US });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldThrowExceptionOnNonParsableCollation() {

		assertThatIllegalArgumentException().isThrownBy(() -> {

			createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
					.execute(new Object[] { "dalinar", 100 });
		});
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationIn() {

		createQueryForMethod("findWithCollationUsingPlaceholderInDocumentByFirstName", String.class, String.class) //
				.execute(new Object[] { "dalinar", "en_US" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyCollationParameter() {

		Collation collation = Collation.of("en_US");
		createQueryForMethod("findWithCollationParameterByFirstName", String.class, Collation.class) //
				.execute(new Object[] { "dalinar", collation });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	void collationParameterShouldOverrideAnnotation() {

		Collation collation = Collation.of("de_AT");
		createQueryForMethod("findWithWithCollationParameterAndAnnotationByFirstName", String.class, Collation.class) //
				.execute(new Object[] { "dalinar", collation });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	void collationParameterShouldNotBeAppliedWhenNullOverrideAnnotation() {

		createQueryForMethod("findWithWithCollationParameterAndAnnotationByFirstName", String.class, Collation.class) //
				.execute(new Object[] { "dalinar", null });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // GH-2107
	void updateExecutionCallsUpdateAllCorrectly() {

		when(terminatingUpdate.all()).thenReturn(updateResultMock);
		
		createQueryForMethod("findAndIncreaseVisitsByLastname", String.class, int.class) //
				.execute(new Object[] { "dalinar", 100 });

		ArgumentCaptor<UpdateDefinition> update = ArgumentCaptor.forClass(UpdateDefinition.class);
		verify(updateWithQuery).apply(update.capture());
		verify(terminatingUpdate).all();

		assertThat(update.getValue().getUpdateObject()).isEqualTo(Document.parse("{ '$inc' : { 'visits' : 100 } }"));
	}

	private MongoQueryFake createQueryForMethod(String methodName, Class<?>... paramTypes) {
		return createQueryForMethod(Repo.class, methodName, paramTypes);
	}

	private MongoQueryFake createQueryForMethod(Class<?> repository, String methodName, Class<?>... paramTypes) {

		try {

			Method method = repository.getMethod(methodName, paramTypes);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(repository), factory,
					mappingContextMock);

			return new MongoQueryFake(queryMethod, mongoOperationsMock);
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	private static class MongoQueryFake extends AbstractMongoQuery {

		private boolean isDeleteQuery;
		private boolean isLimitingQuery;

		MongoQueryFake(MongoQueryMethod method, MongoOperations operations) {
			super(method, operations, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT);
		}

		@Override
		protected Query createQuery(ConvertingParameterAccessor accessor) {
			return new BasicQuery("{'foo':'bar'}");
		}

		@Override
		protected boolean isCountQuery() {
			return false;
		}

		@Override
		protected boolean isExistsQuery() {
			return false;
		}

		@Override
		protected boolean isDeleteQuery() {
			return isDeleteQuery;
		}

		@Override
		protected boolean isLimiting() {
			return isLimitingQuery;
		}

		MongoQueryFake setDeleteQuery(boolean isDeleteQuery) {
			this.isDeleteQuery = isDeleteQuery;
			return this;
		}

		MongoQueryFake setLimitingQuery(boolean limitingQuery) {

			isLimitingQuery = limitingQuery;
			return this;
		}

		@Override
		protected CodecRegistry getCodecRegistry() {
			return MongoClientSettings.getDefaultCodecRegistry();
		}
	}

	private interface Repo extends MongoRepository<Person, Long> {

		List<Person> deleteByLastname(String lastname);

		Long deletePersonByLastname(String lastname);

		List<Person> findByFirstname(String firstname);

		@Meta(comment = "comment", flags = { org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT })
		Page<Person> findByFirstname(String firstnanme, Pageable pageable);

		@Meta(comment = "comment")
		@org.springframework.data.mongodb.repository.Query("{}")
		Page<Person> findByAnnotatedQuery(String firstnanme, Pageable pageable);

		// DATAMONGO-1057
		Slice<Person> findByLastname(String lastname, Pageable page);

		Optional<Person> findByLastname(String lastname);

		Person findFirstByLastname(String lastname);

		@org.springframework.data.mongodb.repository.Query(sort = "{ age : 1 }")
		List<Person> findByAge(Integer age);

		@org.springframework.data.mongodb.repository.Query(sort = "{ age : 1 }")
		List<Person> findByAge(Integer age, Sort page);

		@org.springframework.data.mongodb.repository.Query(collation = "en_US")
		List<Person> findWithCollationUsingSpimpleStringValueByFirstName(String firstname);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : 'en_US' }")
		List<Person> findWithCollationUsingDocumentByFirstName(String firstname);

		@org.springframework.data.mongodb.repository.Query(collation = "?1")
		List<Person> findWithCollationUsingPlaceholderByFirstName(String firstname, Object collation);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : '?1' }")
		List<Person> findWithCollationUsingPlaceholderInDocumentByFirstName(String firstname, String collation);

		List<Person> findWithCollationParameterByFirstName(String firstname, Collation collation);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : 'en_US' }")
		List<Person> findWithWithCollationParameterAndAnnotationByFirstName(String firstname, Collation collation);

		@Update("{ '$inc' : { 'visits' : ?1 } }")
		void findAndIncreaseVisitsByLastname(String lastname, int value);
	}

	// DATAMONGO-1872

	@org.springframework.data.mongodb.core.mapping.Document("#{T(java.lang.Math).random()}")
	static class DynamicallyMapped {}

	interface DynamicallyMappedRepository extends Repository<DynamicallyMapped, ObjectId> {
		DynamicallyMapped findBy();
	}
}
