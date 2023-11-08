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
package org.springframework.data.mongodb.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Locale;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.ReactiveFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ReactiveFindOperation.ReactiveFind;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveUpdateOperation.ReactiveUpdate;
import org.springframework.data.mongodb.core.ReactiveUpdateOperation.TerminatingUpdate;
import org.springframework.data.mongodb.core.ReactiveUpdateOperation.UpdateWithQuery;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ReactiveExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.result.UpdateResult;

/**
 * Unit tests for {@link AbstractReactiveMongoQuery}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jorge Rodríguez
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AbstractReactiveMongoQueryUnitTests {

	@Mock ReactiveMongoOperations mongoOperationsMock;
	@Mock BasicMongoPersistentEntity<?> persitentEntityMock;
	@Mock MongoMappingContext mappingContextMock;

	@Mock ReactiveFind<?> executableFind;
	@Mock FindWithQuery<?> withQueryMock;
	@Mock ReactiveUpdate executableUpdate;
	@Mock UpdateWithQuery updateWithQuery;
	@Mock TerminatingUpdate terminatingUpdate;

	@BeforeEach
	void setUp() {

		doReturn("persons").when(persitentEntityMock).getCollection();
		doReturn(persitentEntityMock).when(mappingContextMock).getPersistentEntity(Mockito.any(Class.class));
		doReturn(persitentEntityMock).when(mappingContextMock).getRequiredPersistentEntity(Mockito.any(Class.class));
		doReturn(Person.class).when(persitentEntityMock).getType();

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContextMock);
		converter.afterPropertiesSet();

		doReturn(converter).when(mongoOperationsMock).getConverter();

		doReturn(executableFind).when(mongoOperationsMock).query(any());
		doReturn(withQueryMock).when(executableFind).as(any());
		doReturn(withQueryMock).when(withQueryMock).matching(any(Query.class));
		doReturn(Flux.empty()).when(withQueryMock).all();
		doReturn(Mono.empty()).when(withQueryMock).first();
		doReturn(Mono.empty()).when(withQueryMock).one();

		doReturn(executableUpdate).when(mongoOperationsMock).update(any());
		doReturn(executableUpdate).when(executableUpdate).inCollection(anyString());
		doReturn(updateWithQuery).when(executableUpdate).matching(any(Query.class));
		doReturn(terminatingUpdate).when(updateWithQuery).apply(any(UpdateDefinition.class));
	}

	@Test // DATAMONGO-1854
	void shouldApplyStaticAnnotatedCollation() {

		createQueryForMethod("findWithCollationUsingSpimpleStringValueByFirstName", String.class) //
				.executeBlocking(new Object[] { "dalinar" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyStaticAnnotatedCollationAsDocument() {

		createQueryForMethod("findWithCollationUsingDocumentByFirstName", String.class) //
				.executeBlocking(new Object[] { "dalinar" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationAsString() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.executeBlocking(new Object[] { "dalinar", "en_US" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationAsDocument() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.executeBlocking(new Object[] { "dalinar", new Document("locale", "en_US") });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationAsLocale() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.executeBlocking(new Object[] { "dalinar", Locale.US });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldThrowExceptionOnNonParsableCollation() {

		assertThatIllegalArgumentException().isThrownBy(() -> {
			createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
					.executeBlocking(new Object[] { "dalinar", 100 });
		});
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationIn() {

		createQueryForMethod("findWithCollationUsingPlaceholderInDocumentByFirstName", String.class, String.class) //
				.executeBlocking(new Object[] { "dalinar", "en_US" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyDynamicAnnotatedCollationWithMultiplePlaceholders() {

		createQueryForMethod("findWithCollationUsingPlaceholdersInDocumentByFirstName", String.class, String.class,
				int.class) //
						.executeBlocking(new Object[] { "dalinar", "en_US", 2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").strength(2).toDocument());
	}

	@Test // DATAMONGO-1854
	void shouldApplyCollationParameter() {

		Collation collation = Collation.of("en_US");
		createQueryForMethod("findWithCollationParameterByFirstName", String.class, Collation.class) //
				.executeBlocking(new Object[] { "dalinar", collation });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	void collationParameterShouldOverrideAnnotation() {

		Collation collation = Collation.of("de_AT");
		createQueryForMethod("findWithWithCollationParameterAndAnnotationByFirstName", String.class, Collation.class) //
				.executeBlocking(new Object[] { "dalinar", collation });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	void collationParameterShouldNotBeAppliedWhenNullOverrideAnnotation() {

		createQueryForMethod("findWithWithCollationParameterAndAnnotationByFirstName", String.class, Collation.class) //
				.executeBlocking(new Object[] { "dalinar", null });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // GH-3230
	void findShouldApplyHint() {

		createQueryForMethod("findWithHintByFirstname", String.class).executeBlocking(new Object[] { "Jasna" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getHint()).isEqualTo("idx-fn");
	}

	@Test // GH-3230
	void updateShouldApplyHint() {

		when(terminatingUpdate.all()).thenReturn(Mono.just(mock(UpdateResult.class)));

		createQueryForMethod("findAndIncreaseVisitsByLastname", String.class, int.class) //
				.executeBlocking(new Object[] { "dalinar", 100 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(executableUpdate).matching(captor.capture());
		assertThat(captor.getValue().getHint()).isEqualTo("idx-ln");
	}

	@Test // GH-2971
	void findShouldApplyReadPreference() {

		createQueryForMethod("findWithReadPreferenceByFirstname", String.class).executeBlocking(new Object[] { "Jasna" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondaryPreferred());
	}

	private ReactiveMongoQueryFake createQueryForMethod(String methodName, Class<?>... paramTypes) {
		return createQueryForMethod(Repo.class, methodName, paramTypes);
	}

	private ReactiveMongoQueryFake createQueryForMethod(Class<?> repository, String methodName, Class<?>... paramTypes) {

		try {

			Method method = repository.getMethod(methodName, paramTypes);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			ReactiveMongoQueryMethod queryMethod = new ReactiveMongoQueryMethod(method,
					new DefaultRepositoryMetadata(repository), factory, mappingContextMock);

			return new ReactiveMongoQueryFake(queryMethod, mongoOperationsMock);
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	private static class ReactiveMongoQueryFake extends AbstractReactiveMongoQuery {

		private boolean isDeleteQuery;
		private boolean isLimitingQuery;

		ReactiveMongoQueryFake(ReactiveMongoQueryMethod method, ReactiveMongoOperations operations) {
			super(method, operations, new SpelExpressionParser(),
					ReactiveExtensionAwareQueryMethodEvaluationContextProvider.DEFAULT);
		}

		@Override
		protected Mono<Query> createQuery(ConvertingParameterAccessor accessor) {
			return Mono.just(new BasicQuery("{'foo':'bar'}"));
		}

		Object executeBlocking(Object[] parameters) {
			return Flux.from(super.execute(parameters)).collectList().block();
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

		public ReactiveMongoQueryFake setDeleteQuery(boolean isDeleteQuery) {
			this.isDeleteQuery = isDeleteQuery;
			return this;
		}

		public ReactiveMongoQueryFake setLimitingQuery(boolean limitingQuery) {

			isLimitingQuery = limitingQuery;
			return this;
		}

		@Override
		protected Mono<CodecRegistry> getCodecRegistry() {
			return Mono.just(MongoClientSettings.getDefaultCodecRegistry());
		}
	}

	private interface Repo extends ReactiveMongoRepository<Person, Long> {

		@org.springframework.data.mongodb.repository.Query(collation = "en_US")
		List<Person> findWithCollationUsingSpimpleStringValueByFirstName(String firstname);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : 'en_US' }")
		List<Person> findWithCollationUsingDocumentByFirstName(String firstname);

		@org.springframework.data.mongodb.repository.Query(collation = "?1")
		List<Person> findWithCollationUsingPlaceholderByFirstName(String firstname, Object collation);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : '?1' }")
		List<Person> findWithCollationUsingPlaceholderInDocumentByFirstName(String firstname, String collation);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : '?1', 'strength' : ?#{[2]}}")
		List<Person> findWithCollationUsingPlaceholdersInDocumentByFirstName(String firstname, String collation,
				int strength);

		List<Person> findWithCollationParameterByFirstName(String firstname, Collation collation);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : 'en_US' }")
		List<Person> findWithWithCollationParameterAndAnnotationByFirstName(String firstname, Collation collation);

		@Hint("idx-ln")
		@Update("{ '$inc' : { 'visits' : ?1 } }")
		void findAndIncreaseVisitsByLastname(String lastname, int value);

		@Hint("idx-fn")
		void findWithHintByFirstname(String firstname);

		@ReadPreference(value = "secondaryPreferred")
		Flux<Person> findWithReadPreferenceByFirstname(String firstname);
	}
}
