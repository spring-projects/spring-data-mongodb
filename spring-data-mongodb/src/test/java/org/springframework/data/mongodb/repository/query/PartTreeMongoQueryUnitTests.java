/*
 * Copyright 2014-2024 the original author or authors.
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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.lang.reflect.Method;
import java.util.List;

import org.bson.Document;
import org.bson.json.JsonParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.ExecutableFindOperation.ExecutableFind;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Unit tests for {@link PartTreeMongoQuery}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class PartTreeMongoQueryUnitTests {

	@Mock MongoOperations mongoOperationsMock;
	@Mock ExecutableFind<?> findOperationMock;

	private MongoMappingContext mappingContext;

	@BeforeEach
	void setUp() {

		mappingContext = new MongoMappingContext();
		MongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);

		doReturn(converter).when(mongoOperationsMock).getConverter();
		doReturn(findOperationMock).when(mongoOperationsMock).query(any());
	}

	@Test // DATAMOGO-952
	void rejectsInvalidFieldSpecification() {

		assertThatIllegalStateException().isThrownBy(() -> deriveQueryFromMethod("findByLastname", "foo"))
				.withMessageContaining("findByLastname");
	}

	@Test // DATAMOGO-952
	void singleFieldJsonIncludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByFirstname", "foo");

		assertThat(query.getFieldsObject()).isEqualTo(new Document().append("firstname", 1));
	}

	@Test // DATAMOGO-952
	void multiFieldJsonIncludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByFirstnameAndLastname", "foo",
				"bar");

		assertThat(query.getFieldsObject()).isEqualTo(new Document().append("firstname", 1).append("lastname", 1));
	}

	@Test // DATAMOGO-952
	void multiFieldJsonExcludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findPersonByFirstnameAndLastname",
				"foo", "bar");

		assertThat(query.getFieldsObject()).isEqualTo(new Document().append("firstname", 0).append("lastname", 0));
	}

	@Test // DATAMOGO-973
	void shouldAddFullTextParamCorrectlyToDerivedQuery() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findPersonByFirstname", "text",
				TextCriteria.forDefaultLanguage().matching("search"));

		assertThat(query.getQueryObject()).containsEntry("$text.$search", "search").containsEntry("firstname", "text");
	}

	@Test // DATAMONGO-1180
	void propagatesRootExceptionForInvalidQuery() {

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> deriveQueryFromMethod("findByAge", 1))
				.withCauseInstanceOf(JsonParseException.class);
	}

	@Test // DATAMONGO-1345, DATAMONGO-1735
	void doesNotDeriveFieldSpecForNormalDomainType() {
		assertThat(deriveQueryFromMethod("findPersonBy", new Object[0]).getFieldsObject()).isEmpty();
	}

	@Test // DATAMONGO-1345
	void restrictsQueryToFieldsRequiredForProjection() {

		Document fieldsObject = deriveQueryFromMethod("findPersonProjectedBy", new Object[0]).getFieldsObject();

		assertThat(fieldsObject.get("firstname")).isEqualTo(1);
		assertThat(fieldsObject.get("lastname")).isEqualTo(1);
	}

	@Test // DATAMONGO-1345
	void restrictsQueryToFieldsRequiredForDto() {

		Document fieldsObject = deriveQueryFromMethod("findPersonDtoByAge", new Object[] { 42 }).getFieldsObject();

		assertThat(fieldsObject.get("firstname")).isEqualTo(1);
		assertThat(fieldsObject.get("lastname")).isEqualTo(1);
	}

	@Test // DATAMONGO-1345
	void usesDynamicProjection() {

		Document fields = deriveQueryFromMethod("findDynamicallyProjectedBy", ExtendedProjection.class).getFieldsObject();

		assertThat(fields.get("firstname")).isEqualTo(1);
		assertThat(fields.get("lastname")).isEqualTo(1);
		assertThat(fields.get("age")).isEqualTo(1);
	}

	@Test // DATAMONGO-1500
	void shouldLeaveParameterConversionToQueryMapper() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findBySex", Sex.FEMALE);

		assertThat(query.getQueryObject().get("sex")).isEqualTo(Sex.FEMALE);
		assertThat(query.getFieldsObject().get("firstname")).isEqualTo(1);
	}

	@Test // DATAMONGO-1729, DATAMONGO-1735
	void doesNotCreateFieldsObjectForOpenProjection() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findAllBy");

		assertThat(query.getFieldsObject()).isEmpty();
	}

	@Test // DATAMONGO-1865
	void limitingReturnsTrueIfTreeIsLimiting() {
		assertThat(createQueryForMethod("findFirstBy").isLimiting()).isTrue();
	}

	@Test // DATAMONGO-1865
	void limitingReturnsFalseIfTreeIsNotLimiting() {
		assertThat(createQueryForMethod("findPersonBy").isLimiting()).isFalse();
	}

	private org.springframework.data.mongodb.core.query.Query deriveQueryFromMethod(String method, Object... args) {

		Class<?>[] types = new Class<?>[args.length];

		for (int i = 0; i < args.length; i++) {
			types[i] = args[i].getClass();
		}

		PartTreeMongoQuery partTreeQuery = createQueryForMethod(method, types);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(partTreeQuery.getQueryMethod(), args);
		return partTreeQuery.createQuery(new ConvertingParameterAccessor(mongoOperationsMock.getConverter(), accessor));
	}

	private PartTreeMongoQuery createQueryForMethod(String methodName, Class<?>... paramTypes) {

		try {

			Method method = Repo.class.getMethod(methodName, paramTypes);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(Repo.class), factory,
					mappingContext);

			return new PartTreeMongoQuery(queryMethod, mongoOperationsMock, new SpelExpressionParser(),
					QueryMethodEvaluationContextProvider.DEFAULT);
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	interface Repo extends MongoRepository<Person, Long> {

		@Query(fields = "firstname")
		Person findByLastname(String lastname);

		@Query(fields = "{ 'firstname' : 1 }")
		Person findByFirstname(String lastname);

		@Query(fields = "{ 'firstname' : 1, 'lastname' : 1 }")
		Person findByFirstnameAndLastname(String firstname, String lastname);

		@Query(fields = "{ 'firstname' : 0, 'lastname' : 0 }")
		Person findPersonByFirstnameAndLastname(String firstname, String lastname);

		Person findPersonByFirstname(String firstname, TextCriteria fullText);

		@Query(fields = "{ 'firstname }")
		Person findByAge(Integer age);

		Person findPersonBy();

		PersonProjection findPersonProjectedBy();

		PersonDto findPersonDtoByAge(Integer age);

		<T> T findDynamicallyProjectedBy(Class<T> type);

		@Query(fields = "{ 'firstname' : 1 }")
		List<Person> findBySex(Sex sex);

		OpenProjection findAllBy();

		Person findFirstBy();
	}

	interface PersonProjection {

		String getFirstname();

		String getLastname();
	}

	interface ExtendedProjection extends PersonProjection {

		int getAge();
	}

	static class PersonDto {

		public String firstname, lastname;

		public PersonDto(String firstname, String lastname) {

			this.firstname = firstname;
			this.lastname = lastname;
		}
	}

	interface OpenProjection {

		String getFirstname();

		@Value("#{target.firstname + ' ' + target.lastname}")
		String getFullname();
	}
}
