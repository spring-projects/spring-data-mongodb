/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.IsTextQuery.*;

import java.lang.reflect.Method;
import java.util.List;

import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.ExecutableFindOperation.ExecutableFind;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

import com.mongodb.util.JSONParseException;

/**
 * Unit tests for {@link PartTreeMongoQuery}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class PartTreeMongoQueryUnitTests {

	@Mock MongoOperations mongoOperationsMock;
	@Mock ExecutableFind<?> findOperationMock;

	MongoMappingContext mappingContext;

	public @Rule ExpectedException exception = ExpectedException.none();

	@Before
	public void setUp() {

		mappingContext = new MongoMappingContext();
		DbRefResolver dbRefResolver = new DefaultDbRefResolver(mock(MongoDbFactory.class));
		MongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);

		doReturn(converter).when(mongoOperationsMock).getConverter();
		doReturn(findOperationMock).when(mongoOperationsMock).query(any());
	}

	@Test // DATAMOGO-952
	public void rejectsInvalidFieldSpecification() {

		exception.expect(IllegalStateException.class);
		exception.expectMessage("findByLastname");

		deriveQueryFromMethod("findByLastname", "foo");
	}

	@Test // DATAMOGO-952
	public void singleFieldJsonIncludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByFirstname", "foo");

		assertThat(query.getFieldsObject(), is(new Document().append("firstname", 1)));
	}

	@Test // DATAMOGO-952
	public void multiFieldJsonIncludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByFirstnameAndLastname", "foo",
				"bar");

		assertThat(query.getFieldsObject(), is(new Document().append("firstname", 1).append("lastname", 1)));
	}

	@Test // DATAMOGO-952
	public void multiFieldJsonExcludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findPersonByFirstnameAndLastname",
				"foo", "bar");

		assertThat(query.getFieldsObject(), is(new Document().append("firstname", 0).append("lastname", 0)));
	}

	@Test // DATAMOGO-973
	public void shouldAddFullTextParamCorrectlyToDerivedQuery() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findPersonByFirstname", "text",
				TextCriteria.forDefaultLanguage().matching("search"));

		assertThat(query, isTextQuery().searchingFor("search").where(new Criteria("firstname").is("text")));
	}

	@Test // DATAMONGO-1180
	public void propagatesRootExceptionForInvalidQuery() {

		exception.expect(IllegalStateException.class);
		exception.expectCause(is(instanceOf(JSONParseException.class)));

		deriveQueryFromMethod("findByAge", 1);
	}

	@Test // DATAMONGO-1345, DATAMONGO-1735
	public void doesNotDeriveFieldSpecForNormalDomainType() {
		assertThat(deriveQueryFromMethod("findPersonBy", new Object[0]).getFieldsObject(), is(new Document()));
	}

	@Test // DATAMONGO-1345
	public void restrictsQueryToFieldsRequiredForProjection() {

		Document fieldsObject = deriveQueryFromMethod("findPersonProjectedBy", new Object[0]).getFieldsObject();

		assertThat(fieldsObject.get("firstname"), is(1));
		assertThat(fieldsObject.get("lastname"), is(1));
	}

	@Test // DATAMONGO-1345
	public void restrictsQueryToFieldsRequiredForDto() {

		Document fieldsObject = deriveQueryFromMethod("findPersonDtoByAge", new Object[] { 42 }).getFieldsObject();

		assertThat(fieldsObject.get("firstname"), is(1));
		assertThat(fieldsObject.get("lastname"), is(1));
	}

	@Test // DATAMONGO-1345
	public void usesDynamicProjection() {

		Document fields = deriveQueryFromMethod("findDynamicallyProjectedBy", ExtendedProjection.class).getFieldsObject();

		assertThat(fields.get("firstname"), is(1));
		assertThat(fields.get("lastname"), is(1));
		assertThat(fields.get("age"), is(1));
	}

	@Test // DATAMONGO-1500
	public void shouldLeaveParameterConversionToQueryMapper() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findBySex", Sex.FEMALE);

		assertThat(query.getQueryObject().get("sex"), is(Sex.FEMALE));
		assertThat(query.getFieldsObject().get("firstname"), is(1));
	}

	@Test // DATAMONGO-1729, DATAMONGO-1735
	public void doesNotCreateFieldsObjectForOpenProjection() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findAllBy");

		assertThat(query.getFieldsObject(), is(new Document()));
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

			return new PartTreeMongoQuery(queryMethod, mongoOperationsMock);
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
