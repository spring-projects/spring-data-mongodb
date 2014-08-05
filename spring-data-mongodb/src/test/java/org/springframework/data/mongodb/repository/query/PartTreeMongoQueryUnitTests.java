/*
 * Copyright 2014-2015 the original author or authors.
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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.data.mongodb.core.query.IsTextQuery.isTextQuery;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
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
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.DefaultEvaluationContextProvider;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.util.JSONParseException;

/**
 * Unit tests for {@link PartTreeMongoQuery}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@RunWith(MockitoJUnitRunner.class)
public class PartTreeMongoQueryUnitTests {

	@Mock RepositoryMetadata metadataMock;
	@Mock MongoOperations mongoOperationsMock;

	MongoMappingContext mappingContext;

	public @Rule ExpectedException exception = ExpectedException.none();

	@Before
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setUp() {

		when(metadataMock.getDomainType()).thenReturn((Class) Person.class);
		when(metadataMock.getReturnedDomainClass(Matchers.any(Method.class))).thenReturn((Class) Person.class);
		mappingContext = new MongoMappingContext();
		DbRefResolver dbRefResolver = new DefaultDbRefResolver(mock(MongoDbFactory.class));
		MongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);

		when(mongoOperationsMock.getConverter()).thenReturn(converter);
	}

	/**
	 * @see DATAMOGO-952
	 */
	@Test
	public void rejectsInvalidFieldSpecification() {

		exception.expect(IllegalStateException.class);
		exception.expectMessage("findByLastname");

		deriveQueryFromMethod("findByLastname", new Object[] { "foo" });
	}

	/**
	 * @see DATAMOGO-952
	 */
	@Test
	public void singleFieldJsonIncludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByFirstname",
				new Object[] { "foo" });

		assertThat(query.getFieldsObject(), is(new BasicDBObjectBuilder().add("firstname", 1).get()));
	}

	/**
	 * @see DATAMOGO-952
	 */
	@Test
	public void multiFieldJsonIncludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByFirstnameAndLastname",
				new Object[] { "foo", "bar" });

		assertThat(query.getFieldsObject(), is(new BasicDBObjectBuilder().add("firstname", 1).add("lastname", 1).get()));
	}

	/**
	 * @see DATAMOGO-952
	 */
	@Test
	public void multiFieldJsonExcludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findPersonByFirstnameAndLastname",
				new Object[] { "foo", "bar" });

		assertThat(query.getFieldsObject(), is(new BasicDBObjectBuilder().add("firstname", 0).add("lastname", 0).get()));
	}

	/**
	 * @see DATAMOGO-973
	 */
	@Test
	public void shouldAddFullTextParamCorrectlyToDerivedQuery() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findPersonByFirstname",
				new Object[] { "text", TextCriteria.forDefaultLanguage().matching("search") });

		assertThat(query, isTextQuery().searchingFor("search").where(new Criteria("firstname").is("text")));
	}

	/**
	 * @see DATAMONGO-1180
	 */
	@Test
	public void propagatesRootExceptionForInvalidQuery() {

		exception.expect(IllegalStateException.class);
		exception.expectCause(is(org.hamcrest.Matchers.<Throwable> instanceOf(JSONParseException.class)));

		deriveQueryFromMethod("findByAge", new Object[] { 1 });
	}

	private org.springframework.data.mongodb.core.query.Query deriveQueryFromMethod(String method, Object[] args) {

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
			MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadataMock, mappingContext);

			return new PartTreeMongoQuery(queryMethod, mongoOperationsMock, DefaultEvaluationContextProvider.INSTANCE);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		} catch (SecurityException e) {
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
	}
}
