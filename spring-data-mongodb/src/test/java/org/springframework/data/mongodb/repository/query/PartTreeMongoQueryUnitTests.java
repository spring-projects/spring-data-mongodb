/*
 * Copyright 2014 the original author or authors.
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

import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.core.RepositoryMetadata;

import com.mongodb.BasicDBObjectBuilder;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class PartTreeMongoQueryUnitTests {

	private @Mock RepositoryMetadata metadataMock;
	private MongoMappingContext mappingContext;
	private @Mock MongoOperations mongoOperationsMock;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Before
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
	public void nonJsonSingleFieldRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByLastname",
				new Object[] { "foo" });

		Assert.assertThat(query.getFieldsObject(), IsEqual.equalTo(new BasicDBObjectBuilder().add("firstname", 1).get()));
	}

	/**
	 * @see DATAMOGO-952
	 */
	@Test
	public void singleFieldJsonIncludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByFirstname",
				new Object[] { "foo" });

		Assert.assertThat(query.getFieldsObject(), IsEqual.equalTo(new BasicDBObjectBuilder().add("firstname", 1).get()));
	}

	/**
	 * @see DATAMOGO-952
	 */
	@Test
	public void multiFieldJsonIncludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findByFirstnameAndLastname",
				new Object[] { "foo", "bar" });

		Assert.assertThat(query.getFieldsObject(),
				IsEqual.equalTo(new BasicDBObjectBuilder().add("firstname", 1).add("lastname", 1).get()));
	}

	/**
	 * @see DATAMOGO-952
	 */
	@Test
	public void multiFieldJsonExcludeRestrictionShouldBeConsidered() {

		org.springframework.data.mongodb.core.query.Query query = deriveQueryFromMethod("findPersonByFirstnameAndLastname",
				new Object[] { "foo", "bar" });

		Assert.assertThat(query.getFieldsObject(),
				IsEqual.equalTo(new BasicDBObjectBuilder().add("firstname", 0).add("lastname", 0).get()));
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

			return new PartTreeMongoQuery(queryMethod, mongoOperationsMock);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	private interface Repo extends MongoRepository<Person, Long> {

		@Query(fields = "firstname")
		Person findByLastname(String lastname);

		@Query(fields = "{ 'firstname' : 1 }")
		Person findByFirstname(String lastname);

		@Query(fields = "{ 'firstname' : 1, 'lastname' : 1 }")
		Person findByFirstnameAndLastname(String firstname, String lastname);

		@Query(fields = "{ 'firstname' : 0, 'lastname' : 0 }")
		Person findPersonByFirstnameAndLastname(String firstname, String lastname);

	}

}
