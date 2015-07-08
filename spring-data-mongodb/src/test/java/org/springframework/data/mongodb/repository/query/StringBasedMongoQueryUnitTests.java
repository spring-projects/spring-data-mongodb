/*
 * Copyright 2011-2015 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.DefaultEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

/**
 * Unit tests for {@link StringBasedMongoQuery}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
@RunWith(MockitoJUnitRunner.class)
public class StringBasedMongoQueryUnitTests {

	SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock MongoOperations operations;
	@Mock RepositoryMetadata metadata;
	@Mock DbRefResolver factory;

	MongoConverter converter;

	@Before
	public void setUp() {

		when(operations.getConverter()).thenReturn(converter);

		this.converter = new MappingMongoConverter(factory, new MongoMappingContext());
	}

	@Test
	public void bindsSimplePropertyCorrectly() throws Exception {

		Method method = SampleRepository.class.getMethod("findByLastname", String.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, converter.getMappingContext());
		StringBasedMongoQuery mongoQuery = new StringBasedMongoQuery(queryMethod, operations, PARSER,
				DefaultEvaluationContextProvider.INSTANCE);
		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, "Matthews");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test
	public void bindsComplexPropertyCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByAddress", Address.class);

		Address address = new Address("Foo", "0123", "Bar");
		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, address);

		DBObject dbObject = new BasicDBObject();
		converter.write(address, dbObject);
		dbObject.removeField(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		BasicDBObject queryObject = new BasicDBObject("address", dbObject);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(queryObject);

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test
	public void bindsMultipleParametersCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameAndAddress", String.class, Address.class);

		Address address = new Address("Foo", "0123", "Bar");
		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, "Matthews", address);

		DBObject addressDbObject = new BasicDBObject();
		converter.write(address, addressDbObject);
		addressDbObject.removeField(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);

		DBObject reference = new BasicDBObject("address", addressDbObject);
		reference.put("lastname", "Matthews");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		assertThat(query.getQueryObject(), is(reference));
	}

	@Test
	public void bindsNullParametersCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByAddress", Address.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, new Object[] { null });

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject().containsField("address"), is(true));
		assertThat(query.getQueryObject().get("address"), is(nullValue()));
	}

	/**
	 * @see DATAMONGO-821
	 */
	@Test
	public void bindsDbrefCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByHavingSizeFansNotZero");
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, new Object[] {});

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new BasicQuery("{ fans : { $not : { $size : 0 } } }").getQueryObject()));
	}

	/**
	 * @see DATAMONGO-566
	 */
	@Test
	public void constructsDeleteQueryCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("removeByLastname", String.class);
		assertThat(mongoQuery.isDeleteQuery(), is(true));
	}

	/**
	 * @see DATAMONGO-566
	 */
	@Test(expected = IllegalArgumentException.class)
	public void preventsDeleteAndCountFlagAtTheSameTime() throws Exception {
		createQueryForMethod("invalidMethod", String.class);
	}

	/**
	 * @see DATAMONGO-420
	 */
	@Test
	public void shouldSupportFindByParameterizedCriteriaAndFields() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, new Object[] {
				new BasicDBObject("firstname", "first").append("lastname", "last"), Collections.singletonMap("lastname", 1) });
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByParameterizedCriteriaAndFields", DBObject.class,
				Map.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(),
				is(new BasicQuery("{ \"firstname\": \"first\", \"lastname\": \"last\"}").getQueryObject()));
		assertThat(query.getFieldsObject(), is(new BasicQuery(null, "{ \"lastname\": 1}").getFieldsObject()));
	}

	/**
	 * @see DATAMONGO-420
	 */
	@Test
	public void shouldSupportRespectExistingQuotingInFindByTitleBeginsWithExplicitQuoting() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, new Object[] { "fun" });
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByTitleBeginsWithExplicitQuoting", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(), is(new BasicQuery("{title: {$regex: '^fun', $options: 'i'}}").getQueryObject()));
	}

	/**
	 * @see DATAMONGO-995, DATAMONGO-420
	 */
	@Test
	public void shouldParseQueryWithParametersInExpression() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, new Object[] { 1, 2, 3, 4 });
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithParametersInExpression", int.class,
				int.class, int.class, int.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(),
				is(new BasicQuery("{$where: 'return this.date.getUTCMonth() == 3 && this.date.getUTCDay() == 4;'}")
						.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-995, DATAMONGO-420
	 */
	@Test
	public void bindsSimplePropertyAlreadyQuotedCorrectly() throws Exception {

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, "Matthews");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-995, DATAMONGO-420
	 */
	@Test
	public void bindsSimplePropertyAlreadyQuotedWithRegexCorrectly() throws Exception {

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, "^Mat.*");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : '^Mat.*'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-995, DATAMONGO-420
	 */
	@Test
	public void bindsSimplePropertyWithRegexCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastname", String.class);
		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, "^Mat.*");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : '^Mat.*'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-1070
	 */
	@Test
	public void parsesDbRefDeclarationsCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("methodWithManuallyDefinedDbRef", String.class);
		ConvertingParameterAccessor parameterAccessor = StubParameterAccessor.getAccessor(converter, "myid");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(parameterAccessor);

		DBRef dbRef = DBObjectTestUtils.getTypedValue(query.getQueryObject(), "reference", DBRef.class);
		assertThat(dbRef.getId(), is((Object) "myid"));
		assertThat(dbRef.getCollectionName(), is("reference"));
	}

	/**
	 * @see DATAMONGO-1072
	 */
	@Test
	public void shouldParseJsonKeyReplacementCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("methodWithPlaceholderInKeyOfJsonStructure", String.class,
				String.class);
		ConvertingParameterAccessor parameterAccessor = StubParameterAccessor.getAccessor(converter, "key", "value");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(parameterAccessor);

		assertThat(query.getQueryObject(), is(new BasicDBObjectBuilder().add("key", "value").get()));
	}

	/**
	 * @see DATAMONGO-990
	 */
	@Test
	public void shouldSupportExpressionsInCustomQueries() throws Exception {

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, "Matthews");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpression", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-1244
	 */
	@Test
	public void shouldSupportExpressionsInCustomQueriesWithNestedObject() throws Exception {

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, true, "param1", "param2");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpressionAndNestedObject", boolean.class,
				String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{ \"id\" : { \"$exists\" : true}}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-1244
	 */
	@Test
	public void shouldSupportExpressionsInCustomQueriesWithMultipleNestedObjects() throws Exception {

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, true, "param1", "param2");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpressionAndMultipleNestedObjects",
				boolean.class, String.class, String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(
				"{ \"id\" : { \"$exists\" : true} , \"foo\" : 42 , \"bar\" : { \"$exists\" : false}}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	private StringBasedMongoQuery createQueryForMethod(String name, Class<?>... parameters) throws Exception {

		Method method = SampleRepository.class.getMethod(name, parameters);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, converter.getMappingContext());
		return new StringBasedMongoQuery(queryMethod, operations, PARSER, DefaultEvaluationContextProvider.INSTANCE);
	}

	private interface SampleRepository {

		@Query("{ 'lastname' : ?0 }")
		Person findByLastname(String lastname);

		@Query("{ 'lastname' : '?0' }")
		Person findByLastnameQuoted(String lastname);

		@Query("{ 'address' : ?0 }")
		Person findByAddress(Address address);

		@Query("{ 'lastname' : ?0, 'address' : ?1 }")
		Person findByLastnameAndAddress(String lastname, Address address);

		@Query("{ fans : { $not : { $size : 0 } } }")
		Person findByHavingSizeFansNotZero();

		@Query(value = "{ 'lastname' : ?0 }", delete = true)
		void removeByLastname(String lastname);

		@Query(value = "{ 'lastname' : ?0 }", delete = true, count = true)
		void invalidMethod(String lastname);

		@Query(value = "?0", fields = "?1")
		DBObject findByParameterizedCriteriaAndFields(DBObject criteria, Map<String, Integer> fields);

		@Query("{'title': { $regex : '^?0', $options : 'i'}}")
		List<DBObject> findByTitleBeginsWithExplicitQuoting(String title);

		@Query("{$where: 'return this.date.getUTCMonth() == ?2 && this.date.getUTCDay() == ?3;'}")
		List<DBObject> findByQueryWithParametersInExpression(int param1, int param2, int param3, int param4);

		@Query("{ 'reference' : { $ref : 'reference', $id : ?0 }}")
		Object methodWithManuallyDefinedDbRef(String id);

		@Query("{ ?0 : ?1}")
		Object methodWithPlaceholderInKeyOfJsonStructure(String keyReplacement, String valueReplacement);

		@Query("{'lastname': ?#{[0]} }")
		List<Person> findByQueryWithExpression(String param0);

		@Query("{'id':?#{ [0] ? { $exists :true} : [1] }}")
		List<Person> findByQueryWithExpressionAndNestedObject(boolean param0, String param1);

		@Query("{'id':?#{ [0] ? { $exists :true} : [1] }, 'foo':42, 'bar': ?#{ [0] ? { $exists :false} : [1] }}")
		List<Person> findByQueryWithExpressionAndMultipleNestedObjects(boolean param0, String param1, String param2);
	}
}
