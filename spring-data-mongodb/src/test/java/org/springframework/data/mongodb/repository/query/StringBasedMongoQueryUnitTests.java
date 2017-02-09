/*
 * Copyright 2011-2017 the original author or authors.
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
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import org.bson.BSON;
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
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.DefaultEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link StringBasedMongoQuery}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class StringBasedMongoQueryUnitTests {

	SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock MongoOperations operations;
	@Mock DbRefResolver factory;

	MongoConverter converter;

	@Before
	public void setUp() {

		when(operations.getConverter()).thenReturn(converter);

		this.converter = new MappingMongoConverter(factory, new MongoMappingContext());
	}

	@Test
	public void bindsSimplePropertyCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastname", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test
	public void bindsComplexPropertyCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByAddress", Address.class);

		Address address = new Address("Foo", "0123", "Bar");
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, address);

		DBObject dbObject = new BasicDBObject();
		converter.write(address, dbObject);
		dbObject.removeField(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		BasicDBObject queryObject = new BasicDBObject("address", dbObject);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(queryObject);

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test
	public void bindsMultipleParametersCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameAndAddress", String.class, Address.class);

		Address address = new Address("Foo", "0123", "Bar");
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews", address);

		DBObject addressDbObject = new BasicDBObject();
		converter.write(address, addressDbObject);
		addressDbObject.removeField(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);

		DBObject reference = new BasicDBObject("address", addressDbObject);
		reference.put("lastname", "Matthews");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
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

	@Test // DATAMONGO-821
	public void bindsDbrefCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByHavingSizeFansNotZero");
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new BasicQuery("{ fans : { $not : { $size : 0 } } }").getQueryObject()));
	}

	@Test // DATAMONGO-566
	public void constructsDeleteQueryCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("removeByLastname", String.class);
		assertThat(mongoQuery.isDeleteQuery(), is(true));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-566
	public void preventsDeleteAndCountFlagAtTheSameTime() throws Exception {
		createQueryForMethod("invalidMethod", String.class);
	}

	@Test // DATAMONGO-420
	public void shouldSupportFindByParameterizedCriteriaAndFields() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter,
				new BasicDBObject("firstname", "first").append("lastname", "last"), Collections.singletonMap("lastname", 1));
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByParameterizedCriteriaAndFields", DBObject.class,
				Map.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(),
				is(new BasicQuery("{ \"firstname\": \"first\", \"lastname\": \"last\"}").getQueryObject()));
		assertThat(query.getFieldsObject(), is(new BasicQuery(null, "{ \"lastname\": 1}").getFieldsObject()));
	}

	@Test // DATAMONGO-420
	public void shouldSupportRespectExistingQuotingInFindByTitleBeginsWithExplicitQuoting() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "fun");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByTitleBeginsWithExplicitQuoting", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(), is(new BasicQuery("{title: {$regex: '^fun', $options: 'i'}}").getQueryObject()));
	}

	@Test // DATAMONGO-995, DATAMONGO-420
	public void shouldParseQueryWithParametersInExpression() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, 1, 2, 3, 4);
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithParametersInExpression", int.class,
				int.class, int.class, int.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(),
				is(new BasicQuery("{$where: 'return this.date.getUTCMonth() == 3 && this.date.getUTCDay() == 4;'}")
						.getQueryObject()));
	}

	@Test // DATAMONGO-995, DATAMONGO-420
	public void bindsSimplePropertyAlreadyQuotedCorrectly() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-995, DATAMONGO-420
	public void bindsSimplePropertyAlreadyQuotedWithRegexCorrectly() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "^Mat.*");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : '^Mat.*'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-995, DATAMONGO-420
	public void bindsSimplePropertyWithRegexCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastname", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "^Mat.*");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : '^Mat.*'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1070
	public void parsesDbRefDeclarationsCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("methodWithManuallyDefinedDbRef", String.class);
		ConvertingParameterAccessor parameterAccessor = StubParameterAccessor.getAccessor(converter, "myid");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(parameterAccessor);

		DBRef dbRef = DBObjectTestUtils.getTypedValue(query.getQueryObject(), "reference", DBRef.class);
		assertThat(dbRef.getId(), is((Object) "myid"));
		assertThat(dbRef.getCollectionName(), is("reference"));
	}

	@Test // DATAMONGO-1072
	public void shouldParseJsonKeyReplacementCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("methodWithPlaceholderInKeyOfJsonStructure", String.class,
				String.class);
		ConvertingParameterAccessor parameterAccessor = StubParameterAccessor.getAccessor(converter, "key", "value");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(parameterAccessor);

		assertThat(query.getQueryObject(), is(new BasicDBObjectBuilder().add("key", "value").get()));
	}

	@Test // DATAMONGO-990
	public void shouldSupportExpressionsInCustomQueries() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpression", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1244
	public void shouldSupportExpressionsInCustomQueriesWithNestedObject() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, true, "param1", "param2");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpressionAndNestedObject", boolean.class,
				String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{ \"id\" : { \"$exists\" : true}}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1244
	public void shouldSupportExpressionsInCustomQueriesWithMultipleNestedObjects() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, true, "param1", "param2");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpressionAndMultipleNestedObjects",
				boolean.class, String.class, String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(
				"{ \"id\" : { \"$exists\" : true} , \"foo\" : 42 , \"bar\" : { \"$exists\" : false}}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1290
	public void shouldSupportNonQuotedBinaryDataReplacement() throws Exception {

		byte[] binaryData = "Matthews".getBytes("UTF-8");
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, binaryData);
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameAsBinary", byte[].class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : { '$binary' : '"
				+ DatatypeConverter.printBase64Binary(binaryData) + "', '$type' : " + BSON.B_GENERAL + "}}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1454
	public void shouldSupportExistsProjection() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("existsByLastname", String.class);

		assertThat(mongoQuery.isExistsQuery(), is(true));
	}

	@Test // DATAMONGO-1565
	public void bindsPropertyReferenceMultipleTimesCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByAgeQuotedAndUnquoted", Integer.TYPE);

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, 3);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		BasicDBList or = new BasicDBList();
		or.add(new BasicDBObject("age", 3));
		or.add(new BasicDBObject("displayAge", "3"));
		BasicDBObject queryObject = new BasicDBObject("$or", or);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(queryObject);

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1565
	public void shouldIgnorePlaceholderPatternInReplacementValue() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "argWith?1andText",
				"nothing-special");

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByStringWithWildcardChar", String.class, String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(JSON.parse("{ \"arg0\" : \"argWith?1andText\" , \"arg1\" : \"nothing-special\"}")));
	}

	@Test // DATAMONGO-1565
	public void shouldQuoteStringReplacementCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews', password: 'foo");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(not(new BasicDBObjectBuilder().add("lastname", "Matthews").add("password", "foo").get())));
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject("lastname", "Matthews', password: 'foo")));
	}

	@Test // DATAMONGO-1565
	public void shouldQuoteStringReplacementContainingQuotesCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews\", password: \"foo");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(not(new BasicDBObjectBuilder().add("lastname", "Matthews").add("password", "foo").get())));
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject("lastname", "Matthews\", password: \"foo")));
	}

	@Test // DATAMONGO-1565
	public void shouldQuoteStringReplacementWithQuotationsCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter,
				"\"Dave Matthews\", password: 'foo");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is((DBObject) new BasicDBObject("lastname", "\"Dave Matthews\", password: 'foo")));
	}

	@Test // DATAMONGO-1565, DATAMONGO-1575
	public void shouldQuoteComplexQueryStringCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "{ $ne : \"calamity\" }");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject("lastname", "{ $ne : \"calamity\" }")));
	}

	@Test // DATAMONGO-1565, DATAMONGO-1575
	public void shouldQuotationInQuotedComplexQueryString() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter,
				"{ $ne : \"\\\"calamity\\\"\" }");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject("lastname", "{ $ne : \"\\\"calamity\\\"\" }")));
	}

	@Test // DATAMONGO-1575
	public void shouldTakeBsonParameterAsIs() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByWithBsonArgument", DBObject.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter,
				new BasicDBObject("$regex", "^calamity$"));

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject("arg0", Pattern.compile("^calamity$"))));
	}

	@Test // DATAMONGO-1575
	public void shouldReplaceParametersInInQuotedExpressionOfNestedQueryOperator() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameRegex", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject("lastname", Pattern.compile("^(calamity)"))));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowReuseOfPlaceholderWithinQuery() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByReusingPlaceholdersMultipleTimes", String.class,
				String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject().append("arg0", "calamity")
				.append("arg1", "regalia").append("arg2", "calamity")));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowReuseOfQuotedPlaceholderWithinQuery() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByReusingPlaceholdersMultipleTimesWhenQuoted",
				String.class, String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject().append("arg0", "calamity")
				.append("arg1", "regalia").append("arg2", "calamity")));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowReuseOfQuotedPlaceholderWithinQueryAndIncludeSuffixCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod(
				"findByReusingPlaceholdersMultipleTimesWhenQuotedAndSomeStuffAppended", String.class, String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject().append("arg0", "calamity")
				.append("arg1", "regalia").append("arg2", "calamitys")));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowQuotedParameterWithSuffixAppended() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByWhenQuotedAndSomeStuffAppended", String.class,
				String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is((DBObject) new BasicDBObject().append("arg0", "calamity").append("arg1", "regalias")));
	}

	@Test // DATAMONGO-1603
	public void shouldCaptureReplacementWithComplexSuffixCorrectly() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByMultiRegex", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(), is((DBObject) JSON.parse(
				"{ \"$or\" : [ { \"firstname\" : { \"$regex\" : \".*calamity.*\" , \"$options\" : \"i\"}} , { \"lastname\" : { \"$regex\" : \".*calamityxyz.*\" , \"$options\" : \"i\"}}]}")));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowPlaceholderReuseInQuotedValue() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameRegex", String.class, String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(),
				is((DBObject) JSON.parse("{ 'lastname' : { '$regex' : '^(calamity|John regalia|regalia)'} }")));
	}

	@Test // DATAMONGO-1605
	public void findUsingSpelShouldRetainParameterType() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByUsingSpel", Object.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, 100.01D);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject().append("arg0", 100.01D)));
	}

	@Test // DATAMONGO-1605
	public void findUsingSpelShouldRetainNullValues() throws Exception {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByUsingSpel", Object.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, new Object[]{null});

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is((DBObject) new BasicDBObject().append("arg0", null)));
	}

	private StringBasedMongoQuery createQueryForMethod(String name, Class<?>... parameters) throws Exception {

		Method method = SampleRepository.class.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(SampleRepository.class),
				factory, converter.getMappingContext());
		return new StringBasedMongoQuery(queryMethod, operations, PARSER, DefaultEvaluationContextProvider.INSTANCE);
	}

	private interface SampleRepository extends Repository<Person, Long> {

		@Query("{ 'lastname' : ?0 }")
		Person findByLastname(String lastname);

		@Query("{ 'lastname' : ?0 }")
		Person findByLastnameAsBinary(byte[] lastname);

		@Query("{ 'lastname' : '?0' }")
		Person findByLastnameQuoted(String lastname);

		@Query("{ 'lastname' : { '$regex' : '^(?0)'} }")
		Person findByLastnameRegex(String lastname);

		@Query("{'$or' : [{'firstname': {'$regex': '.*?0.*', '$options': 'i'}}, {'lastname' : {'$regex': '.*?0xyz.*', '$options': 'i'}} ]}")
		Person findByMultiRegex(String arg0);

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

		@Query(value = "{ $or : [{'age' : ?0 }, {'displayAge' : '?0'}] }")
		boolean findByAgeQuotedAndUnquoted(int age);

		@Query(value = "{ 'lastname' : ?0 }", exists = true)
		boolean existsByLastname(String lastname);

		@Query("{ 'arg0' : ?0, 'arg1' : ?1 }")
		List<Person> findByStringWithWildcardChar(String arg0, String arg1);

		@Query("{ 'arg0' : ?0 }")
		List<Person> findByWithBsonArgument(DBObject arg0);

		@Query("{ 'arg0' : ?0, 'arg1' : ?1, 'arg2' : ?0 }")
		List<Person> findByReusingPlaceholdersMultipleTimes(String arg0, String arg1);

		@Query("{ 'arg0' : ?0, 'arg1' : ?1, 'arg2' : '?0' }")
		List<Person> findByReusingPlaceholdersMultipleTimesWhenQuoted(String arg0, String arg1);

		@Query("{ 'arg0' : '?0', 'arg1' : ?1, 'arg2' : '?0s' }")
		List<Person> findByReusingPlaceholdersMultipleTimesWhenQuotedAndSomeStuffAppended(String arg0, String arg1);

		@Query("{ 'arg0' : '?0', 'arg1' : '?1s' }")
		List<Person> findByWhenQuotedAndSomeStuffAppended(String arg0, String arg1);

		@Query("{ 'lastname' : { '$regex' : '^(?0|John ?1|?1)'} }") // use spel or some regex string this is fucking bad
		Person findByLastnameRegex(String lastname, String alternative);

		@Query("{ arg0 : ?#{[0]} }")
		List<Person> findByUsingSpel(Object arg0);
	}
}
