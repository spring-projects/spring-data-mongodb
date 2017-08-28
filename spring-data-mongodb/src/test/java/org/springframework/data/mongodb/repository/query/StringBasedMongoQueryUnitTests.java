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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.bson.BSON;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.ExecutableFindOperation.ExecutableFind;
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
	@Mock ExecutableFind<Object> findOperation;
	@Mock DbRefResolver factory;

	MongoConverter converter;

	@Before
	public void setUp() {

		this.converter = new MappingMongoConverter(factory, new MongoMappingContext());

		doReturn(findOperation).when(operations).query(any());
	}

	@Test
	public void bindsSimplePropertyCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastname", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test
	public void bindsComplexPropertyCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByAddress", Address.class);

		Address address = new Address("Foo", "0123", "Bar");
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, address);

		Document document = new Document();
		converter.write(address, document);
		document.remove(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		Document queryObject = new Document("address", document);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(queryObject);

		assertThat(query.getQueryObject().toJson(), is(reference.getQueryObject().toJson()));
	}

	@Test
	public void bindsMultipleParametersCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameAndAddress", String.class, Address.class);

		Address address = new Address("Foo", "0123", "Bar");
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews", address);

		Document addressDocument = new Document();
		converter.write(address, addressDocument);
		addressDocument.remove(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);

		Document reference = new Document("lastname", "Matthews");
		reference.append("address", addressDocument);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject().toJson(), is(reference.toJson()));
	}

	@Test
	public void bindsNullParametersCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByAddress", Address.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, new Object[] { null });

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject().containsKey("address"), is(true));
		assertThat(query.getQueryObject().get("address"), is(nullValue()));
	}

	@Test // DATAMONGO-821
	public void bindsDbrefCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByHavingSizeFansNotZero");
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new BasicQuery("{ fans : { $not : { $size : 0 } } }").getQueryObject()));
	}

	@Test // DATAMONGO-566
	public void constructsDeleteQueryCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("removeByLastname", String.class);
		assertThat(mongoQuery.isDeleteQuery(), is(true));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-566
	public void preventsDeleteAndCountFlagAtTheSameTime() {
		createQueryForMethod("invalidMethod", String.class);
	}

	@Test // DATAMONGO-420
	public void shouldSupportFindByParameterizedCriteriaAndFields() {

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter,
				StubParameterAccessor.getAccessor(converter, //
						new Document("firstname", "first").append("lastname", "last"), //
						Collections.singletonMap("lastname", 1)));

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByParameterizedCriteriaAndFields", Document.class,
				Map.class);
		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(),
				is(new BasicQuery("{ \"firstname\": \"first\", \"lastname\": \"last\"}").getQueryObject()));
		assertThat(query.getFieldsObject(), is(new BasicQuery(null, "{ \"lastname\": 1}").getFieldsObject()));
	}

	@Test // DATAMONGO-420
	public void shouldSupportRespectExistingQuotingInFindByTitleBeginsWithExplicitQuoting() {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "fun");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByTitleBeginsWithExplicitQuoting", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject().toJson(),
				is(new BasicQuery("{title: {$regex: '^fun', $options: 'i'}}").getQueryObject().toJson()));
	}

	@Test // DATAMONGO-995, DATAMONGO-420
	public void shouldParseQueryWithParametersInExpression() {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, 1, 2, 3, 4);
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithParametersInExpression", int.class,
				int.class, int.class, int.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(),
				is(new BasicQuery("{$where: 'return this.date.getUTCMonth() == 3 && this.date.getUTCDay() == 4;'}")
						.getQueryObject()));
	}

	@Test // DATAMONGO-995, DATAMONGO-420
	public void bindsSimplePropertyAlreadyQuotedCorrectly() {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-995, DATAMONGO-420
	public void bindsSimplePropertyAlreadyQuotedWithRegexCorrectly() {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "^Mat.*");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : '^Mat.*'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-995, DATAMONGO-420
	public void bindsSimplePropertyWithRegexCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastname", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "^Mat.*");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : '^Mat.*'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1070
	public void parsesDbRefDeclarationsCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("methodWithManuallyDefinedDbRef", String.class);
		ConvertingParameterAccessor parameterAccessor = StubParameterAccessor.getAccessor(converter, "myid");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(parameterAccessor);

		Document dbRef = DocumentTestUtils.getTypedValue(query.getQueryObject(), "reference", Document.class);
		assertThat(dbRef, is(new Document("$ref", "reference").append("$id", "myid")));
	}

	@Test // DATAMONGO-1072
	public void shouldParseJsonKeyReplacementCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("methodWithPlaceholderInKeyOfJsonStructure", String.class,
				String.class);
		ConvertingParameterAccessor parameterAccessor = StubParameterAccessor.getAccessor(converter, "key", "value");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(parameterAccessor);

		assertThat(query.getQueryObject(), is(new Document().append("key", "value")));
	}

	@Test // DATAMONGO-990
	public void shouldSupportExpressionsInCustomQueries() {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpression", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1244
	public void shouldSupportExpressionsInCustomQueriesWithNestedObject() {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, true, "param1", "param2");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpressionAndNestedObject", boolean.class,
				String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{ \"id\" : { \"$exists\" : true}}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1244
	public void shouldSupportExpressionsInCustomQueriesWithMultipleNestedObjects() {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, true, "param1", "param2");
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpressionAndMultipleNestedObjects",
				boolean.class, String.class, String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(
				"{ \"id\" : { \"$exists\" : true} , \"foo\" : 42 , \"bar\" : { \"$exists\" : false}}");

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1290
	public void shouldSupportNonQuotedBinaryDataReplacement() {

		byte[] binaryData = "Matthews".getBytes(StandardCharsets.UTF_8);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, (Object) binaryData);
		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameAsBinary", byte[].class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : { '$binary' : '"
				+ DatatypeConverter.printBase64Binary(binaryData) + "', '$type' : '" + BSON.B_GENERAL + "'}}");

		assertThat(query.getQueryObject().toJson(), is(reference.getQueryObject().toJson()));
	}

	@Test // DATAMONGO-1454
	public void shouldSupportExistsProjection() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("existsByLastname", String.class);

		assertThat(mongoQuery.isExistsQuery(), is(true));
	}

	@Test // DATAMONGO-1565
	public void bindsPropertyReferenceMultipleTimesCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByAgeQuotedAndUnquoted", Integer.TYPE);

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, 3);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		List<Object> or = new ArrayList<>();
		or.add(new Document("age", 3));
		or.add(new Document("displayAge", "3"));
		Document queryObject = new Document("$or", or);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(queryObject);

		assertThat(query.getQueryObject(), is(reference.getQueryObject()));
	}

	@Test // DATAMONGO-1565
	public void shouldIgnorePlaceholderPatternInReplacementValue() {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "argWith?1andText",
				"nothing-special");

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByStringWithWildcardChar", String.class, String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(Document.parse("{ \"arg0\" : \"argWith?1andText\" , \"arg1\" : \"nothing-special\"}")));
	}

	@Test // DATAMONGO-1565
	public void shouldQuoteStringReplacementCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews', password: 'foo");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(not(new Document().append("lastname", "Matthews").append("password", "foo"))));
		assertThat(query.getQueryObject(), is(new Document("lastname", "Matthews', password: 'foo")));
	}

	@Test // DATAMONGO-1565
	public void shouldQuoteStringReplacementContainingQuotesCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "Matthews\", password: \"foo");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(not(new Document().append("lastname", "Matthews").append("password", "foo"))));
		assertThat(query.getQueryObject(), is(new Document("lastname", "Matthews\", password: \"foo")));
	}

	@Test // DATAMONGO-1565
	public void shouldQuoteStringReplacementWithQuotationsCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter,
				"\"Dave Matthews\", password: 'foo");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new Document("lastname", "\"Dave Matthews\", password: 'foo")));
	}

	@Test // DATAMONGO-1565, DATAMONGO-1575
	public void shouldQuoteComplexQueryStringCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "{ $ne : \"calamity\" }");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new Document("lastname", "{ $ne : \"calamity\" }")));
	}

	@Test // DATAMONGO-1565, DATAMONGO-1575
	public void shouldQuotationInQuotedComplexQueryString() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameQuoted", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter,
				"{ $ne : \"\\\"calamity\\\"\" }");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(), is(new Document("lastname", "{ $ne : \"\\\"calamity\\\"\" }")));
	}

	@Test // DATAMONGO-1575, DATAMONGO-1770
	public void shouldTakeBsonParameterAsIs() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByWithBsonArgument", Document.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter,
				new Document("$regex", "^calamity$"));

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new Document("arg0", new Document("$regex", "^calamity$"))));
	}

	@Test // DATAMONGO-1575, DATAMONGO-1770
	public void shouldReplaceParametersInInQuotedExpressionOfNestedQueryOperator() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameRegex", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new Document("lastname", new Document("$regex", "^(calamity)"))));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowReuseOfPlaceholderWithinQuery() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByReusingPlaceholdersMultipleTimes", String.class,
				String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(new Document().append("arg0", "calamity").append("arg1", "regalia").append("arg2", "calamity")));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowReuseOfQuotedPlaceholderWithinQuery() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByReusingPlaceholdersMultipleTimesWhenQuoted",
				String.class, String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(new Document().append("arg0", "calamity").append("arg1", "regalia").append("arg2", "calamity")));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowReuseOfQuotedPlaceholderWithinQueryAndIncludeSuffixCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod(
				"findByReusingPlaceholdersMultipleTimesWhenQuotedAndSomeStuffAppended", String.class, String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(),
				is(new Document().append("arg0", "calamity").append("arg1", "regalia").append("arg2", "calamitys")));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowQuotedParameterWithSuffixAppended() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByWhenQuotedAndSomeStuffAppended", String.class,
				String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new Document().append("arg0", "calamity").append("arg1", "regalias")));
	}

	@Test // DATAMONGO-1603
	public void shouldCaptureReplacementWithComplexSuffixCorrectly() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByMultiRegex", String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(), is(Document.parse(
				"{ \"$or\" : [ { \"firstname\" : { \"$regex\" : \".*calamity.*\" , \"$options\" : \"i\"}} , { \"lastname\" : { \"$regex\" : \".*calamityxyz.*\" , \"$options\" : \"i\"}}]}")));
	}

	@Test // DATAMONGO-1603
	public void shouldAllowPlaceholderReuseInQuotedValue() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameRegex", String.class, String.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, "calamity", "regalia");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);

		assertThat(query.getQueryObject(),
				is(Document.parse("{ 'lastname' : { '$regex' : '^(calamity|John regalia|regalia)'} }")));
	}

	@Test // DATAMONGO-1605
	public void findUsingSpelShouldRetainParameterType() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByUsingSpel", Object.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, 100.01D);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new Document("arg0", 100.01D)));
	}

	@Test // DATAMONGO-1605
	public void findUsingSpelShouldRetainNullValues() {

		StringBasedMongoQuery mongoQuery = createQueryForMethod("findByUsingSpel", Object.class);
		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, new Object[] { null });

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor);
		assertThat(query.getQueryObject(), is(new Document("arg0", null)));
	}

	private StringBasedMongoQuery createQueryForMethod(String name, Class<?>... parameters) {

		try {

			Method method = SampleRepository.class.getMethod(name, parameters);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(SampleRepository.class),
					factory, converter.getMappingContext());
			return new StringBasedMongoQuery(queryMethod, operations, PARSER, DefaultEvaluationContextProvider.INSTANCE);

		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
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
		Document findByParameterizedCriteriaAndFields(Document criteria, Map<String, Integer> fields);

		@Query("{'title': { $regex : '^?0', $options : 'i'}}")
		List<Document> findByTitleBeginsWithExplicitQuoting(String title);

		@Query("{$where: 'return this.date.getUTCMonth() == ?2 && this.date.getUTCDay() == ?3;'}")
		List<Document> findByQueryWithParametersInExpression(int param1, int param2, int param3, int param4);

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
		List<Person> findByWithBsonArgument(Document arg0);

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
