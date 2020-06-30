/*
 * Copyright 2016-2020 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.core.ReactiveFindOperation.ReactiveFind;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
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
import org.springframework.data.repository.query.ReactiveExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.data.spel.spi.ReactiveEvaluationContextExtension;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Base64Utils;

/**
 * Unit tests for {@link ReactiveStringBasedMongoQuery}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
public class ReactiveStringBasedMongoQueryUnitTests {

	SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock ReactiveMongoOperations operations;
	@Mock DbRefResolver factory;
	@Mock ReactiveFind reactiveFind;

	MongoConverter converter;

	@BeforeEach
	public void setUp() {

		when(operations.query(any())).thenReturn(reactiveFind);

		this.converter = new MappingMongoConverter(factory, new MongoMappingContext());
	}

	@Test // DATAMONGO-1444
	public void bindsSimplePropertyCorrectly() throws Exception {

		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastname", String.class);
		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, "Matthews");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor).block();
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject()).isEqualTo(reference.getQueryObject());
	}

	@Test // DATAMONGO-1444
	public void bindsComplexPropertyCorrectly() throws Exception {

		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("findByAddress", Address.class);

		Address address = new Address("Foo", "0123", "Bar");
		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, address);

		Document dbObject = new Document();
		converter.write(address, dbObject);
		dbObject.remove(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor).block();
		Document queryObject = new Document("address", dbObject);
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(queryObject);

		assertThat(query.getQueryObject().toJson()).isEqualTo(reference.getQueryObject().toJson());
	}

	@Test // DATAMONGO-1444
	public void constructsDeleteQueryCorrectly() throws Exception {

		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("removeByLastname", String.class);
		assertThat(mongoQuery.isDeleteQuery()).isTrue();
	}

	@Test // DATAMONGO-1444
	public void preventsDeleteAndCountFlagAtTheSameTime() {
		assertThatIllegalArgumentException().isThrownBy(() -> createQueryForMethod("invalidMethod", String.class));
	}

	@Test // DATAMONGO-2030
	public void shouldSupportExistsProjection() throws Exception {

		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("existsByLastname", String.class);

		assertThat(mongoQuery.isExistsQuery()).isTrue();
	}

	@Test // DATAMONGO-1444
	public void shouldSupportFindByParameterizedCriteriaAndFields() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter,
				new Document("firstname", "first").append("lastname", "last"), Collections.singletonMap("lastname", 1));
		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("findByParameterizedCriteriaAndFields",
				Document.class, Map.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor).block();

		assertThat(query.getQueryObject())
				.isEqualTo(new BasicQuery("{ \"firstname\": \"first\", \"lastname\": \"last\"}").getQueryObject());
		assertThat(query.getFieldsObject()).isEqualTo(new BasicQuery(null, "{ \"lastname\": 1}").getFieldsObject());
	}

	@Test // DATAMONGO-1444
	public void shouldParseQueryWithParametersInExpression() throws Exception {

		ConvertingParameterAccessor accessor = StubParameterAccessor.getAccessor(converter, 1, 2, 3, 4);
		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithParametersInExpression", int.class,
				int.class, int.class, int.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accessor).block();

		assertThat(query.getQueryObject())
				.isEqualTo(new BasicQuery("{$where: 'return this.date.getUTCMonth() == 3 && this.date.getUTCDay() == 4;'}")
						.getQueryObject());
	}

	@Test // DATAMONGO-1444
	public void shouldParseJsonKeyReplacementCorrectly() throws Exception {

		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("methodWithPlaceholderInKeyOfJsonStructure",
				String.class, String.class);
		ConvertingParameterAccessor parameterAccessor = StubParameterAccessor.getAccessor(converter, "key", "value");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(parameterAccessor).block();

		assertThat(query.getQueryObject()).isEqualTo(new Document().append("key", "value"));
	}

	@Test // DATAMONGO-1444
	public void shouldSupportExpressionsInCustomQueries() throws Exception {

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, "Matthews");
		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpression", String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor).block();
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{'lastname' : 'Matthews'}");

		assertThat(query.getQueryObject()).isEqualTo(reference.getQueryObject());
	}

	@Test // DATAMONGO-1444
	public void shouldSupportExpressionsInCustomQueriesWithNestedObject() throws Exception {

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, true, "param1", "param2");
		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpressionAndNestedObject",
				boolean.class, String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor).block();
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{ \"id\" : { \"$exists\" : true}}");

		assertThat(query.getQueryObject()).isEqualTo(reference.getQueryObject());
	}

	@Test // DATAMONGO-1444
	public void shouldSupportExpressionsInCustomQueriesWithMultipleNestedObjects() throws Exception {

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, true, "param1", "param2");
		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("findByQueryWithExpressionAndMultipleNestedObjects",
				boolean.class, String.class, String.class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor).block();
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(
				"{ \"id\" : { \"$exists\" : true} , \"foo\" : 42 , \"bar\" : { \"$exists\" : false}}");

		assertThat(query.getQueryObject()).isEqualTo(reference.getQueryObject());
	}

	@Test // DATAMONGO-1444
	public void shouldSupportNonQuotedBinaryDataReplacement() throws Exception {

		byte[] binaryData = "Matthews".getBytes("UTF-8");
		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter, binaryData);
		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod("findByLastnameAsBinary", byte[].class);

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor).block();
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery(
				"{'lastname' : { '$binary' : '" + Base64Utils.encodeToString(binaryData) + "', '$type' : '" + 0 + "'}}");

		assertThat(query.getQueryObject().toJson()).isEqualTo(reference.getQueryObject().toJson());
	}

	@Test // DATAMONGO-1894
	void shouldConsiderReactiveSpelExtension() throws Exception {

		ReactiveExtensionAwareQueryMethodEvaluationContextProvider contextProvider = new ReactiveExtensionAwareQueryMethodEvaluationContextProvider(
				Collections.singletonList(ReactiveSpelExtension.INSTANCE));

		ConvertingParameterAccessor accesor = StubParameterAccessor.getAccessor(converter);
		ReactiveStringBasedMongoQuery mongoQuery = createQueryForMethod(contextProvider, "withReactiveSpelExtensions");

		org.springframework.data.mongodb.core.query.Query query = mongoQuery.createQuery(accesor).block();
		org.springframework.data.mongodb.core.query.Query reference = new BasicQuery("{lastname: true}", "{project: true}");

		assertThat(query.getQueryObject().toJson()).isEqualTo(reference.getQueryObject().toJson());
	}

	private ReactiveStringBasedMongoQuery createQueryForMethod(String name, Class<?>... parameters) throws Exception {
		return createQueryForMethod(ReactiveQueryMethodEvaluationContextProvider.DEFAULT, name, parameters);
	}

	private ReactiveStringBasedMongoQuery createQueryForMethod(
			ReactiveQueryMethodEvaluationContextProvider contextProvider, String name, Class<?>... parameters)
			throws Exception {

		Method method = SampleRepository.class.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		ReactiveMongoQueryMethod queryMethod = new ReactiveMongoQueryMethod(method,
				new DefaultRepositoryMetadata(SampleRepository.class), factory, converter.getMappingContext());

		return new ReactiveStringBasedMongoQuery(queryMethod, operations, PARSER,
				contextProvider);
	}

	private interface SampleRepository extends Repository<Person, Long> {

		@Query("{ 'lastname' : ?0 }")
		Mono<Person> findByLastname(String lastname);

		@Query("{ 'lastname' : ?0 }")
		Mono<Person> findByLastnameAsBinary(byte[] lastname);

		@Query("{ 'address' : ?0 }")
		Mono<Person> findByAddress(Address address);

		@Query(value = "{ 'lastname' : ?0 }", delete = true)
		Mono<Void> removeByLastname(String lastname);

		@Query(value = "{ 'lastname' : ?0 }", delete = true, count = true)
		Mono<Void> invalidMethod(String lastname);

		@Query(value = "?0", fields = "?1")
		Mono<Document> findByParameterizedCriteriaAndFields(Document criteria, Map<String, Integer> fields);

		@Query("{$where: 'return this.date.getUTCMonth() == ?2 && this.date.getUTCDay() == ?3;'}")
		Flux<Document> findByQueryWithParametersInExpression(int param1, int param2, int param3, int param4);

		@Query("{ ?0 : ?1}")
		Mono<Object> methodWithPlaceholderInKeyOfJsonStructure(String keyReplacement, String valueReplacement);

		@Query("{'lastname': ?#{[0]} }")
		Flux<Person> findByQueryWithExpression(String param0);

		@Query("{'id':?#{ [0] ? { $exists :true} : [1] }}")
		Flux<Person> findByQueryWithExpressionAndNestedObject(boolean param0, String param1);

		@Query("{'id':?#{ [0] ? { $exists :true} : [1] }, 'foo':42, 'bar': ?#{ [0] ? { $exists :false} : [1] }}")
		Flux<Person> findByQueryWithExpressionAndMultipleNestedObjects(boolean param0, String param1, String param2);

		@Query(value = "{ 'lastname' : ?0 }", exists = true)
		Mono<Boolean> existsByLastname(String lastname);

		@Query(value = "{ 'lastname' : ?#{hasRole()} }", fields = "{project: ?#{hasRole()}}")
		Mono<Document> withReactiveSpelExtensions();
	}

	public enum ReactiveSpelExtension implements ReactiveEvaluationContextExtension {

		INSTANCE;

		@Override
		public Mono<? extends EvaluationContextExtension> getExtension() {
			return Mono.just(SpelExtension.INSTANCE);
		}

		@Override
		public String getExtensionId() {
			return "sample";
		}
	}

	public enum SpelExtension implements EvaluationContextExtension {

		INSTANCE;

		@Override
		public Object getRootObject() {
			return this;
		}

		@Override
		public String getExtensionId() {
			return "sample";
		}

		public boolean hasRole() {
			return true;
		}
	}
}
