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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.User;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.Contact;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit test for {@link ReactiveMongoQueryMethod}.
 *
 * @author Mark Paluch
 */
public class ReactiveMongoQueryMethodUnitTests {

	MongoMappingContext context;

	@Before
	public void setUp() {
		context = new MongoMappingContext();
	}

	@Test // DATAMONGO-1444
	public void detectsCollectionFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		ReactiveMongoQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");
		MongoEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType()).isAssignableFrom(Address.class);
		assertThat(metadata.getCollectionName()).isEqualTo("contact");
	}

	@Test // DATAMONGO-1444
	public void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(SampleRepository2.class, "method");
		MongoEntityMetadata<?> entityInformation = queryMethod.getEntityInformation();

		assertThat(entityInformation.getJavaType()).isAssignableFrom(Person.class);
		assertThat(entityInformation.getCollectionName()).isEqualTo("person");
	}

	@Test // DATAMONGO-1444
	public void discoversUserAsDomainTypeForGeoPagingQueryMethod() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(PersonRepository.class, "findByLocationNear", Point.class,
				Distance.class, Pageable.class);
		assertThat(queryMethod.isGeoNearQuery()).isFalse();
		assertThat(queryMethod.isPageQuery()).isFalse();

		queryMethod = queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class);
		assertThat(queryMethod.isGeoNearQuery()).isFalse();
		assertThat(queryMethod.isPageQuery()).isFalse();
		assertThat(queryMethod.getEntityInformation().getJavaType()).isAssignableFrom(User.class);

		assertThat(queryMethod(PersonRepository.class, "findByEmailAddress", String.class, Point.class).isGeoNearQuery())
				.isTrue();
		assertThat(queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class).isGeoNearQuery())
				.isFalse();
		assertThat(queryMethod(PersonRepository.class, "findByLastname", String.class, Point.class).isGeoNearQuery())
				.isTrue();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1444
	public void rejectsNullMappingContext() throws Exception {

		Method method = PersonRepository.class.getMethod("findByFirstname", String.class, Point.class);

		new MongoQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
				new SpelAwareProxyProjectionFactory(), null);
	}

	@Test // DATAMONGO-1444
	public void rejectsMonoPageableResult() {
		assertThatIllegalStateException()
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoByLastname", String.class, Pageable.class));
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodObjectForMethodReturningAnInterface() throws Exception {
		queryMethod(SampleRepository2.class, "methodReturningAnInterface");
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodWithEmptyMetaCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "emptyMetaAnnotation");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().hasValues()).isFalse();
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodWithMaxExecutionTimeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithMaxExecutionTime");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getMaxTimeMsec()).isEqualTo(100L);
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionOnWrappedPage() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoPageByLastname", String.class, Pageable.class));
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionOnWrappedSlice() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoSliceByLastname", String.class, Pageable.class));
	}

	@Test // DATAMONGO-1444
	public void fallsBackToRepositoryDomainTypeIfMethodDoesNotReturnADomainType() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.getEntityInformation().getJavaType()).isAssignableFrom(User.class);
	}

	@Test // DATAMONGO-2153
	public void findsAnnotatedAggregation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "findByAggregation");

		Assertions.assertThat(method.hasAnnotatedAggregation()).isTrue();
		Assertions.assertThat(method.getAnnotatedAggregation()).hasSize(1);
	}

	@Test // DATAMONGO-2153
	public void detectsCollationForAggregation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "findByAggregationWithCollation");

		Assertions.assertThat(method.hasAnnotatedCollation()).isTrue();
		Assertions.assertThat(method.getAnnotatedCollation()).isEqualTo("de_AT");
	}

	private ReactiveMongoQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters)
			throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new ReactiveMongoQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	interface PersonRepository extends Repository<User, Long> {

		Mono<Person> findMonoByLastname(String lastname, Pageable pageRequest);

		Mono<Page<Person>> findMonoPageByLastname(String lastname, Pageable pageRequest);

		Mono<Slice<Person>> findMonoSliceByLastname(String lastname, Pageable pageRequest);

		// Misses Pageable
		Flux<User> findByLocationNear(Point point, Distance distance);

		Flux<User> findByLocationNear(Point point, Distance distance, Pageable pageable);

		Mono<GeoResult<User>> findByEmailAddress(String lastname, Point location);

		Flux<User> findByFirstname(String firstname, Point location);

		Flux<GeoResult<User>> findByLastname(String lastname, Point location);

		@Meta
		Flux<User> emptyMetaAnnotation();

		@Meta(maxExecutionTimeMs = 100)
		Flux<User> metaWithMaxExecutionTime();

		void deleteByUserName(String userName);

		@Aggregation("{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }")
		Flux<User> findByAggregation();

		@Aggregation(pipeline = "{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }",
				collation = "de_AT")
		Flux<User> findByAggregationWithCollation();
	}

	interface SampleRepository extends Repository<Contact, Long> {

		List<Address> method();
	}

	interface SampleRepository2 extends Repository<Contact, Long> {

		List<Person> method();

		Customer methodReturningAnInterface();
	}

	interface Customer {}
}
