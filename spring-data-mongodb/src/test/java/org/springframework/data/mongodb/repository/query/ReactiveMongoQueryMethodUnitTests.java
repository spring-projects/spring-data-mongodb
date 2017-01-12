/*
 * Copyright 2016-2017 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.List;

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
import org.springframework.data.mongodb.repository.Contact;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

		assertThat(metadata.getJavaType(), is(typeCompatibleWith(Address.class)));
		assertThat(metadata.getCollectionName(), is("contact"));
	}

	@Test // DATAMONGO-1444
	public void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(SampleRepository2.class, "method");
		MongoEntityMetadata<?> entityInformation = queryMethod.getEntityInformation();

		assertThat(entityInformation.getJavaType(), is(typeCompatibleWith(Person.class)));
		assertThat(entityInformation.getCollectionName(), is("person"));
	}

	@Test // DATAMONGO-1444
	public void discoversUserAsDomainTypeForGeoPagingQueryMethod() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(PersonRepository.class, "findByLocationNear", Point.class,
				Distance.class, Pageable.class);
		assertThat(queryMethod.isGeoNearQuery(), is(false));
		assertThat(queryMethod.isPageQuery(), is(false));

		queryMethod = queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class);
		assertThat(queryMethod.isGeoNearQuery(), is(false));
		assertThat(queryMethod.isPageQuery(), is(false));
		assertThat(queryMethod.getEntityInformation().getJavaType(), is(typeCompatibleWith(User.class)));

		assertThat(queryMethod(PersonRepository.class, "findByEmailAddress", String.class, Point.class).isGeoNearQuery(),
				is(true));
		assertThat(queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class).isGeoNearQuery(),
				is(false));
		assertThat(queryMethod(PersonRepository.class, "findByLastname", String.class, Point.class).isGeoNearQuery(),
				is(true));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1444
	public void rejectsNullMappingContext() throws Exception {

		Method method = PersonRepository.class.getMethod("findByFirstname", String.class, Point.class);

		new MongoQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
				new SpelAwareProxyProjectionFactory(), null);
	}

	@Test(expected = IllegalStateException.class) // DATAMONGO-1444
	public void rejectsMonoPageableResult() throws Exception {
		queryMethod(PersonRepository.class, "findMonoByLastname", String.class, Pageable.class);
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodObjectForMethodReturningAnInterface() throws Exception {
		queryMethod(SampleRepository2.class, "methodReturningAnInterface");
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodWithEmptyMetaCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "emptyMetaAnnotation");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().hasValues(), is(false));
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodWithMaxExecutionTimeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithMaxExecutionTime");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().getMaxTimeMsec(), is(100L));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAMONGO-1444
	public void throwsExceptionOnWrappedPage() throws Exception {
		queryMethod(PersonRepository.class, "findMonoPageByLastname", String.class, Pageable.class);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAMONGO-1444
	public void throwsExceptionOnWrappedSlice() throws Exception {
		queryMethod(PersonRepository.class, "findMonoSliceByLastname", String.class, Pageable.class);
	}

	@Test // DATAMONGO-1444
	public void fallsBackToRepositoryDomainTypeIfMethodDoesNotReturnADomainType() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.getEntityInformation().getJavaType(), is(typeCompatibleWith(User.class)));
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
