/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.User;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.GeoPage;
import org.springframework.data.mongodb.core.geo.GeoResult;
import org.springframework.data.mongodb.core.geo.GeoResults;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Contact;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.support.DefaultEntityInformationCreator;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit test for {@link MongoQueryMethod}.
 * 
 * @author Oliver Gierke
 */
public class MongoQueryMethodUnitTests {

	EntityInformationCreator creator;

	@Before
	public void setUp() {
		MongoMappingContext context = new MongoMappingContext();
		creator = new DefaultEntityInformationCreator(context);
	}

	@Test
	public void detectsCollectionFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		Method method = SampleRepository.class.getMethod("method");

		MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(SampleRepository.class),
				creator);
		MongoEntityInformation<?, ?> entityInformation = queryMethod.getEntityInformation();

		assertThat(entityInformation.getJavaType(), is(typeCompatibleWith(Address.class)));
		assertThat(entityInformation.getCollectionName(), is("contact"));
	}

	@Test
	public void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {

		Method method = SampleRepository2.class.getMethod("method");

		MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(SampleRepository.class),
				creator);
		MongoEntityInformation<?, ?> entityInformation = queryMethod.getEntityInformation();

		assertThat(entityInformation.getJavaType(), is(typeCompatibleWith(Person.class)));
		assertThat(entityInformation.getCollectionName(), is("person"));
	}

	@Test
	public void discoversUserAsDomainTypeForGeoPageQueryMethod() throws Exception {

		MongoQueryMethod queryMethod = queryMethod("findByLocationNear", Point.class, Distance.class, Pageable.class);
		assertThat(queryMethod.isGeoNearQuery(), is(true));
		assertThat(queryMethod.isPageQuery(), is(true));

		queryMethod = queryMethod("findByFirstname", String.class, Point.class);
		assertThat(queryMethod.isGeoNearQuery(), is(true));
		assertThat(queryMethod.isPageQuery(), is(false));
		assertThat(queryMethod.getEntityInformation().getJavaType(), is(typeCompatibleWith(User.class)));

		assertThat(queryMethod("findByEmailAddress", String.class, Point.class).isGeoNearQuery(), is(true));
		assertThat(queryMethod("findByFirstname", String.class, Point.class).isGeoNearQuery(), is(true));
		assertThat(queryMethod("findByLastname", String.class, Point.class).isGeoNearQuery(), is(true));
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsGeoPageQueryWithoutPageable() throws Exception {
		queryMethod("findByLocationNear", Point.class, Distance.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullEntityCreator() throws Exception {
		Method method = PersonRepository.class.getMethod("findByFirstname", String.class, Point.class);
		new MongoQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class), null);
	}

	@Test
	public void considersMethodReturningGeoPageAsPagingMethod() throws Exception {
		MongoQueryMethod method = queryMethod("findByLocationNear", Point.class, Distance.class, Pageable.class);
		assertThat(method.isPageQuery(), is(true));
		assertThat(method.isCollectionQuery(), is(false));
	}

	private MongoQueryMethod queryMethod(String name, Class<?>... parameters) throws Exception {
		Method method = PersonRepository.class.getMethod(name, parameters);
		return new MongoQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class), creator);
	}

	interface PersonRepository extends Repository<User, Long> {

		// Misses Pageable
		GeoPage<User> findByLocationNear(Point point, Distance distance);

		GeoPage<User> findByLocationNear(Point point, Distance distance, Pageable pageable);

		GeoResult<User> findByEmailAddress(String lastname, Point location);

		GeoResults<User> findByFirstname(String firstname, Point location);

		Collection<GeoResult<User>> findByLastname(String lastname, Point location);
	}

	interface SampleRepository extends Repository<Contact, Long> {

		List<Address> method();
	}

	interface SampleRepository2 extends Repository<Contact, Long> {

		List<Person> method();
	}
}
