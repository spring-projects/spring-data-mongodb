/*
 * Copyright 2011-2018 the original author or authors.
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
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
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

/**
 * Unit test for {@link MongoQueryMethod}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoQueryMethodUnitTests {

	MongoMappingContext context;

	@Before
	public void setUp() {
		context = new MongoMappingContext();
	}

	@Test
	public void detectsCollectionFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");
		MongoEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType(), is(typeCompatibleWith(Address.class)));
		assertThat(metadata.getCollectionName(), is("contact"));
	}

	@Test
	public void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(SampleRepository2.class, "method");
		MongoEntityMetadata<?> entityInformation = queryMethod.getEntityInformation();

		assertThat(entityInformation.getJavaType(), is(typeCompatibleWith(Person.class)));
		assertThat(entityInformation.getCollectionName(), is("person"));
	}

	@Test
	public void discoversUserAsDomainTypeForGeoPageQueryMethod() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(PersonRepository.class, "findByLocationNear", Point.class,
				Distance.class, Pageable.class);
		assertThat(queryMethod.isGeoNearQuery(), is(true));
		assertThat(queryMethod.isPageQuery(), is(true));

		queryMethod = queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class);
		assertThat(queryMethod.isGeoNearQuery(), is(true));
		assertThat(queryMethod.isPageQuery(), is(false));
		assertThat(queryMethod.getEntityInformation().getJavaType(), is(typeCompatibleWith(User.class)));

		assertThat(queryMethod(PersonRepository.class, "findByEmailAddress", String.class, Point.class).isGeoNearQuery(),
				is(true));
		assertThat(queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class).isGeoNearQuery(),
				is(true));
		assertThat(queryMethod(PersonRepository.class, "findByLastname", String.class, Point.class).isGeoNearQuery(),
				is(true));
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsGeoPageQueryWithoutPageable() throws Exception {
		queryMethod(PersonRepository.class, "findByLocationNear", Point.class, Distance.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullMappingContext() throws Exception {

		Method method = PersonRepository.class.getMethod("findByFirstname", String.class, Point.class);

		new MongoQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
				new SpelAwareProxyProjectionFactory(), null);
	}

	@Test
	public void considersMethodReturningGeoPageAsPagingMethod() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findByLocationNear", Point.class, Distance.class,
				Pageable.class);

		assertThat(method.isPageQuery(), is(true));
		assertThat(method.isCollectionQuery(), is(false));
	}

	@Test
	public void createsMongoQueryMethodObjectForMethodReturningAnInterface() throws Exception {

		queryMethod(SampleRepository2.class, "methodReturningAnInterface");
	}

	@Test // DATAMONGO-957
	public void createsMongoQueryMethodWithEmptyMetaCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "emptyMetaAnnotation");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().hasValues(), is(false));
	}

	@Test // DATAMONGO-957
	public void createsMongoQueryMethodWithMaxExecutionTimeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithMaxExecutionTime");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().getMaxTimeMsec(), is(100L));
	}

	@Test // DATAMONGO-1403
	public void createsMongoQueryMethodWithSpellFixedMaxExecutionTimeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithSpellFixedMaxExecutionTime");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().getMaxTimeMsec(), is(100L));
	}

	@Test // DATAMONGO-957
	public void createsMongoQueryMethodWithMaxScanCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithMaxScan");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().getMaxScan(), is(10L));
	}

	@Test // DATAMONGO-957
	public void createsMongoQueryMethodWithCommentCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithComment");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().getComment(), is("foo bar"));
	}

	@Test // DATAMONGO-957
	public void createsMongoQueryMethodWithSnapshotCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithSnapshotUsage");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().getSnapshot(), is(true));
	}

	@Test // DATAMONGO-1480
	public void createsMongoQueryMethodWithNoCursorTimeoutCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithNoCursorTimeout");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().getFlags(),
				containsInAnyOrder(org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT));
	}

	@Test // DATAMONGO-1480
	public void createsMongoQueryMethodWithMultipleFlagsCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithMultipleFlags");

		assertThat(method.hasQueryMetaAttributes(), is(true));
		assertThat(method.getQueryMetaAttributes().getFlags(),
				containsInAnyOrder(org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT, org.springframework.data.mongodb.core.query.Meta.CursorOption.SLAVE_OK));
	}

	@Test // DATAMONGO-1266
	public void fallsBackToRepositoryDomainTypeIfMethodDoesNotReturnADomainType() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.getEntityInformation().getJavaType(), is(typeCompatibleWith(User.class)));
	}

	private MongoQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters) throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new MongoQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	interface PersonRepository extends Repository<User, Long> {

		// Misses Pageable
		GeoPage<User> findByLocationNear(Point point, Distance distance);

		GeoPage<User> findByLocationNear(Point point, Distance distance, Pageable pageable);

		GeoResult<User> findByEmailAddress(String lastname, Point location);

		GeoResults<User> findByFirstname(String firstname, Point location);

		Collection<GeoResult<User>> findByLastname(String lastname, Point location);

		@Meta
		List<User> emptyMetaAnnotation();

		@Meta(maxExecutionTimeMs = 100)
		List<User> metaWithMaxExecutionTime();

		@Meta(maxExecutionTimeMs = 100)
		List<User> metaWithSpellFixedMaxExecutionTime();

		@Meta(maxScanDocuments = 10)
		List<User> metaWithMaxScan();

		@Meta(comment = "foo bar")
		List<User> metaWithComment();

		@Meta(snapshot = true)
		List<User> metaWithSnapshotUsage();

		@Meta(flags = { org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT })
		List<User> metaWithNoCursorTimeout();

		@Meta(flags = { org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT, org.springframework.data.mongodb.core.query.Meta.CursorOption.SLAVE_OK })
		List<User> metaWithMultipleFlags();

		// DATAMONGO-1266
		void deleteByUserName(String userName);
	}

	interface SampleRepository extends Repository<Contact, Long> {

		List<Address> method();
	}

	interface SampleRepository2 extends Repository<Contact, Long> {

		List<Person> method();

		Customer methodReturningAnInterface();
	}

	interface Customer {

	}

}
