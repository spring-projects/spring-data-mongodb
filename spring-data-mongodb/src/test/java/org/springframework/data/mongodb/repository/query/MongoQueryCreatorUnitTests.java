/*
 * Copyright 2011-2012 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.repository.query.StubParameterAccessor.*;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.Metrics;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.support.DefaultEntityInformationCreator;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Unit test for {@link MongoQueryCreator}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoQueryCreatorUnitTests {

	Method findByFirstname, findByFirstnameAndFriend, findByFirstnameNotNull;

	@Mock
	MongoConverter converter;

	MappingContext<?, MongoPersistentProperty> context;

	@Before
	public void setUp() throws SecurityException, NoSuchMethodException {

		context = new MongoMappingContext();

		doAnswer(new Answer<Object>() {
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return invocation.getArguments()[0];
			}
		}).when(converter).convertToMongoType(any());
	}

	@Test
	public void createsQueryCorrectly() throws Exception {

		PartTree tree = new PartTree("findByFirstName", Person.class);

		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Oliver"), context);

		creator.createQuery();

		creator = new MongoQueryCreator(new PartTree("findByFirstNameAndFriend", Person.class), getAccessor(converter,
				"Oliver", new Person()), context);
		creator.createQuery();
	}

	@Test
	public void createsNotNullQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameNotNull", Person.class);
		Query query = new MongoQueryCreator(tree, getAccessor(converter), context).createQuery();

		assertThat(query.getQueryObject(), is(new Query(Criteria.where("firstName").ne(null)).getQueryObject()));
	}

	@Test
	public void createsIsNullQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameIsNull", Person.class);
		Query query = new MongoQueryCreator(tree, getAccessor(converter), context).createQuery();

		assertThat(query.getQueryObject(), is(new Query(Criteria.where("firstName").is(null)).getQueryObject()));
	}

	@Test
	public void bindsMetricDistanceParameterToNearSphereCorrectly() throws Exception {

		Point point = new Point(10, 20);
		Distance distance = new Distance(2.5, Metrics.KILOMETERS);

		Query query = query(where("location").nearSphere(point).maxDistance(distance.getNormalizedValue()).and("firstname")
				.is("Dave"));
		assertBindsDistanceToQuery(point, distance, query);
	}

	@Test
	public void bindsDistanceParameterToNearCorrectly() throws Exception {

		Point point = new Point(10, 20);
		Distance distance = new Distance(2.5);

		Query query = query(where("location").near(point).maxDistance(distance.getNormalizedValue()).and("firstname")
				.is("Dave"));
		assertBindsDistanceToQuery(point, distance, query);
	}

	@Test
	public void createsLessThanEqualQueryCorrectly() throws Exception {

		PartTree tree = new PartTree("findByAgeLessThanEqual", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, 18), context);

		Query reference = query(where("age").lte(18));
		assertThat(creator.createQuery().getQueryObject(), is(reference.getQueryObject()));
	}

	@Test
	public void createsGreaterThanEqualQueryCorrectly() throws Exception {

		PartTree tree = new PartTree("findByAgeGreaterThanEqual", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, 18), context);

		Query reference = query(where("age").gte(18));
		assertThat(creator.createQuery().getQueryObject(), is(reference.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-291
	 */
	@Test
	public void honoursMappingInformationForPropertyPaths() {

		PartTree partTree = new PartTree("findByUsername", User.class);

		MongoQueryCreator creator = new MongoQueryCreator(partTree, getAccessor(converter, "Oliver"), context);
		Query reference = query(where("foo").is("Oliver"));
		assertThat(creator.createQuery().getQueryObject(), is(reference.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-338
	 */
	@Test
	public void createsExistsClauseCorrectly() {

		PartTree tree = new PartTree("findByAgeExists", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, true), context);
		Query query = query(where("age").exists(true));
		assertThat(creator.createQuery().getQueryObject(), is(query.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-338
	 */
	@Test
	public void createsRegexClauseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameRegex", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, ".*"), context);
		Query query = query(where("firstName").regex(".*"));
		assertThat(creator.createQuery().getQueryObject(), is(query.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-338
	 */
	@Test
	public void createsTrueClauseCorrectly() {

		PartTree tree = new PartTree("findByActiveTrue", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter), context);
		Query query = query(where("active").is(true));
		assertThat(creator.createQuery().getQueryObject(), is(query.getQueryObject()));
	}

	/**
	 * @see DATAMONGO-338
	 */
	@Test
	public void createsFalseClauseCorrectly() {

		PartTree tree = new PartTree("findByActiveFalse", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter), context);
		Query query = query(where("active").is(false));
		assertThat(creator.createQuery().getQueryObject(), is(query.getQueryObject()));
	}

	/**
	 * @see DATAMONGO
	 */
	@Test
	public void createsOrQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameOrAge", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Dave", 42), context);

		Query query = creator.createQuery();
		assertThat(query.getQueryObject(),
				is(query(new Criteria().orOperator(where("firstName").is("Dave"), where("age").is(42))).getQueryObject()));
	}

	private void assertBindsDistanceToQuery(Point point, Distance distance, Query reference) throws Exception {

		when(converter.convertToMongoType("Dave")).thenReturn("Dave");

		PartTree tree = new PartTree("findByLocationNearAndFirstname",
				org.springframework.data.mongodb.repository.Person.class);
		Method method = PersonRepository.class.getMethod("findByLocationNearAndFirstname", Point.class, Distance.class,
				String.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
				new DefaultEntityInformationCreator(new MongoMappingContext()));
		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod, new Object[] { point, distance,
				"Dave" });

		Query query = new MongoQueryCreator(tree, new ConvertingParameterAccessor(converter, accessor), context)
				.createQuery();
		assertThat(query.getQueryObject(), is(query.getQueryObject()));
	}

	interface PersonRepository extends Repository<Person, Long> {

		List<Person> findByLocationNearAndFirstname(Point location, Distance maxDistance, String firstname);
	}

	class User {

		@Field("foo")
		String username;
	}
}
