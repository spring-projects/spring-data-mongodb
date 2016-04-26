/*
 * Copyright 2011-2016 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.repository.query.StubParameterAccessor.*;

import java.lang.reflect.Method;
import java.util.List;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.Venue;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.parser.PartTree;

import com.mongodb.DBObject;

/**
 * Unit test for {@link MongoQueryCreator}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class MongoQueryCreatorUnitTests {

	Method findByFirstname, findByFirstnameAndFriend, findByFirstnameNotNull;

	MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context;
	MongoConverter converter;

	@Rule public ExpectedException expection = ExpectedException.none();

	@Before
	public void setUp() throws SecurityException, NoSuchMethodException {

		context = new MongoMappingContext();

		DbRefResolver resolver = new DefaultDbRefResolver(mock(MongoDbFactory.class));
		converter = new MappingMongoConverter(resolver, context);
	}

	@Test
	public void createsQueryCorrectly() throws Exception {

		PartTree tree = new PartTree("findByFirstName", Person.class);

		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Oliver"), context);
		Query query = creator.createQuery();
		assertThat(query, is(query(where("firstName").is("Oliver"))));
	}

	/**
	 * @see DATAMONGO-469
	 */
	@Test
	public void createsAndQueryCorrectly() {

		Person person = new Person();
		MongoQueryCreator creator = new MongoQueryCreator(new PartTree("findByFirstNameAndFriend", Person.class),
				getAccessor(converter, "Oliver", person), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("firstName").is("Oliver").and("friend").is(person))));
	}

	@Test
	public void createsNotNullQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameNotNull", Person.class);
		Query query = new MongoQueryCreator(tree, getAccessor(converter), context).createQuery();

		assertThat(query, is(new Query(Criteria.where("firstName").ne(null))));
	}

	@Test
	public void createsIsNullQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameIsNull", Person.class);
		Query query = new MongoQueryCreator(tree, getAccessor(converter), context).createQuery();

		assertThat(query, is(new Query(Criteria.where("firstName").is(null))));
	}

	@Test
	public void bindsMetricDistanceParameterToNearSphereCorrectly() throws Exception {

		Point point = new Point(10, 20);
		Distance distance = new Distance(2.5, Metrics.KILOMETERS);

		Query query = query(
				where("location").nearSphere(point).maxDistance(distance.getNormalizedValue()).and("firstname").is("Dave"));
		assertBindsDistanceToQuery(point, distance, query);
	}

	@Test
	public void bindsDistanceParameterToNearCorrectly() throws Exception {

		Point point = new Point(10, 20);
		Distance distance = new Distance(2.5);

		Query query = query(
				where("location").near(point).maxDistance(distance.getNormalizedValue()).and("firstname").is("Dave"));
		assertBindsDistanceToQuery(point, distance, query);
	}

	@Test
	public void createsLessThanEqualQueryCorrectly() throws Exception {

		PartTree tree = new PartTree("findByAgeLessThanEqual", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, 18), context);

		Query reference = query(where("age").lte(18));
		assertThat(creator.createQuery(), is(reference));
	}

	@Test
	public void createsGreaterThanEqualQueryCorrectly() throws Exception {

		PartTree tree = new PartTree("findByAgeGreaterThanEqual", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, 18), context);

		Query reference = query(where("age").gte(18));
		assertThat(creator.createQuery(), is(reference));
	}

	/**
	 * @see DATAMONGO-338
	 */
	@Test
	public void createsExistsClauseCorrectly() {

		PartTree tree = new PartTree("findByAgeExists", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, true), context);
		Query query = query(where("age").exists(true));
		assertThat(creator.createQuery(), is(query));
	}

	/**
	 * @see DATAMONGO-338
	 */
	@Test
	public void createsRegexClauseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameRegex", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, ".*"), context);
		Query query = query(where("firstName").regex(".*"));
		assertThat(creator.createQuery(), is(query));
	}

	/**
	 * @see DATAMONGO-338
	 */
	@Test
	public void createsTrueClauseCorrectly() {

		PartTree tree = new PartTree("findByActiveTrue", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter), context);
		Query query = query(where("active").is(true));
		assertThat(creator.createQuery(), is(query));
	}

	/**
	 * @see DATAMONGO-338
	 */
	@Test
	public void createsFalseClauseCorrectly() {

		PartTree tree = new PartTree("findByActiveFalse", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter), context);
		Query query = query(where("active").is(false));
		assertThat(creator.createQuery(), is(query));
	}

	/**
	 * @see DATAMONGO-413
	 */
	@Test
	public void createsOrQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameOrAge", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Dave", 42), context);

		Query query = creator.createQuery();
		assertThat(query, is(query(new Criteria().orOperator(where("firstName").is("Dave"), where("age").is(42)))));
	}

	/**
	 * @see DATAMONGO-347
	 */
	@Test
	public void createsQueryReferencingADBRefCorrectly() {

		User user = new User();
		user.id = new ObjectId();

		PartTree tree = new PartTree("findByCreator", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, user), context);
		DBObject queryObject = creator.createQuery().getQueryObject();

		assertThat(queryObject.get("creator"), is((Object) user));
	}

	/**
	 * @see DATAMONGO-418
	 */
	@Test
	public void createsQueryWithStartingWithPredicateCorrectly() {

		PartTree tree = new PartTree("findByUsernameStartingWith", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Matt"), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("username").regex("^Matt"))));
	}

	/**
	 * @see DATAMONGO-418
	 */
	@Test
	public void createsQueryWithEndingWithPredicateCorrectly() {

		PartTree tree = new PartTree("findByUsernameEndingWith", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "ews"), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("username").regex("ews$"))));
	}

	/**
	 * @see DATAMONGO-418
	 */
	@Test
	public void createsQueryWithContainingPredicateCorrectly() {

		PartTree tree = new PartTree("findByUsernameContaining", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "thew"), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("username").regex(".*thew.*"))));
	}

	private void assertBindsDistanceToQuery(Point point, Distance distance, Query reference) throws Exception {

		PartTree tree = new PartTree("findByLocationNearAndFirstname",
				org.springframework.data.mongodb.repository.Person.class);
		Method method = PersonRepository.class.getMethod("findByLocationNearAndFirstname", Point.class, Distance.class,
				String.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
				new SpelAwareProxyProjectionFactory(), new MongoMappingContext());
		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { point, distance, "Dave" });

		Query query = new MongoQueryCreator(tree, new ConvertingParameterAccessor(converter, accessor), context)
				.createQuery();
		assertThat(query, is(query));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void createsQueryWithFindByIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByfirstNameIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query, is(query(where("firstName").regex("^dave$", "i"))));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void createsQueryWithFindByNotIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameNotIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query.toString(), is(query(where("firstName").not().regex("^dave$", "i")).toString()));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void createsQueryWithFindByStartingWithIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameStartingWithIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query, is(query(where("firstName").regex("^dave", "i"))));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void createsQueryWithFindByEndingWithIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameEndingWithIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query, is(query(where("firstName").regex("dave$", "i"))));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void createsQueryWithFindByContainingIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameContainingIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query, is(query(where("firstName").regex(".*dave.*", "i"))));
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void shouldThrowExceptionForQueryWithFindByIgnoreCaseOnNonStringProperty() {

		expection.expect(IllegalArgumentException.class);
		expection.expectMessage("must be of type String");

		PartTree tree = new PartTree("findByFirstNameAndAgeIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "foo", 42), context);

		creator.createQuery();
	}

	/**
	 * @see DATAMONGO-770
	 */
	@Test
	public void shouldOnlyGenerateLikeExpressionsForStringPropertiesIfAllIgnoreCase() {

		PartTree tree = new PartTree("findByFirstNameAndAgeAllIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave", 42), context);

		Query query = creator.createQuery();
		assertThat(query, is(query(where("firstName").regex("^dave$", "i").and("age").is(42))));
	}

	/**
	 * @see DATAMONGO-566
	 */
	@Test
	public void shouldCreateDeleteByQueryCorrectly() {

		PartTree tree = new PartTree("deleteByFirstName", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave", 42), context);

		Query query = creator.createQuery();

		assertThat(tree.isDelete(), is(true));
		assertThat(query, is(query(where("firstName").is("dave"))));
	}

	/**
	 * @see DATAMONGO-566
	 */
	@Test
	public void shouldCreateDeleteByQueryCorrectlyForMultipleCriteriaAndCaseExpressions() {

		PartTree tree = new PartTree("deleteByFirstNameAndAgeAllIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave", 42), context);

		Query query = creator.createQuery();

		assertThat(tree.isDelete(), is(true));
		assertThat(query, is(query(where("firstName").regex("^dave$", "i").and("age").is(42))));
	}

	/**
	 * @see DATAMONGO-1075
	 */
	@Test
	public void shouldCreateInClauseWhenUsingContainsOnCollectionLikeProperty() {

		PartTree tree = new PartTree("findByEmailAddressesContaining", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();

		assertThat(query, is(query(where("emailAddresses").in("dave"))));
	}

	/**
	 * @see DATAMONGO-1075
	 */
	@Test
	public void shouldCreateInClauseWhenUsingNotContainsOnCollectionLikeProperty() {

		PartTree tree = new PartTree("findByEmailAddressesNotContaining", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();

		assertThat(query, is(query(where("emailAddresses").not().in("dave"))));
	}

	/**
	 * @see DATAMONGO-1075
	 * @see DATAMONGO-1425
	 */
	@Test
	public void shouldCreateRegexWhenUsingNotContainsOnStringProperty() {

		PartTree tree = new PartTree("findByUsernameNotContaining", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "thew"), context);
		Query query = creator.createQuery();

		assertThat(query.getQueryObject(), is(query(where("username").not().regex(".*thew.*")).getQueryObject()));
	}

	/**
	 * @see DATAMONGO-1139
	 */
	@Test
	public void createsNonShericalNearForDistanceWithDefaultMetric() {

		Point point = new Point(1.0, 1.0);
		Distance distance = new Distance(1.0);

		PartTree tree = new PartTree("findByLocationNear", Venue.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, point, distance), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("location").near(point).maxDistance(1.0))));
	}

	/**
	 * @see DATAMONGO-1136
	 */
	@Test
	public void shouldCreateWithinQueryCorrectly() {

		Point first = new Point(1, 1);
		Point second = new Point(2, 2);
		Point third = new Point(3, 3);
		Shape shape = new Polygon(first, second, third);

		PartTree tree = new PartTree("findByAddress_GeoWithin", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, shape), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("address.geo").within(shape))));
	}

	/**
	 * @see DATAMONGO-1110
	 */
	@Test
	public void shouldCreateNearSphereQueryForSphericalProperty() {

		Point point = new Point(10, 20);

		PartTree tree = new PartTree("findByAddress2dSphere_GeoNear", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, point), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("address2dSphere.geo").nearSphere(point))));
	}

	/**
	 * @see DATAMONGO-1110
	 */
	@Test
	public void shouldCreateNearSphereQueryForSphericalPropertyHavingDistanceWithDefaultMetric() {

		Point point = new Point(1.0, 1.0);
		Distance distance = new Distance(1.0);

		PartTree tree = new PartTree("findByAddress2dSphere_GeoNear", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, point, distance), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("address2dSphere.geo").nearSphere(point).maxDistance(1.0))));
	}

	/**
	 * @see DATAMONGO-1110
	 */
	@Test
	public void shouldCreateNearQueryForMinMaxDistance() {

		Point point = new Point(10, 20);
		Range<Distance> range = Distance.between(new Distance(10), new Distance(20));

		PartTree tree = new PartTree("findByAddress_GeoNear", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, point, range), context);
		Query query = creator.createQuery();

		assertThat(query, is(query(where("address.geo").near(point).minDistance(10D).maxDistance(20D))));
	}

	/**
	 * @see DATAMONGO-1229
	 */
	@Test
	public void appliesIgnoreCaseToLeafProperty() {

		PartTree tree = new PartTree("findByAddressStreetIgnoreCase", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "Street");

		assertThat(new MongoQueryCreator(tree, accessor, context).createQuery(), is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-1232
	 */
	@Test
	public void ignoreCaseShouldEscapeSource() {

		PartTree tree = new PartTree("findByUsernameIgnoreCase", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "con.flux+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query, is(query(where("username").regex("^\\Qcon.flux+\\E$", "i"))));
	}

	/**
	 * @see DATAMONGO-1232
	 */
	@Test
	public void ignoreCaseShouldEscapeSourceWhenUsedForStartingWith() {

		PartTree tree = new PartTree("findByUsernameStartingWithIgnoreCase", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "dawns.light+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query, is(query(where("username").regex("^\\Qdawns.light+\\E", "i"))));
	}

	/**
	 * @see DATAMONGO-1232
	 */
	@Test
	public void ignoreCaseShouldEscapeSourceWhenUsedForEndingWith() {

		PartTree tree = new PartTree("findByUsernameEndingWithIgnoreCase", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "new.ton+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query, is(query(where("username").regex("\\Qnew.ton+\\E$", "i"))));
	}

	/**
	 * @see DATAMONGO-1232
	 */
	@Test
	public void likeShouldEscapeSourceWhenUsedWithLeadingAndTrailingWildcard() {

		PartTree tree = new PartTree("findByUsernameLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*fire.fight+*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query, is(query(where("username").regex(".*\\Qfire.fight+\\E.*"))));
	}

	/**
	 * @see DATAMONGO-1232
	 */
	@Test
	public void likeShouldEscapeSourceWhenUsedWithLeadingWildcard() {

		PartTree tree = new PartTree("findByUsernameLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*steel.heart+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query, is(query(where("username").regex(".*\\Qsteel.heart+\\E"))));
	}

	/**
	 * @see DATAMONGO-1232
	 */
	@Test
	public void likeShouldEscapeSourceWhenUsedWithTrailingWildcard() {

		PartTree tree = new PartTree("findByUsernameLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "cala.mity+*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();
		assertThat(query, is(query(where("username").regex("\\Qcala.mity+\\E.*"))));
	}

	/**
	 * @see DATAMONGO-1232
	 */
	@Test
	public void likeShouldBeTreatedCorrectlyWhenUsedWithWildcardOnly() {

		PartTree tree = new PartTree("findByUsernameLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();
		assertThat(query, is(query(where("username").regex(".*"))));
	}

	/**
	 * @see DATAMONGO-1342
	 */
	@Test
	public void bindsNullValueToContainsClause() {

		PartTree partTree = new PartTree("emailAddressesContains", User.class);

		ConvertingParameterAccessor accessor = getAccessor(converter, new Object[] { null });
		Query query = new MongoQueryCreator(partTree, accessor, context).createQuery();

		assertThat(query, is(query(where("emailAddresses").in((Object) null))));
	}

	/**
	 * @see DATAMONGO-1424
	 */
	@Test
	public void notLikeShouldEscapeSourceWhenUsedWithLeadingAndTrailingWildcard() {

		PartTree tree = new PartTree("findByUsernameNotLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*fire.fight+*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query.getQueryObject(),
				is(query(where("username").not().regex(".*\\Qfire.fight+\\E.*")).getQueryObject()));
	}

	/**
	 * @see DATAMONGO-1424
	 */
	@Test
	public void notLikeShouldEscapeSourceWhenUsedWithLeadingWildcard() {

		PartTree tree = new PartTree("findByUsernameNotLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*steel.heart+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query.getQueryObject(),
				is(query(where("username").not().regex(".*\\Qsteel.heart+\\E")).getQueryObject()));
	}

	/**
	 * @see DATAMONGO-1424
	 */
	@Test
	public void notLikeShouldEscapeSourceWhenUsedWithTrailingWildcard() {

		PartTree tree = new PartTree("findByUsernameNotLike", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "cala.mity+*"), context);
		Query query = creator.createQuery();

		assertThat(query.getQueryObject(), is(query(where("username").not().regex("\\Qcala.mity+\\E.*")).getQueryObject()));
	}

	/**
	 * @see DATAMONGO-1424
	 */
	@Test
	public void notLikeShouldBeTreatedCorrectlyWhenUsedWithWildcardOnly() {

		PartTree tree = new PartTree("findByUsernameNotLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();
		assertThat(query.getQueryObject(), is(query(where("username").not().regex(".*")).getQueryObject()));
	}

	interface PersonRepository extends Repository<Person, Long> {

		List<Person> findByLocationNearAndFirstname(Point location, Distance maxDistance, String firstname);
	}

	class User {

		ObjectId id;

		@Field("foo") String username;

		@DBRef User creator;

		List<String> emailAddresses;

		Address address;

		Address2dSphere address2dSphere;
	}

	static class Address {

		String street;
		Point geo;
	}

	static class Address2dSphere {
		String street;
		@GeoSpatialIndexed(type = GeoSpatialIndexType.GEO_2DSPHERE) Point geo;
	}
}
