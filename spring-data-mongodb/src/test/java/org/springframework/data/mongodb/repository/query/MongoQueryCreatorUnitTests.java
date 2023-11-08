/*
 * Copyright 2011-2023 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.repository.query.StubParameterAccessor.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.Venue;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.geo.GeoJsonLineString;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
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

/**
 * Unit test for {@link MongoQueryCreator}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
class MongoQueryCreatorUnitTests {

	private MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context;
	private MongoConverter converter;

	@BeforeEach
	void beforeEach() {

		context = new MongoMappingContext();
		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
	}

	@Test
	void createsQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstName", Person.class);

		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Oliver"), context);
		Query query = creator.createQuery();
		assertThat(query).isEqualTo(query(where("firstName").is("Oliver")));
	}

	@Test // DATAMONGO-469
	void createsAndQueryCorrectly() {

		Person person = new Person();
		MongoQueryCreator creator = new MongoQueryCreator(new PartTree("findByFirstNameAndFriend", Person.class),
				getAccessor(converter, "Oliver", person), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("firstName").is("Oliver").and("friend").is(person)));
	}

	@Test
	void createsNotNullQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameNotNull", Person.class);
		Query query = new MongoQueryCreator(tree, getAccessor(converter), context).createQuery();

		assertThat(query).isEqualTo(new Query(Criteria.where("firstName").ne(null)));
	}

	@Test
	void createsIsNullQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameIsNull", Person.class);
		Query query = new MongoQueryCreator(tree, getAccessor(converter), context).createQuery();

		assertThat(query).isEqualTo(new Query(Criteria.where("firstName").is(null)));
	}

	@Test
	void bindsMetricDistanceParameterToNearSphereCorrectly() throws Exception {

		Point point = new Point(10, 20);
		Distance distance = new Distance(2.5, Metrics.KILOMETERS);

		Query query = query(
				where("location").nearSphere(point).maxDistance(distance.getNormalizedValue()).and("firstname").is("Dave"));
		assertBindsDistanceToQuery(point, distance, query);
	}

	@Test
	void bindsDistanceParameterToNearCorrectly() throws Exception {

		Point point = new Point(10, 20);
		Distance distance = new Distance(2.5);

		Query query = query(
				where("location").near(point).maxDistance(distance.getNormalizedValue()).and("firstname").is("Dave"));
		assertBindsDistanceToQuery(point, distance, query);
	}

	@Test
	void createsLessThanEqualQueryCorrectly() {

		PartTree tree = new PartTree("findByAgeLessThanEqual", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, 18), context);

		Query reference = query(where("age").lte(18));
		assertThat(creator.createQuery()).isEqualTo(reference);
	}

	@Test
	void createsGreaterThanEqualQueryCorrectly() {

		PartTree tree = new PartTree("findByAgeGreaterThanEqual", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, 18), context);

		Query reference = query(where("age").gte(18));
		assertThat(creator.createQuery()).isEqualTo(reference);
	}

	@Test // DATAMONGO-338
	void createsExistsClauseCorrectly() {

		PartTree tree = new PartTree("findByAgeExists", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, true), context);
		Query query = query(where("age").exists(true));
		assertThat(creator.createQuery()).isEqualTo(query);
	}

	@Test // DATAMONGO-338
	void createsRegexClauseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameRegex", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, ".*"), context);
		Query query = query(where("firstName").regex(".*"));
		assertThat(creator.createQuery()).isEqualTo(query);
	}

	@Test // DATAMONGO-338
	void createsTrueClauseCorrectly() {

		PartTree tree = new PartTree("findByActiveTrue", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter), context);
		Query query = query(where("active").is(true));
		assertThat(creator.createQuery()).isEqualTo(query);
	}

	@Test // DATAMONGO-338
	void createsFalseClauseCorrectly() {

		PartTree tree = new PartTree("findByActiveFalse", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter), context);
		Query query = query(where("active").is(false));
		assertThat(creator.createQuery()).isEqualTo(query);
	}

	@Test // DATAMONGO-413
	void createsOrQueryCorrectly() {

		PartTree tree = new PartTree("findByFirstNameOrAge", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Dave", 42), context);

		Query query = creator.createQuery();
		assertThat(query).isEqualTo(query(new Criteria().orOperator(where("firstName").is("Dave"), where("age").is(42))));
	}

	@Test // DATAMONGO-347
	void createsQueryReferencingADBRefCorrectly() {

		User user = new User();
		user.id = new ObjectId();

		PartTree tree = new PartTree("findByCreator", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, user), context);
		Document queryObject = creator.createQuery().getQueryObject();

		assertThat(queryObject.get("creator")).isEqualTo(user);
	}

	@Test // DATAMONGO-418
	void createsQueryWithStartingWithPredicateCorrectly() {

		PartTree tree = new PartTree("findByUsernameStartingWith", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "Matt"), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("username").regex("^Matt")));
	}

	@Test // DATAMONGO-418
	void createsQueryWithEndingWithPredicateCorrectly() {

		PartTree tree = new PartTree("findByUsernameEndingWith", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "ews"), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("username").regex("ews$")));
	}

	@Test // DATAMONGO-418
	void createsQueryWithContainingPredicateCorrectly() {

		PartTree tree = new PartTree("findByUsernameContaining", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "thew"), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("username").regex(".*thew.*")));
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
		assertThat(query).isEqualTo(query);
	}

	@Test // DATAMONGO-770
	void createsQueryWithFindByIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByfirstNameIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query).isEqualTo(query(where("firstName").regex("^dave$", "i")));
	}

	@Test // GH-4404
	void createsQueryWithFindByInClauseHavingIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findAllByFirstNameInIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, List.of("da've", "carter")), context);

		Query query = creator.createQuery();
		assertThat(query).isEqualTo(query(where("firstName")
				.in(List.of(new BsonRegularExpression("^\\Qda've\\E$", "i"), new BsonRegularExpression("^carter$", "i")))));
	}

	@Test // DATAMONGO-770
	void createsQueryWithFindByNotIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameNotIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query.toString()).isEqualTo(query(where("firstName").not().regex("^dave$", "i")).toString());
	}

	@Test // DATAMONGO-770
	void createsQueryWithFindByStartingWithIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameStartingWithIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query).isEqualTo(query(where("firstName").regex("^dave", "i")));
	}

	@Test // DATAMONGO-770
	void createsQueryWithFindByEndingWithIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameEndingWithIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query).isEqualTo(query(where("firstName").regex("dave$", "i")));
	}

	@Test // DATAMONGO-770
	void createsQueryWithFindByContainingIgnoreCaseCorrectly() {

		PartTree tree = new PartTree("findByFirstNameContainingIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();
		assertThat(query).isEqualTo(query(where("firstName").regex(".*dave.*", "i")));
	}

	@Test // DATAMONGO-770
	void shouldThrowExceptionForQueryWithFindByIgnoreCaseOnNonStringProperty() {

		PartTree tree = new PartTree("findByFirstNameAndAgeIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "foo", 42), context);

		assertThatIllegalArgumentException().isThrownBy(creator::createQuery)
				.withMessageContaining("must be of type String");
	}

	@Test // DATAMONGO-770
	void shouldOnlyGenerateLikeExpressionsForStringPropertiesIfAllIgnoreCase() {

		PartTree tree = new PartTree("findByFirstNameAndAgeAllIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave", 42), context);

		Query query = creator.createQuery();
		assertThat(query).isEqualTo(query(where("firstName").regex("^dave$", "i").and("age").is(42)));
	}

	@Test // DATAMONGO-566
	void shouldCreateDeleteByQueryCorrectly() {

		PartTree tree = new PartTree("deleteByFirstName", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave", 42), context);

		Query query = creator.createQuery();

		assertThat(tree.isDelete()).isTrue();
		assertThat(query).isEqualTo(query(where("firstName").is("dave")));
	}

	@Test // DATAMONGO-566
	void shouldCreateDeleteByQueryCorrectlyForMultipleCriteriaAndCaseExpressions() {

		PartTree tree = new PartTree("deleteByFirstNameAndAgeAllIgnoreCase", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave", 42), context);

		Query query = creator.createQuery();

		assertThat(tree.isDelete()).isTrue();
		assertThat(query).isEqualTo(query(where("firstName").regex("^dave$", "i").and("age").is(42)));
	}

	@Test // DATAMONGO-1075
	void shouldCreateInClauseWhenUsingContainsOnCollectionLikeProperty() {

		PartTree tree = new PartTree("findByEmailAddressesContaining", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("emailAddresses").in("dave")));
	}

	@Test // DATAMONGO-1075
	void shouldCreateInClauseWhenUsingNotContainsOnCollectionLikeProperty() {

		PartTree tree = new PartTree("findByEmailAddressesNotContaining", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "dave"), context);

		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("emailAddresses").not().in("dave")));
	}

	@Test // DATAMONGO-1075, DATAMONGO-1425
	void shouldCreateRegexWhenUsingNotContainsOnStringProperty() {

		PartTree tree = new PartTree("findByUsernameNotContaining", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "thew"), context);
		Query query = creator.createQuery();

		assertThat(query.getQueryObject().toJson())
				.isEqualTo(query(where("username").not().regex(".*thew.*")).getQueryObject().toJson());
	}

	@Test // DATAMONGO-1139
	void createsNonSphericalNearForDistanceWithDefaultMetric() {

		Point point = new Point(1.0, 1.0);
		Distance distance = new Distance(1.0);

		PartTree tree = new PartTree("findByLocationNear", Venue.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, point, distance), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("location").near(point).maxDistance(1.0)));
	}

	@Test // DATAMONGO-1136
	void shouldCreateWithinQueryCorrectly() {

		Point first = new Point(1, 1);
		Point second = new Point(2, 2);
		Point third = new Point(3, 3);
		Shape shape = new Polygon(first, second, third);

		PartTree tree = new PartTree("findByAddress_GeoWithin", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, shape), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("address.geo").within(shape)));
	}

	@Test // DATAMONGO-1110
	void shouldCreateNearSphereQueryForSphericalProperty() {

		Point point = new Point(10, 20);

		PartTree tree = new PartTree("findByAddress2dSphere_GeoNear", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, point), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("address2dSphere.geo").nearSphere(point)));
	}

	@Test // DATAMONGO-1110
	void shouldCreateNearSphereQueryForSphericalPropertyHavingDistanceWithDefaultMetric() {

		Point point = new Point(1.0, 1.0);
		Distance distance = new Distance(1.0);

		PartTree tree = new PartTree("findByAddress2dSphere_GeoNear", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, point, distance), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("address2dSphere.geo").nearSphere(point).maxDistance(1.0)));
	}

	@Test // DATAMONGO-1110
	void shouldCreateNearQueryForMinMaxDistance() {

		Point point = new Point(10, 20);
		Range<Distance> range = Distance.between(new Distance(10), new Distance(20));

		PartTree tree = new PartTree("findByAddress_GeoNear", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, point, range), context);
		Query query = creator.createQuery();

		assertThat(query).isEqualTo(query(where("address.geo").near(point).minDistance(10D).maxDistance(20D)));
	}

	@Test // DATAMONGO-1229
	void appliesIgnoreCaseToLeafProperty() {

		PartTree tree = new PartTree("findByAddressStreetIgnoreCase", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "Street");

		assertThat(new MongoQueryCreator(tree, accessor, context).createQuery()).isNotNull();
	}

	@Test // DATAMONGO-1232
	void ignoreCaseShouldEscapeSource() {

		PartTree tree = new PartTree("findByUsernameIgnoreCase", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "con.flux+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query).isEqualTo(query(where("username").regex("^\\Qcon.flux+\\E$", "i")));
	}

	@Test // DATAMONGO-1232
	void ignoreCaseShouldEscapeSourceWhenUsedForStartingWith() {

		PartTree tree = new PartTree("findByUsernameStartingWithIgnoreCase", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "dawns.light+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query).isEqualTo(query(where("username").regex("^\\Qdawns.light+\\E", "i")));
	}

	@Test // DATAMONGO-1232
	void ignoreCaseShouldEscapeSourceWhenUsedForEndingWith() {

		PartTree tree = new PartTree("findByUsernameEndingWithIgnoreCase", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "new.ton+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query).isEqualTo(query(where("username").regex("\\Qnew.ton+\\E$", "i")));
	}

	@Test // DATAMONGO-1232
	void likeShouldEscapeSourceWhenUsedWithLeadingAndTrailingWildcard() {

		PartTree tree = new PartTree("findByUsernameLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*fire.fight+*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query).isEqualTo(query(where("username").regex(".*\\Qfire.fight+\\E.*")));
	}

	@Test // DATAMONGO-1232
	void likeShouldEscapeSourceWhenUsedWithLeadingWildcard() {

		PartTree tree = new PartTree("findByUsernameLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*steel.heart+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query).isEqualTo(query(where("username").regex(".*\\Qsteel.heart+\\E")));
	}

	@Test // DATAMONGO-1232
	void likeShouldEscapeSourceWhenUsedWithTrailingWildcard() {

		PartTree tree = new PartTree("findByUsernameLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "cala.mity+*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();
		assertThat(query).isEqualTo(query(where("username").regex("\\Qcala.mity+\\E.*")));
	}

	@Test // DATAMONGO-1232
	void likeShouldBeTreatedCorrectlyWhenUsedWithWildcardOnly() {

		PartTree tree = new PartTree("findByUsernameLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();
		assertThat(query).isEqualTo(query(where("username").regex(".*")));
	}

	@Test // DATAMONGO-1342
	void bindsNullValueToContainsClause() {

		PartTree partTree = new PartTree("emailAddressesContains", User.class);

		ConvertingParameterAccessor accessor = getAccessor(converter, new Object[] { null });
		Query query = new MongoQueryCreator(partTree, accessor, context).createQuery();

		assertThat(query).isEqualTo(query(where("emailAddresses").in((Object) null)));
	}

	@Test // DATAMONGO-1424
	void notLikeShouldEscapeSourceWhenUsedWithLeadingAndTrailingWildcard() {

		PartTree tree = new PartTree("findByUsernameNotLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*fire.fight+*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query.getQueryObject().toJson())
				.isEqualTo(query(where("username").not().regex(".*\\Qfire.fight+\\E.*")).getQueryObject().toJson());
	}

	@Test // DATAMONGO-1424
	void notLikeShouldEscapeSourceWhenUsedWithLeadingWildcard() {

		PartTree tree = new PartTree("findByUsernameNotLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*steel.heart+");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();

		assertThat(query.getQueryObject().toJson())
				.isEqualTo(query(where("username").not().regex(".*\\Qsteel.heart+\\E")).getQueryObject().toJson());
	}

	@Test // DATAMONGO-1424
	void notLikeShouldEscapeSourceWhenUsedWithTrailingWildcard() {

		PartTree tree = new PartTree("findByUsernameNotLike", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, "cala.mity+*"), context);
		Query query = creator.createQuery();

		assertThat(query.getQueryObject().toJson())
				.isEqualTo(query(where("username").not().regex("\\Qcala.mity+\\E.*")).getQueryObject().toJson());
	}

	@Test // DATAMONGO-1424
	void notLikeShouldBeTreatedCorrectlyWhenUsedWithWildcardOnly() {

		PartTree tree = new PartTree("findByUsernameNotLike", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, "*");

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();
		assertThat(query.getQueryObject().toJson())
				.isEqualTo(query(where("username").not().regex(".*")).getQueryObject().toJson());
	}

	@Test // DATAMONGO-1588
	void queryShouldAcceptSubclassOfDeclaredArgument() {

		PartTree tree = new PartTree("findByLocationNear", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter, new GeoJsonPoint(-74.044502D, 40.689247D));

		Query query = new MongoQueryCreator(tree, accessor, context).createQuery();
		assertThat(query.getQueryObject()).containsKey("location");
	}

	@Test // DATAMONGO-1588
	void queryShouldThrowExceptionWhenArgumentDoesNotMatchDeclaration() {

		PartTree tree = new PartTree("findByLocationNear", User.class);
		ConvertingParameterAccessor accessor = getAccessor(converter,
				new GeoJsonLineString(new Point(-74.044502D, 40.689247D), new Point(-73.997330D, 40.730824D)));

		assertThatIllegalArgumentException().isThrownBy(() -> new MongoQueryCreator(tree, accessor, context).createQuery())
				.withMessageContaining("Expected parameter type of " + Point.class);
	}

	@Test // DATAMONGO-2003
	void createsRegexQueryForPatternCorrectly() {

		PartTree tree = new PartTree("findByFirstNameRegex", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, Pattern.compile(".*")), context);

		assertThat(creator.createQuery()).isEqualTo(query(where("firstName").regex(".*")));
	}

	@Test // DATAMONGO-2003
	void createsRegexQueryForPatternWithOptionsCorrectly() {

		Pattern pattern = Pattern.compile(".*", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);

		PartTree tree = new PartTree("findByFirstNameRegex", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree, getAccessor(converter, pattern), context);
		assertThat(creator.createQuery()).isEqualTo(query(where("firstName").regex(".*", "iu")));
	}

	@Test // DATAMONGO-2071
	void betweenShouldAllowSingleRageParameter() {

		PartTree tree = new PartTree("findByAgeBetween", Person.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree,
				getAccessor(converter, Range.of(Bound.exclusive(10), Bound.exclusive(11))), context);

		assertThat(creator.createQuery()).isEqualTo(query(where("age").gt(10).lt(11)));
	}

	@Test // DATAMONGO-2394
	void nearShouldUseMetricDistanceForGeoJsonTypes() {

		GeoJsonPoint point = new GeoJsonPoint(27.987901, 86.9165379);
		PartTree tree = new PartTree("findByLocationNear", User.class);
		MongoQueryCreator creator = new MongoQueryCreator(tree,
				getAccessor(converter, point, new Distance(1, Metrics.KILOMETERS)), context);

		assertThat(creator.createQuery()).isEqualTo(query(where("location").nearSphere(point).maxDistance(1000.0D)));
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

		Point location;
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
