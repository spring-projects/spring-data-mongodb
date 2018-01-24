/*
 * Copyright 2010-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.geo.GeoJsonLineString;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;

/**
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class CriteriaTests {

	@Test
	public void testSimpleCriteria() {
		Criteria c = new Criteria("name").is("Bubba");
		assertEquals(Document.parse("{ \"name\" : \"Bubba\"}"), c.getCriteriaObject());
	}

	@Test
	public void testNotEqualCriteria() {
		Criteria c = new Criteria("name").ne("Bubba");
		assertEquals(Document.parse("{ \"name\" : { \"$ne\" : \"Bubba\"}}"), c.getCriteriaObject());
	}

	@Test
	public void buildsIsNullCriteriaCorrectly() {

		Document reference = new Document("name", null);

		Criteria criteria = new Criteria("name").is(null);
		assertThat(criteria.getCriteriaObject(), is(reference));
	}

	@Test
	public void testChainedCriteria() {
		Criteria c = new Criteria("name").is("Bubba").and("age").lt(21);
		assertEquals(Document.parse("{ \"name\" : \"Bubba\" , \"age\" : { \"$lt\" : 21}}"), c.getCriteriaObject());
	}

	@Test(expected = InvalidMongoDbApiUsageException.class)
	public void testCriteriaWithMultipleConditionsForSameKey() {
		Criteria c = new Criteria("name").gte("M").and("name").ne("A");
		c.getCriteriaObject();
	}

	@Test
	public void equalIfCriteriaMatches() {

		Criteria left = new Criteria("name").is("Foo").and("lastname").is("Bar");
		Criteria right = new Criteria("name").is("Bar").and("lastname").is("Bar");

		assertThat(left, is(not(right)));
		assertThat(right, is(not(left)));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-507
	public void shouldThrowExceptionWhenTryingToNegateAndOperation() {

		new Criteria() //
				.not() //
				.andOperator(Criteria.where("delete").is(true).and("_id").is(42)); //
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-507
	public void shouldThrowExceptionWhenTryingToNegateOrOperation() {

		new Criteria() //
				.not() //
				.orOperator(Criteria.where("delete").is(true).and("_id").is(42)); //
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-507
	public void shouldThrowExceptionWhenTryingToNegateNorOperation() {

		new Criteria() //
				.not() //
				.norOperator(Criteria.where("delete").is(true).and("_id").is(42)); //
	}

	@Test // DATAMONGO-507
	public void shouldNegateFollowingSimpleExpression() {

		Criteria c = Criteria.where("age").not().gt(18).and("status").is("student");
		Document co = c.getCriteriaObject();

		assertThat(co, is(notNullValue()));
		assertThat(co, is(Document.parse("{ \"age\" : { \"$not\" : { \"$gt\" : 18}} , \"status\" : \"student\"}")));
	}

	@Test // DATAMONGO-1068
	public void getCriteriaObjectShouldReturnEmptyDocumentWhenNoCriteriaSpecified() {

		Document document = new Criteria().getCriteriaObject();

		assertThat(document, equalTo(new Document()));
	}

	@Test // DATAMONGO-1068
	public void getCriteriaObjectShouldUseCritieraValuesWhenNoKeyIsPresent() {

		Document document = new Criteria().lt("foo").getCriteriaObject();

		assertThat(document, equalTo(new Document().append("$lt", "foo")));
	}

	@Test // DATAMONGO-1068
	public void getCriteriaObjectShouldUseCritieraValuesWhenNoKeyIsPresentButMultipleCriteriasPresent() {

		Document document = new Criteria().lt("foo").gt("bar").getCriteriaObject();

		assertThat(document, equalTo(new Document().append("$lt", "foo").append("$gt", "bar")));
	}

	@Test // DATAMONGO-1068
	public void getCriteriaObjectShouldRespectNotWhenNoKeyPresent() {

		Document document = new Criteria().lt("foo").not().getCriteriaObject();

		assertThat(document, equalTo(new Document().append("$not", new Document("$lt", "foo"))));
	}

	@Test // DATAMONGO-1135
	public void geoJsonTypesShouldBeWrappedInGeometry() {

		Document document = new Criteria("foo").near(new GeoJsonPoint(100, 200)).getCriteriaObject();

		assertThat(document, isBsonObject().containing("foo.$near.$geometry", new GeoJsonPoint(100, 200)));
	}

	@Test // DATAMONGO-1135
	public void legacyCoordinateTypesShouldNotBeWrappedInGeometry() {

		Document document = new Criteria("foo").near(new Point(100, 200)).getCriteriaObject();

		assertThat(document, isBsonObject().notContaining("foo.$near.$geometry"));
	}

	@Test // DATAMONGO-1135
	public void maxDistanceShouldBeMappedInsideNearWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").near(new GeoJsonPoint(100, 200)).maxDistance(50D).getCriteriaObject();

		assertThat(document, isBsonObject().containing("foo.$near.$maxDistance", 50D));
	}

	@Test // DATAMONGO-1135
	public void maxDistanceShouldBeMappedInsideNearSphereWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").nearSphere(new GeoJsonPoint(100, 200)).maxDistance(50D).getCriteriaObject();

		assertThat(document, isBsonObject().containing("foo.$nearSphere.$maxDistance", 50D));
	}

	@Test // DATAMONGO-1110
	public void minDistanceShouldBeMappedInsideNearWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").near(new GeoJsonPoint(100, 200)).minDistance(50D).getCriteriaObject();

		assertThat(document, isBsonObject().containing("foo.$near.$minDistance", 50D));
	}

	@Test // DATAMONGO-1110
	public void minDistanceShouldBeMappedInsideNearSphereWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").nearSphere(new GeoJsonPoint(100, 200)).minDistance(50D).getCriteriaObject();

		assertThat(document, isBsonObject().containing("foo.$nearSphere.$minDistance", 50D));
	}

	@Test // DATAMONGO-1110
	public void minAndMaxDistanceShouldBeMappedInsideNearSphereWhenUsedAlongWithGeoJsonType() {

		Document document = new Criteria("foo").nearSphere(new GeoJsonPoint(100, 200)).minDistance(50D).maxDistance(100D)
				.getCriteriaObject();

		assertThat(document, isBsonObject().containing("foo.$nearSphere.$minDistance", 50D));
		assertThat(document, isBsonObject().containing("foo.$nearSphere.$maxDistance", 100D));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1134
	public void intersectsShouldThrowExceptionWhenCalledWihtNullValue() {
		new Criteria("foo").intersects(null);
	}

	@Test // DATAMONGO-1134
	public void intersectsShouldWrapGeoJsonTypeInGeometryCorrectly() {

		GeoJsonLineString lineString = new GeoJsonLineString(new Point(0, 0), new Point(10, 10));
		Document document = new Criteria("foo").intersects(lineString).getCriteriaObject();

		assertThat(document, isBsonObject().containing("foo.$geoIntersects.$geometry", lineString));
	}
}
