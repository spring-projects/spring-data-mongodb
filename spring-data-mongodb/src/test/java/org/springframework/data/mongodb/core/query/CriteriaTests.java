/*
 * Copyright 2010-2014 the original author or authors.
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

import org.junit.Test;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.test.util.IsBsonObject;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class CriteriaTests {

	@Test
	public void testSimpleCriteria() {
		Criteria c = new Criteria("name").is("Bubba");
		assertEquals("{ \"name\" : \"Bubba\"}", c.getCriteriaObject().toString());
	}

	@Test
	public void testNotEqualCriteria() {
		Criteria c = new Criteria("name").ne("Bubba");
		assertEquals("{ \"name\" : { \"$ne\" : \"Bubba\"}}", c.getCriteriaObject().toString());
	}

	@Test
	public void buildsIsNullCriteriaCorrectly() {

		DBObject reference = new BasicDBObject("name", null);

		Criteria criteria = new Criteria("name").is(null);
		assertThat(criteria.getCriteriaObject(), is(reference));
	}

	@Test
	public void testChainedCriteria() {
		Criteria c = new Criteria("name").is("Bubba").and("age").lt(21);
		assertEquals("{ \"name\" : \"Bubba\" , \"age\" : { \"$lt\" : 21}}", c.getCriteriaObject().toString());
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

	/**
	 * @see DATAMONGO-507
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionWhenTryingToNegateAndOperation() {

		new Criteria() //
				.not() //
				.andOperator(Criteria.where("delete").is(true).and("_id").is(42)); //
	}

	/**
	 * @see DATAMONGO-507
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionWhenTryingToNegateOrOperation() {

		new Criteria() //
				.not() //
				.orOperator(Criteria.where("delete").is(true).and("_id").is(42)); //
	}

	/**
	 * @see DATAMONGO-507
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionWhenTryingToNegateNorOperation() {

		new Criteria() //
				.not() //
				.norOperator(Criteria.where("delete").is(true).and("_id").is(42)); //
	}

	/**
	 * @see DATAMONGO-507
	 */
	@Test
	public void shouldNegateFollowingSimpleExpression() {

		Criteria c = Criteria.where("age").not().gt(18).and("status").is("student");
		DBObject co = c.getCriteriaObject();

		assertThat(co, is(notNullValue()));
		assertThat(co.toString(), is("{ \"age\" : { \"$not\" : { \"$gt\" : 18}} , \"status\" : \"student\"}"));
	}

	/**
	 * @see DATAMONGO-1068
	 */
	@Test
	public void getCriteriaObjectShouldReturnEmptyDBOWhenNoCriteriaSpecified() {

		DBObject dbo = new Criteria().getCriteriaObject();

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().get()));
	}

	/**
	 * @see DATAMONGO-1068
	 */
	@Test
	public void getCriteriaObjectShouldUseCritieraValuesWhenNoKeyIsPresent() {

		DBObject dbo = new Criteria().lt("foo").getCriteriaObject();

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("$lt", "foo").get()));
	}

	/**
	 * @see DATAMONGO-1068
	 */
	@Test
	public void getCriteriaObjectShouldUseCritieraValuesWhenNoKeyIsPresentButMultipleCriteriasPresent() {

		DBObject dbo = new Criteria().lt("foo").gt("bar").getCriteriaObject();

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("$lt", "foo").add("$gt", "bar").get()));
	}

	/**
	 * @see DATAMONGO-1068
	 */
	@Test
	public void getCriteriaObjectShouldRespectNotWhenNoKeyPresent() {

		DBObject dbo = new Criteria().lt("foo").not().getCriteriaObject();

		assertThat(dbo, equalTo(new BasicDBObjectBuilder().add("$not", new BasicDBObject("$lt", "foo")).get()));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void geoJsonTypesShouldBeWrappedInGeometry() {

		DBObject dbo = new Criteria("foo").near(new GeoJsonPoint(100, 200)).getCriteriaObject();

		assertThat(dbo, IsBsonObject.isBsonObject().containing("foo.$near.$geometry", new GeoJsonPoint(100, 200)));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void legacyCoordinateTypesShouldNotBeWrappedInGeometry() {

		DBObject dbo = new Criteria("foo").near(new Point(100, 200)).getCriteriaObject();

		assertThat(dbo, IsBsonObject.isBsonObject().notContaining("foo.$near.$geometry"));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void maxDistanceShouldBeMappedInsideNearWhenUsedAlongWithGeoJsonType() {

		DBObject dbo = new Criteria("foo").near(new GeoJsonPoint(100, 200)).maxDistance(50D).getCriteriaObject();

		assertThat(dbo, IsBsonObject.isBsonObject().containing("foo.$near.$maxDistance", 50D));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void maxDistanceShouldBeMappedInsideNearSphereWhenUsedAlongWithGeoJsonType() {

		DBObject dbo = new Criteria("foo").nearSphere(new GeoJsonPoint(100, 200)).maxDistance(50D).getCriteriaObject();

		assertThat(dbo, IsBsonObject.isBsonObject().containing("foo.$nearSphere.$maxDistance", 50D));
	}
}
