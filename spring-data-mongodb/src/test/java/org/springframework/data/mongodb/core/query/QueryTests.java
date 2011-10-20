/*
 * Copyright 2010-2011 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.Criteria.*;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;

public class QueryTests {

	@Test
	public void testSimpleQuery() {
		Query q = new Query(where("name").is("Thomas").and("age").lt(80));
		String expected = "{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testQueryWithNot() {
		Query q = new Query(where("name").is("Thomas").and("age").not().mod(10, 0));
		String expected = "{ \"name\" : \"Thomas\" , \"age\" : { \"$not\" : { \"$mod\" : [ 10 , 0]}}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testInvalidQueryWithNotIs() {
		try {
			new Query(where("name").not().is("Thomas"));
			Assert.fail("This should have caused an InvalidDocumentStoreApiUsageException");
		} catch (InvalidMongoDbApiUsageException e) {
		}
	}

	@Test
	public void testOrQuery() {
		Query q = new Query(new Criteria().orOperator(where("name").is("Sven").and("age").lt(50), where("age").lt(50), where("name").is("Thomas")));
		String expected = "{ \"$or\" : [ { \"name\" : \"Sven\" , \"age\" : { \"$lt\" : 50}} , { \"age\" : { \"$lt\" : 50}} , { \"name\" : \"Thomas\"}]}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testAndQuery() {
		Query q = new Query(new Criteria().andOperator(where("name").is("Sven"), where("age").lt(50)));
		String expected = "{ \"$and\" : [ { \"name\" : \"Sven\"} , { \"age\" : { \"$lt\" : 50}}]}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testNorQuery() {
		Query q = new Query(new Criteria().norOperator(where("name").is("Sven"), where("age").lt(50), where("name").is("Thomas")));
		String expected = "{ \"$nor\" : [ { \"name\" : \"Sven\"} , { \"age\" : { \"$lt\" : 50}} , { \"name\" : \"Thomas\"}]}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testQueryWithLimit() {
		Query q = new Query(where("name").gte("M").lte("T").and("age").not().gt(22));
		q.limit(50);
		String expected = "{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
		Assert.assertEquals(50, q.getLimit());
	}

	@Test
	public void testQueryWithFieldsAndSlice() {
		Query q = new Query(where("name").gte("M").lte("T").and("age").not().gt(22));
		q.fields().exclude("address").include("name").slice("orders", 10);

		String expected = "{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
		String expectedFields = "{ \"address\" : 0 , \"name\" : 1 , \"orders\" : { \"$slice\" : 10}}";
		Assert.assertEquals(expectedFields, q.getFieldsObject().toString());
	}

	@Test
	public void testSimpleQueryWithChainedCriteria() {
		Query q = new Query(where("name").is("Thomas").and("age").lt(80));
		String expected = "{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testComplexQueryWithMultipleChainedCriteria() {
		Query q = new Query(where("name").regex("^T.*").and("age").gt(20).lt(80).and("city")
				.in("Stockholm", "London", "New York"));
		String expected = "{ \"name\" : { \"$regex\" : \"^T.*\"} , \"age\" : { \"$gt\" : 20 , \"$lt\" : 80} , "
				+ "\"city\" : { \"$in\" : [ \"Stockholm\" , \"London\" , \"New York\"]}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testAddCriteriaWithComplexQueryWithMultipleChainedCriteria() {
		Query q1 = new Query(where("name").regex("^T.*").and("age").gt(20).lt(80).and("city")
				.in("Stockholm", "London", "New York"));
		Query q2 = new Query(where("name").regex("^T.*").and("age").gt(20).lt(80)).addCriteria(where("city").in(
				"Stockholm", "London", "New York"));
		Assert.assertEquals(q1.getQueryObject().toString(), q2.getQueryObject().toString());
		Query q3 = new Query(where("name").regex("^T.*")).addCriteria(where("age").gt(20).lt(80)).addCriteria(
				where("city").in("Stockholm", "London", "New York"));
		Assert.assertEquals(q1.getQueryObject().toString(), q3.getQueryObject().toString());
	}

	@Test
	public void testQueryWithElemMatch() {
		Query q = new Query(where("openingHours").elemMatch(where("dayOfWeek").is("Monday").and("open").lte("1800")));
		String expected = "{ \"openingHours\" : { \"$elemMatch\" : { \"dayOfWeek\" : \"Monday\" , \"open\" : { \"$lte\" : \"1800\"}}}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testQueryWithIn() {
		Query q = new Query(where("state").in("NY", "NJ", "PA"));
		String expected = "{ \"state\" : { \"$in\" : [ \"NY\" , \"NJ\" , \"PA\"]}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testQueryWithRegex() {
		Query q = new Query(where("name").regex("b.*"));
		String expected = "{ \"name\" : { \"$regex\" : \"b.*\"}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testQueryWithRegexandOption() {
		Query q = new Query(where("name").regex("b.*", "i"));
		String expected = "{ \"name\" : { \"$regex\" : \"b.*\" , \"$options\" : \"i\"}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}
}
