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
package org.springframework.data.document.mongodb.query;

import static org.springframework.data.document.mongodb.query.Criteria.where;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.document.mongodb.query.BasicQuery;
import org.springframework.data.document.mongodb.query.Query;

public class QueryTests {

	@Test
	public void testSimpleQuery() {
		Query q = new Query(where("name").is("Thomas")).and(where("age").lt(80));
		String expected = "{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testQueryWithNot() {
		Query q = new Query(where("name").is("Thomas")).and(where("age").not().mod(10, 0));
		String expected = "{ \"name\" : \"Thomas\" , \"age\" : { \"$not\" : { \"$mod\" : [ 10 , 0]}}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testOrQuery() {
		Query q = new Query();;
		q.or(
				new Query(where("name").is("Sven")).and(where("age").lt(50)), 
				new Query(where("age").lt(50)),
				new BasicQuery("{'name' : 'Thomas'}")
		);
		String expected = "{ \"$or\" : [ { \"name\" : \"Sven\" , \"age\" : { \"$lt\" : 50}} , { \"age\" : { \"$lt\" : 50}} , { \"name\" : \"Thomas\"}]}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

	@Test
	public void testQueryWithLimit() {
		Query q = new Query(where("name").gte("M").lte("T")).and(where("age").not().gt(22));		
		q.limit(50);
		String expected = "{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
		Assert.assertEquals(50, q.getLimit());
	}

	@Test
	public void testQueryWithFieldsAndSlice() {
		Query q = new Query(where("name").gte("M").lte("T")).and(where("age").not().gt(22));		
		q.fields().exclude("address").include("name").slice("orders", 10);

		String expected = "{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
		String expectedFields = "{ \"address\" : 0 , \"name\" : 1 , \"orders\" : { \"$slice\" : 10}}";
		Assert.assertEquals(expectedFields, q.getFieldsObject().toString());
	}

	@Test
	public void testBasicQuery() {
		Query q = new BasicQuery("{ \"name\" : \"Thomas\"}").and(where("age").lt(80));
		String expected = "{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}";
		Assert.assertEquals(expected, q.getQueryObject().toString());
	}

}
