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
package org.springframework.data.document.mongodb.builder;

import org.junit.Assert;
import org.junit.Test;

public class QueryTests {

	@Test
	public void testSimpleQuery() {
		QuerySpec q = new QuerySpec();
		q.find("name").is("Thomas");
		q.find("age").lt(80);
		String expected = "{ \"name\" : \"Thomas\" , \"age\" : { \"$lt\" : 80}}";
		Assert.assertEquals(expected, q.build().getQueryObject().toString());
	}

	@Test
	public void testQueryWithNot() {
		QuerySpec q = new QuerySpec();
		q.find("name").is("Thomas");
		q.find("age").not().mod(10, 0);
		String expected = "{ \"name\" : \"Thomas\" , \"age\" : { \"$not\" : { \"$mod\" : [ 10 , 0]}}}";
		Assert.assertEquals(expected, q.build().getQueryObject().toString());
	}

	@Test
	public void testOrQuery() {
		QuerySpec q = new QuerySpec();;
		q.or(
				new QuerySpec().find("name").is("Sven").and("age").lt(50).build(), 
				new QuerySpec().find("age").lt(50).build(),
				new BasicQuery("{'name' : 'Thomas'}")
		);
		String expected = "{ \"$or\" : [ { \"name\" : \"Sven\" , \"age\" : { \"$lt\" : 50}} , { \"age\" : { \"$lt\" : 50}} , { \"name\" : \"Thomas\"}]}";
		Assert.assertEquals(expected, q.build().getQueryObject().toString());
	}

	@Test
	public void testQueryWithLimit() {
		QuerySpec q = new QuerySpec();
		q.find("name").gte("M").lte("T").and("age").not().gt(22);		
		q.limit(50);
		String expected = "{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}";
		Assert.assertEquals(expected, q.build().getQueryObject().toString());
		Assert.assertEquals(50, q.build().getLimit());
	}

	@Test
	public void testQueryWithFieldsAndSlice() {
		QuerySpec q = new QuerySpec();
		q.find("name").gte("M").lte("T").and("age").not().gt(22);		
		q.fields().exclude("address").include("name").slice("orders", 10);

		String expected = "{ \"name\" : { \"$gte\" : \"M\" , \"$lte\" : \"T\"} , \"age\" : { \"$not\" : { \"$gt\" : 22}}}";
		Assert.assertEquals(expected, q.build().getQueryObject().toString());
		String expectedFields = "{ \"address\" : 0 , \"name\" : 1 , \"orders\" : { \"$slice\" : 10}}";
		Assert.assertEquals(expectedFields, q.build().getFieldsObject().toString());
	}
}
