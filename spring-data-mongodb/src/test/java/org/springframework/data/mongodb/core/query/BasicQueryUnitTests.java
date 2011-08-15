/*
 * Copyright 2011 the original author or authors.
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

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link BasicQuery}.
 *
 * @author Oliver Gierke
 */
public class BasicQueryUnitTests {
	
	@Test
	public void createsQueryFromPlainJson() {
		Query q = new BasicQuery("{ \"name\" : \"Thomas\"}");
		DBObject reference = new BasicDBObject("name", "Thomas");
		assertThat(q.getQueryObject(), is(reference));
	}

	@Test
	public void addsCriteriaCorrectly() {
		Query q = new BasicQuery("{ \"name\" : \"Thomas\"}").addCriteria(where("age").lt(80));
		DBObject reference = new BasicDBObject("name", "Thomas");
		reference.put("age", new BasicDBObject("$lt", 80));
		assertThat(q.getQueryObject(), is(reference));
	}
	
	@Test
	public void overridesSortCorrectly() {
		
		BasicQuery query = new BasicQuery("{}");
		query.setSortObject(new BasicDBObject("name", -1));
		query.sort().on("lastname", Order.ASCENDING);
		
		DBObject sortReference = new BasicDBObject("name", -1);
		sortReference.put("lastname", 1);
		assertThat(query.getSortObject(), is(sortReference));
	}
}
