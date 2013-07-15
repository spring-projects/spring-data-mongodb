/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.domain.Sort.Direction.*;
import static org.springframework.data.mongodb.core.DBObjectUtils.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.query.Criteria;

import com.mongodb.DBObject;

/**
 * Tests of the {@link AggregationPipeline}.
 * 
 * @see DATAMONGO-586
 * @author Tobias Trelle
 * @author Thomas Darimont
 */
public class AggregationPipelineTests {

	@Test
	public void limitOperation() {

		assertSingleDBObject("$limit", 42L, limit(42).toDbObject());
	}

	@Test
	public void skipOperation() {

		assertSingleDBObject("$skip", 5L, skip(5).toDbObject());
	}

	@Test
	public void unwindOperation() {

		assertSingleDBObject("$unwind", "$field", unwind("$field").toDbObject());
	}

	@Test
	public void unwindOperationWithAddedPrefix() {

		assertSingleDBObject("$unwind", "$field", unwind("field").toDbObject());
	}

	@Test
	public void matchOperation() {

		DBObject match = match(new Criteria("title").is("Doc 1")).toDbObject();
		DBObject criteriaDoc = getAsDBObject(match, "$match");
		assertThat(criteriaDoc, is(notNullValue()));
		assertSingleDBObject("title", "Doc 1", criteriaDoc);
	}

	@Test
	public void sortOperation() {

		DBObject sortDoc = sort(ASC, "n").toDbObject();
		DBObject orderDoc = getAsDBObject(sortDoc, "$sort");
		assertThat(orderDoc, is(notNullValue()));
		assertSingleDBObject("n", 1, orderDoc);
	}

	@Test
	public void projectOperation() {

		DBObject projectionDoc = project("a").toDbObject();
		DBObject fields = getAsDBObject(projectionDoc, "$project");
		assertThat(fields, is(notNullValue()));
		assertSingleDBObject("a", 1, fields);
	}

	private static void assertSingleDBObject(String key, Object value, DBObject doc) {
		assertThat(doc.get(key), is(value));
	}
}
