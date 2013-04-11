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
import static org.springframework.data.mongodb.core.DBObjectUtils.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.Criteria;

import com.mongodb.DBObject;

/**
 * Tests of the {@link AggregationPipeline}.
 * 
 * @see DATAMONGO-586
 * @author Tobias Trelle
 */
public class AggregationPipelineTests {

	AggregationPipeline pipeline;

	@Before
	public void setUp() {
		pipeline = new AggregationPipeline();
	}

	@Test
	public void limitOperation() {

		pipeline.limit(42);

		List<DBObject> rawPipeline = pipeline.getOperations();
		assertDBObject("$limit", 42L, rawPipeline);
	}

	@Test
	public void skipOperation() {

		pipeline.skip(5);

		List<DBObject> rawPipeline = pipeline.getOperations();
		assertDBObject("$skip", 5L, rawPipeline);
	}

	@Test
	public void unwindOperation() {

		pipeline.unwind("$field");

		List<DBObject> rawPipeline = pipeline.getOperations();
		assertDBObject("$unwind", "$field", rawPipeline);
	}

	@Test
	public void unwindOperationWithAddedPrefix() {

		pipeline.unwind("field");

		List<DBObject> rawPipeline = pipeline.getOperations();
		assertDBObject("$unwind", "$field", rawPipeline);
	}

	@Test
	public void matchOperation() {

		Criteria criteria = new Criteria("title").is("Doc 1");
		pipeline.match(criteria);

		List<DBObject> rawPipeline = pipeline.getOperations();
		assertOneDocument(rawPipeline);

		DBObject match = rawPipeline.get(0);
		DBObject criteriaDoc = getAsDBObject(match, "$match");
		assertThat(criteriaDoc, is(notNullValue()));
		assertSingleDBObject("title", "Doc 1", criteriaDoc);
	}

	@Test
	public void sortOperation() {

		Sort sort = new Sort(new Sort.Order(Direction.ASC, "n"));
		pipeline.sort(sort);

		List<DBObject> rawPipeline = pipeline.getOperations();
		assertOneDocument(rawPipeline);

		DBObject sortDoc = rawPipeline.get(0);
		DBObject orderDoc = getAsDBObject(sortDoc, "$sort");
		assertThat(orderDoc, is(notNullValue()));
		assertSingleDBObject("n", 1, orderDoc);
	}

	@Test
	public void projectOperation() {

		Projection projection = new Projection("a");
		pipeline.project(projection);

		List<DBObject> rawPipeline = pipeline.getOperations();
		assertOneDocument(rawPipeline);

		DBObject projectionDoc = rawPipeline.get(0);
		DBObject fields = getAsDBObject(projectionDoc, "$project");
		assertThat(fields, is(notNullValue()));
		assertSingleDBObject("a", 1, fields);
	}

	private static void assertOneDocument(List<DBObject> result) {

		assertThat(result, is(notNullValue()));
		assertThat(result.size(), is(1));
	}

	private static void assertDBObject(String key, Object value, List<DBObject> result) {

		assertOneDocument(result);
		assertSingleDBObject(key, value, result.get(0));
	}

	private static void assertSingleDBObject(String key, Object value, DBObject doc) {
		assertThat(doc.get(key), is(value));
	}
}
