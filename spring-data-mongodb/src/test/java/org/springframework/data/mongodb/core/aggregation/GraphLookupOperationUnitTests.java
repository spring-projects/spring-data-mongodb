/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.query.Criteria;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link GraphLookupOperation}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class GraphLookupOperationUnitTests {

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1551
	public void rejectsNullFromCollection() {
		GraphLookupOperation.builder().from(null);
	}

	@Test // DATAMONGO-1551
	public void shouldRenderCorrectly() {

		GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder() //
				.from("employees") //
				.startWith("reportsTo") //
				.connectFrom("reportsTo") //
				.connectTo("name") //
				.depthField("depth") //
				.maxDepth(42) //
				.as("reportingHierarchy");

		DBObject dbObject = graphLookupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(dbObject,
				isBsonObject().containing("$graphLookup.depthField", "depth").containing("$graphLookup.maxDepth", 42L));
	}

	@Test // DATAMONGO-1551
	public void shouldRenderCriteriaCorrectly() {

		GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder() //
				.from("employees") //
				.startWith("reportsTo") //
				.connectFrom("reportsTo") //
				.connectTo("name") //
				.restrict(Criteria.where("key").is("value")) //
				.as("reportingHierarchy");

		DBObject dbObject = graphLookupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(dbObject,
				isBsonObject().containing("$graphLookup.restrictSearchWithMatch", new BasicDBObject("key", "value")));
	}

	@Test // DATAMONGO-1551
	public void shouldRenderArrayOfStartsWithCorrectly() {

		GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder() //
				.from("employees") //
				.startWith("reportsTo", "boss") //
				.connectFrom("reportsTo") //
				.connectTo("name") //
				.as("reportingHierarchy");

		DBObject dbObject = graphLookupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject,
				is(JSON.parse("{ $graphLookup : { from: \"employees\", startWith: [\"$reportsTo\", \"$boss\"], "
						+ "connectFromField: \"reportsTo\", connectToField: \"name\", as: \"reportingHierarchy\" } }")));
	}

	@Test // DATAMONGO-1551
	public void shouldRenderMixedArrayOfStartsWithCorrectly() {

		GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder() //
				.from("employees") //
				.startWith("reportsTo", LiteralOperators.Literal.asLiteral("$boss")) //
				.connectFrom("reportsTo") //
				.connectTo("name") //
				.as("reportingHierarchy");

		DBObject dbObject = graphLookupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject,
				is(JSON.parse("{ $graphLookup : { from: \"employees\", startWith: [\"$reportsTo\", { $literal: \"$boss\"}], "
						+ "connectFromField: \"reportsTo\", connectToField: \"name\", as: \"reportingHierarchy\" } }")));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1551
	public void shouldRejectUnknownTypeInMixedArrayOfStartsWithCorrectly() {

		GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder() //
				.from("employees") //
				.startWith("reportsTo", new Person()) //
				.connectFrom("reportsTo") //
				.connectTo("name") //
				.as("reportingHierarchy");
	}

	@Test // DATAMONGO-1551
	public void shouldRenderStartWithAggregationExpressions() {

		GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder() //
				.from("employees") //
				.startWith(LiteralOperators.Literal.asLiteral("hello")) //
				.connectFrom("reportsTo") //
				.connectTo("name") //
				.as("reportingHierarchy");

		DBObject dbObject = graphLookupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject, is(JSON.parse("{ $graphLookup : { from: \"employees\", startWith: { $literal: \"hello\"}, "
				+ "connectFromField: \"reportsTo\", connectToField: \"name\", as: \"reportingHierarchy\" } }")));
	}
}
