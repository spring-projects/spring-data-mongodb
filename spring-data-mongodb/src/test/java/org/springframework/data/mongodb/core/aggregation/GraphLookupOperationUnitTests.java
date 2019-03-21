/*
 * Copyright 2016-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.query.Criteria;

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

		Document document = graphLookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).containsEntry("$graphLookup.depthField", "depth").containsEntry("$graphLookup.maxDepth", 42L);
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

		Document document = graphLookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).containsEntry("$graphLookup.restrictSearchWithMatch", new Document("key", "value"));
	}

	@Test // DATAMONGO-1551
	public void shouldRenderArrayOfStartsWithCorrectly() {

		GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder() //
				.from("employees") //
				.startWith("reportsTo", "boss") //
				.connectFrom("reportsTo") //
				.connectTo("name") //
				.as("reportingHierarchy");

		Document document = graphLookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document)
				.isEqualTo(Document.parse("{ $graphLookup : { from: \"employees\", startWith: [\"$reportsTo\", \"$boss\"], "
						+ "connectFromField: \"reportsTo\", connectToField: \"name\", as: \"reportingHierarchy\" } }"));
	}

	@Test // DATAMONGO-1551
	public void shouldRenderMixedArrayOfStartsWithCorrectly() {

		GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder() //
				.from("employees") //
				.startWith("reportsTo", LiteralOperators.Literal.asLiteral("$boss")) //
				.connectFrom("reportsTo") //
				.connectTo("name") //
				.as("reportingHierarchy");

		Document document = graphLookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document).containsEntry("$graphLookup.startWith",
				Arrays.asList("$reportsTo", new Document("$literal", "$boss")));
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

		Document document = graphLookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document).containsEntry("$graphLookup.startWith", new Document("$literal", "hello"));
	}

	@Test // DATAMONGO-2096
	public void connectFromShouldUseTargetFieldInsteadOfAlias() {

		AggregationOperation graphLookupOperation = Aggregation.graphLookup("user").startWith("contacts.userId")
				.connectFrom("contacts.userId").connectTo("_id").depthField("numConnections").as("connections");

		Document document = graphLookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document).containsEntry("$graphLookup.startWith", "$contacts.userId");
	}

	@Test // DATAMONGO-2096
	public void connectToShouldUseTargetFieldInsteadOfAlias() {

		AggregationOperation graphLookupOperation = Aggregation.graphLookup("user").startWith("contacts.userId")
				.connectFrom("userId").connectTo("connectto.field").depthField("numConnections").as("connections");

		Document document = graphLookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document).containsEntry("$graphLookup.connectToField", "connectto.field");
	}

	@Test // DATAMONGO-2096
	public void depthFieldShouldUseTargetFieldInsteadOfAlias() {

		AggregationOperation graphLookupOperation = Aggregation.graphLookup("user").startWith("contacts.userId")
				.connectFrom("contacts.userId").connectTo("_id").depthField("foo.bar").as("connections");

		Document document = graphLookupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document).containsEntry("$graphLookup.depthField", "foo.bar");
	}
}
