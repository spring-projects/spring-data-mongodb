/*
 * Copyright 2016-2024 the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.bson.Document;
import org.junit.Test;

import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link FacetOperation}.
 *
 * @author Mark Paluch
 * @author Jérôme Guyon
 * @soundtrack Stanley Foort - You Make Me Believe In Magic (Extended Mix)
 */
public class FacetOperationUnitTests {

	@Test // DATAMONGO-1552
	public void shouldRenderCorrectly() {

		FacetOperation facetOperation = new FacetOperation().and(match(Criteria.where("price").exists(true)), //
				bucket("price") //
						.withBoundaries(0, 150, 200, 300, 400) //
						.withDefaultBucket("Other") //
						.andOutputCount().as("count") //
						.andOutput("title").push().as("titles")) //
				.as("categorizedByPrice") //
				.and(bucketAuto("year", 5)).as("categorizedByYears");

		Document agg = facetOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $facet: { categorizedByPrice: [" + "{ $match: { price: { $exists: true } } }, "
						+ "{ $bucket: { boundaries: [  0, 150, 200, 300, 400 ], groupBy: \"$price\", default: \"Other\", "
						+ "output: { count: { $sum: 1 }, titles: { $push: \"$title\" } } } } ],"
						+ "categorizedByYears: [ { $bucketAuto: { buckets: 5, groupBy: \"$year\" } } ] } }"));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderEmpty() {

		FacetOperation facetOperation = facet();

		Document agg = facetOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $facet: { } }"));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1552
	public void shouldRejectNonExistingFields() {

		FacetOperation facetOperation = new FacetOperation().and(project("price"), //
				bucket("price") //
						.withBoundaries(0, 150, 200, 300, 400) //
						.withDefaultBucket("Other") //
						.andOutputCount().as("count") //
						.andOutput("title").push().as("titles")) //
				.as("categorizedByPrice");

		Document agg = facetOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $facet: { categorizedByPrice: [" + "{ $match: { price: { $exists: true } } }, "
						+ "{ $bucket: {boundaries: [  0, 150, 200, 300, 400 ], groupBy: \"$price\", default: \"Other\", "
						+ "output: { count: { $sum: 1 }, titles: { $push: \"$title\" } } } } ],"
						+ "categorizedByYears: [ { $bucketAuto: { buckets: 5, groupBy: \"$year\" } } ] } }"));
	}

	@Test // DATAMONGO-1552
	public void shouldHonorProjectedFields() {

		FacetOperation facetOperation = new FacetOperation().and(project("price").and("title").as("name"), //
				bucketAuto("price", 5) //
						.andOutput("name").push().as("titles")) //
				.as("categorizedByPrice");

		Document agg = facetOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $facet: { categorizedByPrice: ["
				+ "{ $project: { price: 1, name: \"$title\" } }, " + "{ $bucketAuto: {  buckets: 5, groupBy: \"$price\", "
				+ "output: { titles: { $push: \"$name\" } } } } ] } }"));
	}

	@Test // DATAMONGO-1553
	public void shouldRenderSortByCountCorrectly() {

		FacetOperation facetOperation = new FacetOperation() //
				.and(sortByCount("country")) //
				.as("categorizedByCountry");

		Document agg = facetOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).containsEntry("$facet.categorizedByCountry.[0].$sortByCount", "$country");
	}
}
