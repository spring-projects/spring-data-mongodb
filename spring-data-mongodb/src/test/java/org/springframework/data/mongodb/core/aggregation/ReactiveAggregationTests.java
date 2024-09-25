/*
 * Copyright 2017-2024 the original author or authors.
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

import static org.assertj.core.api.AssertionsForInterfaceTypes.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import reactor.test.StepVerifier;

import java.util.Arrays;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.TestEntities;
import org.springframework.data.mongodb.core.Venue;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration test for aggregation via {@link org.springframework.data.mongodb.core.ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveAggregationTests {

	private static final String INPUT_COLLECTION = "aggregation_test_collection";
	private static final String OUTPUT_COLLECTION = "aggregation_test_out";

	@Autowired ReactiveMongoTemplate reactiveMongoTemplate;

	@Before
	public void setUp() {
		cleanDb();
	}

	@After
	public void cleanUp() {
		cleanDb();
	}

	private void cleanDb() {

		reactiveMongoTemplate.dropCollection(INPUT_COLLECTION) //
				.then(reactiveMongoTemplate.dropCollection(OUTPUT_COLLECTION)) //
				.then(reactiveMongoTemplate.dropCollection(Product.class)) //
				.then(reactiveMongoTemplate.dropCollection(City.class)) //
				.then(reactiveMongoTemplate.dropCollection(Venue.class)).as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1646
	public void expressionsInProjectionExampleShowcase() {

		Product product = new Product("P1", "A", 1.99, 3, 0.05, 0.19);
		reactiveMongoTemplate.insert(product).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		double shippingCosts = 1.2;

		TypedAggregation<Product> agg = newAggregation(Product.class, //
				project("name", "netPrice") //
						.andExpression("netPrice * 10", shippingCosts).as("salesPrice") //
		);

		reactiveMongoTemplate.aggregate(agg, Document.class).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual).containsEntry("_id", product.id);
			assertThat(actual).containsEntry("name", product.name);
			assertThat(actual).containsEntry("salesPrice", product.netPrice * 10);
		}).verifyComplete();
	}

	@Test // DATAMONGO-1646
	public void shouldProjectMultipleDocuments() {

		City dresden = new City("Dresden", 100);
		City linz = new City("Linz", 101);
		City braunschweig = new City("Braunschweig", 102);
		City weinheim = new City("Weinheim", 103);

		reactiveMongoTemplate.insertAll(Arrays.asList(dresden, linz, braunschweig, weinheim)).as(StepVerifier::create)
				.expectNextCount(4).verifyComplete();

		Aggregation agg = newAggregation( //
				match(where("population").lt(103)));

		reactiveMongoTemplate.aggregate(agg, "city", City.class).collectList().as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(3).contains(dresden, linz, braunschweig);
				}).verifyComplete();
	}

	@Test // DATAMONGO-1646
	public void shouldAggregateToOutCollection() {

		City dresden = new City("Dresden", 100);
		City linz = new City("Linz", 101);
		City braunschweig = new City("Braunschweig", 102);
		City weinheim = new City("Weinheim", 103);

		reactiveMongoTemplate.insertAll(Arrays.asList(dresden, linz, braunschweig, weinheim)).as(StepVerifier::create)
				.expectNextCount(4).verifyComplete();

		Aggregation agg = newAggregation( //
				out(OUTPUT_COLLECTION));

		reactiveMongoTemplate.aggregate(agg, "city", City.class).as(StepVerifier::create).expectNextCount(4)
				.verifyComplete();
		reactiveMongoTemplate.find(new Query(), City.class, OUTPUT_COLLECTION).as(StepVerifier::create).expectNextCount(4)
				.verifyComplete();
	}

	@Test // DATAMONGO-1986
	public void runMatchOperationCriteriaThroughQueryMapperForTypedAggregation() {

		reactiveMongoTemplate.insertAll(TestEntities.geolocation().newYork()).as(StepVerifier::create).expectNextCount(12)
				.verifyComplete();

		Aggregation aggregation = newAggregation(Venue.class,
				match(Criteria.where("location")
						.within(new Box(new Point(-73.99756, 40.73083), new Point(-73.988135, 40.741404)))),
				project("id", "location", "name"));

		reactiveMongoTemplate.aggregate(aggregation, "newyork", Document.class).as(StepVerifier::create).expectNextCount(4)
				.verifyComplete();
	}

	@Test // DATAMONGO-1986
	public void runMatchOperationCriteriaThroughQueryMapperForUntypedAggregation() {

		reactiveMongoTemplate.insertAll(TestEntities.geolocation().newYork()).as(StepVerifier::create).expectNextCount(12)
				.verifyComplete();

		Aggregation aggregation = newAggregation(
				match(Criteria.where("location")
						.within(new Box(new Point(-73.99756, 40.73083), new Point(-73.988135, 40.741404)))),
				project("id", "location", "name"));

		reactiveMongoTemplate.aggregate(aggregation, "newyork", Document.class).as(StepVerifier::create).expectNextCount(4)
				.verifyComplete();
	}

	@Test // DATAMONGO-2356
	public void skipOutputDoesNotReadBackAggregationResults() {

		Product product = new Product("P1", "A", 1.99, 3, 0.05, 0.19);
		reactiveMongoTemplate.insert(product).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		double shippingCosts = 1.2;

		TypedAggregation<Product> agg = newAggregation(Product.class, //
				project("name", "netPrice") //
						.andExpression("netPrice * 10", shippingCosts).as("salesPrice") //
		).withOptions(AggregationOptions.builder().skipOutput().build());

		reactiveMongoTemplate.aggregate(agg, Document.class).as(StepVerifier::create).verifyComplete();
	}

}
