/*
 * Copyright 2017 the original author or authors.
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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for aggregation via {@link org.springframework.data.mongodb.core.ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
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
		StepVerifier
				.create(reactiveMongoTemplate.dropCollection(INPUT_COLLECTION) //
						.then(reactiveMongoTemplate.dropCollection(OUTPUT_COLLECTION)) //
						.then(reactiveMongoTemplate.dropCollection(Product.class)) //
						.then(reactiveMongoTemplate.dropCollection(City.class))) //
				.verifyComplete();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1646
	public void shouldHandleMissingInputCollection() {
		reactiveMongoTemplate.aggregate(newAggregation(), (String) null, TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1646
	public void shouldHandleMissingAggregationPipeline() {
		reactiveMongoTemplate.aggregate(null, INPUT_COLLECTION, TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1646
	public void shouldHandleMissingEntityClass() {
		reactiveMongoTemplate.aggregate(newAggregation(), INPUT_COLLECTION, null);
	}

	@Test // DATAMONGO-1646
	public void expressionsInProjectionExampleShowcase() {

		Product product = new Product("P1", "A", 1.99, 3, 0.05, 0.19);
		StepVerifier.create(reactiveMongoTemplate.insert(product)).expectNextCount(1).verifyComplete();

		double shippingCosts = 1.2;

		TypedAggregation<Product> agg = newAggregation(Product.class, //
				project("name", "netPrice") //
						.andExpression("netPrice * 10", shippingCosts).as("salesPrice") //
		);

		StepVerifier.create(reactiveMongoTemplate.aggregate(agg, Document.class)).consumeNextWith(actual -> {

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

		StepVerifier.create(reactiveMongoTemplate.insertAll(Arrays.asList(dresden, linz, braunschweig, weinheim)))
				.expectNextCount(4).verifyComplete();

		Aggregation agg = newAggregation( //
				match(where("population").lt(103)));

		StepVerifier.create(reactiveMongoTemplate.aggregate(agg, "city", City.class).collectList())
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

		StepVerifier.create(reactiveMongoTemplate.insertAll(Arrays.asList(dresden, linz, braunschweig, weinheim)))
				.expectNextCount(4).verifyComplete();

		Aggregation agg = newAggregation( //
				out(OUTPUT_COLLECTION));

		StepVerifier.create(reactiveMongoTemplate.aggregate(agg, "city", City.class)).expectNextCount(4).verifyComplete();
		StepVerifier.create(reactiveMongoTemplate.find(new Query(), City.class, OUTPUT_COLLECTION)).expectNextCount(4)
				.verifyComplete();
	}
}
