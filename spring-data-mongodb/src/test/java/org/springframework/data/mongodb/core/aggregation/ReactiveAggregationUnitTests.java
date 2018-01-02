/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Collation;

import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveAggregationUnitTests {

	static final String INPUT_COLLECTION = "collection-1";

	ReactiveMongoTemplate template;
	ReactiveMongoDatabaseFactory factory;
	@Mock MongoClient mongoClient;
	@Mock MongoDatabase db;
	@Mock MongoCollection<Document> collection;
	@Mock AggregatePublisher<Document> publisher;

	@Before
	public void setUp() {

		factory = new SimpleReactiveMongoDatabaseFactory(mongoClient, "db");
		template = new ReactiveMongoTemplate(factory);

		when(mongoClient.getDatabase("db")).thenReturn(db);
		when(db.getCollection(INPUT_COLLECTION)).thenReturn(collection);
		when(collection.aggregate(any())).thenReturn(publisher);
		when(publisher.allowDiskUse(any())).thenReturn(publisher);
		when(publisher.useCursor(any())).thenReturn(publisher);
		when(publisher.collation(any())).thenReturn(publisher);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1646
	public void shouldHandleMissingInputCollection() {
		template.aggregate(newAggregation(), (String) null, TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1646
	public void shouldHandleMissingAggregationPipeline() {
		template.aggregate(null, INPUT_COLLECTION, TagCount.class);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1646
	public void shouldHandleMissingEntityClass() {
		template.aggregate(newAggregation(), INPUT_COLLECTION, null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1646
	public void errorsOnCursorBatchSizeUsage() {

		template.aggregate(
				newAggregation(Product.class, //
						project("name", "netPrice")) //
								.withOptions(AggregationOptions.builder().cursorBatchSize(10).build()),
				INPUT_COLLECTION, TagCount.class).subscribe();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1646
	public void errorsOnExplainUsage() {

		template
				.aggregate(newAggregation(Product.class, //
						project("name", "netPrice")) //
								.withOptions(AggregationOptions.builder().explain(true).build()),
						INPUT_COLLECTION, TagCount.class)
				.subscribe();
	}

	@Test // DATAMONGO-1646
	public void appliesCollationCorrectlyWhenPresent() {

		template.aggregate(
				newAggregation(Product.class, //
						project("name", "netPrice")) //
								.withOptions(AggregationOptions.builder().collation(Collation.of("en_US")).build()),
				INPUT_COLLECTION, TagCount.class).subscribe();

		verify(publisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("en_US").build()));
	}

	@Test // DATAMONGO-1646
	public void doesNotSetCollationWhenNotPresent() {

		template.aggregate(newAggregation(Product.class, //
				project("name", "netPrice")) //
						.withOptions(AggregationOptions.builder().build()),
				INPUT_COLLECTION, TagCount.class).subscribe();

		verify(publisher, never()).collation(any());
	}

	@Test // DATAMONGO-1646
	public void appliesDiskUsageCorrectly() {

		template
				.aggregate(
						newAggregation(Product.class, //
								project("name", "netPrice")) //
										.withOptions(AggregationOptions.builder().allowDiskUse(true).build()),
						INPUT_COLLECTION, TagCount.class)
				.subscribe();

		verify(publisher).allowDiskUse(eq(true));
	}
}
