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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

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
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveAggregationUnitTests {

	private static final String INPUT_COLLECTION = "collection-1";

	private ReactiveMongoTemplate template;
	private ReactiveMongoDatabaseFactory factory;
	@Mock MongoClient mongoClient;
	@Mock MongoDatabase db;
	@Mock MongoCollection<Document> collection;
	@Mock AggregatePublisher<Document> publisher;

	@BeforeEach
	void setUp() {

		factory = new SimpleReactiveMongoDatabaseFactory(mongoClient, "db");
		template = new ReactiveMongoTemplate(factory);

		when(mongoClient.getDatabase("db")).thenReturn(db);
		when(db.getCollection(eq(INPUT_COLLECTION), any(Class.class))).thenReturn(collection);
		when(collection.aggregate(anyList(), any(Class.class))).thenReturn(publisher);
		when(publisher.allowDiskUse(any())).thenReturn(publisher);
		when(publisher.collation(any())).thenReturn(publisher);
	}

	@Test // DATAMONGO-1646
	void shouldHandleMissingInputCollection() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> template.aggregate(newAggregation(), (String) null, TagCount.class));
	}

	@Test // DATAMONGO-1646
	void shouldHandleMissingAggregationPipeline() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.aggregate(null, INPUT_COLLECTION, TagCount.class));
	}

	@Test // DATAMONGO-1646
	void shouldHandleMissingEntityClass() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.aggregate(newAggregation(), INPUT_COLLECTION, null));
	}

	@Test // DATAMONGO-1646
	void errorsOnExplainUsage() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.aggregate(newAggregation(Product.class, //
				project("name", "netPrice")) //
						.withOptions(AggregationOptions.builder().explain(true).build()),
				INPUT_COLLECTION, TagCount.class).subscribe());
	}

	@Test // DATAMONGO-1646, DATAMONGO-1311
	void appliesBatchSizeWhenPresent() {

		when(publisher.batchSize(anyInt())).thenReturn(publisher);

		AggregationOptions options = AggregationOptions.builder().cursorBatchSize(1234).build();
		template.aggregate(newAggregation(Product.class, //
				project("name", "netPrice")) //
						.withOptions(options),
				INPUT_COLLECTION, TagCount.class).subscribe();

		verify(publisher).batchSize(1234);
	}

	@Test // DATAMONGO-1646
	void appliesCollationCorrectlyWhenPresent() {

		template.aggregate(newAggregation(Product.class, //
				project("name", "netPrice")) //
						.withOptions(AggregationOptions.builder().collation(Collation.of("en_US")).build()),
				INPUT_COLLECTION, TagCount.class).subscribe();

		verify(publisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("en_US").build()));
	}

	@Test // DATAMONGO-1646
	void doesNotSetCollationWhenNotPresent() {

		template.aggregate(newAggregation(Product.class, //
				project("name", "netPrice")) //
						.withOptions(AggregationOptions.builder().build()),
				INPUT_COLLECTION, TagCount.class).subscribe();

		verify(publisher, never()).collation(any());
	}

	@Test // DATAMONGO-1646
	void appliesDiskUsageCorrectly() {

		template.aggregate(newAggregation(Product.class, //
				project("name", "netPrice")) //
						.withOptions(AggregationOptions.builder().allowDiskUse(true).build()),
				INPUT_COLLECTION, TagCount.class).subscribe();

		verify(publisher).allowDiskUse(eq(true));
	}
}
