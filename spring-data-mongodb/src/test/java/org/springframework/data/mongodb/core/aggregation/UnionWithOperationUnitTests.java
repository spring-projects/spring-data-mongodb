/*
 * Copyright 2020-2023 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link UnionWithOperation}.
 *
 * @author Christoph Strobl
 */
class UnionWithOperationUnitTests {

	@Test // DATAMONGO-2622
	void throwsErrorWhenNoCollectionPresent() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> UnionWithOperation.unionWith(null));
	}

	@Test // DATAMONGO-2622
	void rendersJustCollectionCorrectly() {

		assertThat(UnionWithOperation.unionWith("coll-1").toPipelineStages(contextFor(Warehouse.class)))
				.containsExactly(new Document("$unionWith", new Document("coll", "coll-1")));
	}

	@Test // DATAMONGO-2622
	void rendersPipelineCorrectly() {

		assertThat(UnionWithOperation.unionWith("coll-1").mapFieldsTo(Warehouse.class)
				.pipeline(Aggregation.project().and("location").as("region")).toPipelineStages(contextFor(Warehouse.class)))
						.containsExactly(new Document("$unionWith", new Document("coll", "coll-1").append("pipeline",
								Arrays.asList(new Document("$project", new Document("region", 1))))));
	}

	@Test // DATAMONGO-2622
	void rendersPipelineCorrectlyForDifferentDomainType() {

		assertThat(UnionWithOperation.unionWith("coll-1").pipeline(Aggregation.project().and("name").as("name"))
				.mapFieldsTo(Supplier.class).toPipelineStages(contextFor(Warehouse.class)))
						.containsExactly(new Document("$unionWith", new Document("coll", "coll-1").append("pipeline",
								Arrays.asList(new Document("$project", new Document("name", "$supplier"))))));
	}

	@Test // DATAMONGO-2622
	void rendersPipelineCorrectlyForUntypedContext() {

		assertThat(UnionWithOperation.unionWith("coll-1").pipeline(Aggregation.project("region"))
				.toPipelineStages(contextFor(null)))
						.containsExactly(new Document("$unionWith", new Document("coll", "coll-1").append("pipeline",
								Arrays.asList(new Document("$project", new Document("region", 1))))));
	}

	@Test // DATAMONGO-2622
	void doesNotMapAgainstFieldsFromAPreviousStage() {

		TypedAggregation<Supplier> agg = TypedAggregation.newAggregation(Supplier.class,
				Aggregation.project().and("name").as("supplier"),
				UnionWithOperation.unionWith("coll-1").pipeline(Aggregation.project().and("name").as("name")));

		List<Document> pipeline = agg.toPipeline(contextFor(Supplier.class));
		assertThat(pipeline).containsExactly(new Document("$project", new Document("supplier", 1)), //
				new Document("$unionWith", new Document("coll", "coll-1").append("pipeline",
						Arrays.asList(new Document("$project", new Document("name", 1))))));
	}

	@Test // DATAMONGO-2622
	void mapAgainstUnionWithDomainTypeEvenWhenInsideTypedAggregation() {

		TypedAggregation<Supplier> agg = TypedAggregation.newAggregation(Supplier.class,
				Aggregation.project().and("name").as("supplier"), UnionWithOperation.unionWith("coll-1")
						.mapFieldsTo(Warehouse.class).pipeline(Aggregation.project().and("location").as("location")));

		List<Document> pipeline = agg.toPipeline(contextFor(Supplier.class));
		assertThat(pipeline).containsExactly(new Document("$project", new Document("supplier", 1)), //
				new Document("$unionWith", new Document("coll", "coll-1").append("pipeline",
						Arrays.asList(new Document("$project", new Document("location", "$region"))))));
	}

	private static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return new TypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter));
	}

	static class Warehouse {

		String name;
		@Field("region") String location;
		String state;
	}

	static class Supplier {

		@Field("supplier") String name;
		String state;
	}
}
