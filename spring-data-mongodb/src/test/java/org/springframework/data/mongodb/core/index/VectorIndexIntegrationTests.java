/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.awaitility.Awaitility.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.VectorIndex.SimilarityFunction;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.test.util.EnableIfVectorSearchAvailable;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.lang.Nullable;

import com.mongodb.client.AggregateIterable;

/**
 * Integration tests for vector index creation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@EnableIfVectorSearchAvailable
class VectorIndexIntegrationTests {

	MongoTestTemplate template = new MongoTestTemplate(cfg -> {
		cfg.configureMappingContext(ctx -> {
			ctx.initialEntitySet(Movie.class);
		});
	});

	SearchIndexOperations indexOps;

	@BeforeEach
	void init() {
		template.createCollection(Movie.class);
		indexOps = template.searchIndexOps(Movie.class);
	}

	@AfterEach
	void cleanup() {

		template.searchIndexOps(Movie.class).dropAllIndexes();
		template.dropCollection(Movie.class);
	}

	@ParameterizedTest // GH-4706
	@ValueSource(strings = { "euclidean", "cosine", "dotProduct" })
	void createsSimpleVectorIndex(String similarityFunction) throws InterruptedException {

		VectorIndex idx = new VectorIndex("vector_index").addVector("plotEmbedding",
				builder -> builder.dimensions(1536).similarity(similarityFunction));

		indexOps.ensureIndex(idx);

		await().untilAsserted(() -> {
			Document raw = readRawIndexInfo(idx.getName());
			assertThat(raw).containsEntry("name", idx.getName()) //
					.containsEntry("type", "vectorSearch") //
					.containsEntry("latestDefinition.fields.[0].type", "vector") //
					.containsEntry("latestDefinition.fields.[0].path", "plot_embedding") //
					.containsEntry("latestDefinition.fields.[0].numDimensions", 1536) //
					.containsEntry("latestDefinition.fields.[0].similarity", similarityFunction); //
		});
	}

	@Test // GH-4706
	void updatesVectorIndex() {

		String indexName = "vector_index";
		VectorIndex idx = new VectorIndex(indexName).addVector("plotEmbedding",
				builder -> builder.dimensions(1536).similarity("cosine"));

		indexOps.ensureIndex(idx);

		await().untilAsserted(() -> {
			Document raw = readRawIndexInfo(idx.getName());
			assertThat(raw).containsEntry("name", idx.getName()) //
					.containsEntry("type", "vectorSearch") //
					.containsEntry("latestDefinition.fields.[0].type", "vector") //
					.containsEntry("latestDefinition.fields.[0].path", "plot_embedding") //
					.containsEntry("latestDefinition.fields.[0].numDimensions", 1536) //
					.containsEntry("latestDefinition.fields.[0].similarity", "cosine"); //
		});

		VectorIndex updatedIdx = new VectorIndex(indexName).addVector("plotEmbedding",
				builder -> builder.dimensions(1536).similarity(SimilarityFunction.DOT_PRODUCT));
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> indexOps.updateIndex(idx));
	}

	@Test // GH-4706
	void createsVectorIndexWithFilters() {

		VectorIndex idx = new VectorIndex("vector_index")
				.addVector("plotEmbedding", builder -> builder.dimensions(1536).cosine()).addFilter("description")
				.addFilter("year");

		indexOps.ensureIndex(idx);

		await().untilAsserted(() -> {
			Document raw = readRawIndexInfo(idx.getName());
			assertThat(raw).containsEntry("name", idx.getName()) //
					.containsEntry("type", "vectorSearch") //
					.containsEntry("latestDefinition.fields.[0].type", "vector") //
					.containsEntry("latestDefinition.fields.[1].type", "filter") //
					.containsEntry("latestDefinition.fields.[1].path", "plot") //
					.containsEntry("latestDefinition.fields.[2].type", "filter") //
					.containsEntry("latestDefinition.fields.[2].path", "year"); //
		});
	}

	@Nullable
	private Document readRawIndexInfo(String name) {

		AggregateIterable<Document> indexes = template.execute(Movie.class, collection -> {
			return collection.aggregate(List.of(new Document("$listSearchIndexes", new Document("name", name))));
		});

		return indexes.first();
	}

	static class Movie {

		@Id String id;
		String title;

		@Field("plot") String description;
		int year;

		@Field("plot_embedding") Double[] plotEmbedding;
	}

}
