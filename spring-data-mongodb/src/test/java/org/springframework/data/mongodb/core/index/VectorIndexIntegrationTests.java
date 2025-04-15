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

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;
import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import java.util.List;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.VectorIndex.SimilarityFunction;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.test.util.AtlasContainer;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.MongoTestUtils;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.mongodb.ConnectionString;
import com.mongodb.client.AggregateIterable;

/**
 * Integration tests for vector index creation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@Testcontainers(disabledWithoutDocker = true)
class VectorIndexIntegrationTests {

	private static final @Container AtlasContainer atlasLocal = AtlasContainer.bestMatch();

	MongoTestTemplate template = new MongoTestTemplate(cfg -> {
		cfg.configureDatabaseFactory(ctx -> {
			ctx.client(MongoTestUtils.client(new ConnectionString(atlasLocal.getConnectionString())));
		});
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
	void createsSimpleVectorIndex(String similarityFunction) {

		VectorIndex idx = new VectorIndex("vector_index").addVector("plotEmbedding",
				builder -> builder.dimensions(1536).similarity(similarityFunction));

		indexOps.createIndex(idx);

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
	void dropIndex() {

		VectorIndex idx = new VectorIndex("vector_index").addVector("plotEmbedding",
				builder -> builder.dimensions(1536).similarity("cosine"));

		indexOps.createIndex(idx);

		template.awaitIndexCreation(Movie.class, idx.getName());

		indexOps.dropIndex(idx.getName());

		assertThat(readRawIndexInfo(idx.getName())).isNull();
	}

	@Test // GH-4706
	void statusChanges() throws InterruptedException {

		String indexName = "vector_index";
		assertThat(indexOps.status(indexName)).isEqualTo(SearchIndexStatus.DOES_NOT_EXIST);

		VectorIndex idx = new VectorIndex(indexName).addVector("plotEmbedding",
				builder -> builder.dimensions(1536).similarity("cosine"));

		indexOps.createIndex(idx);

		// without synchronization, the container might crash.
		Thread.sleep(500);

		assertThat(indexOps.status(indexName)).isIn(SearchIndexStatus.PENDING, SearchIndexStatus.BUILDING,
				SearchIndexStatus.READY);
	}

	@Test // GH-4706
	void exists() throws InterruptedException {

		String indexName = "vector_index";
		assertThat(indexOps.exists(indexName)).isFalse();

		VectorIndex idx = new VectorIndex(indexName).addVector("plotEmbedding",
				builder -> builder.dimensions(1536).similarity("cosine"));

		indexOps.createIndex(idx);

		// without synchronization, the container might crash.
		Thread.sleep(500);

		assertThat(indexOps.exists(indexName)).isTrue();
	}

	@Test // GH-4706
	void updatesVectorIndex() throws InterruptedException {

		String indexName = "vector_index";
		VectorIndex idx = new VectorIndex(indexName).addVector("plotEmbedding",
				builder -> builder.dimensions(1536).similarity("cosine"));

		indexOps.createIndex(idx);

		// without synchronization, the container might crash.
		Thread.sleep(500);

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

		// updating vector index does currently not work, one needs to delete and recreat
		assertThatRuntimeException().isThrownBy(() -> indexOps.updateIndex(updatedIdx));
	}

	@Test // GH-4706
	void createsVectorIndexWithFilters() throws InterruptedException {

		VectorIndex idx = new VectorIndex("vector_index")
				.addVector("plotEmbedding", builder -> builder.dimensions(1536).cosine()).addFilter("description")
				.addFilter("year");

		indexOps.createIndex(idx);

		// without synchronization, the container might crash.
		Thread.sleep(500);

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

	private @Nullable Document readRawIndexInfo(String name) {

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
