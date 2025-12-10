/*
 * Copyright 2024 the original author or authors.
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

import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.bson.BinaryVector;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.domain.Vector;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation.SearchType;
import org.springframework.data.mongodb.core.index.VectorIndex;
import org.springframework.data.mongodb.core.index.VectorIndex.SimilarityFunction;
import org.springframework.data.mongodb.core.mapping.MongoVector;
import org.springframework.data.mongodb.test.util.AtlasContainer;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Integration tests using Vector Search and Vector Indexes through local MongoDB Atlas.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@Testcontainers(disabledWithoutDocker = true)
@Tag("VectorSearch")
public class VectorSearchTests {

	private static final String SCORE_FIELD = "vector-search-tests";
	private static final @Container AtlasContainer atlasLocal = AtlasContainer.bestMatch();
	private static final String COLLECTION_NAME = "collection-1";

	static MongoClient client;
	static MongoTestTemplate template;

	@BeforeAll
	static void beforeAll() throws InterruptedException {

		client = MongoClients.create(atlasLocal.getConnectionString());
		template = new MongoTestTemplate(client, SCORE_FIELD);

		Thread.sleep(250); // just wait a little or the index will be broken

		initDocuments();
		initIndexes();
	}

	@AfterAll
	static void afterAll() {
		template.dropCollection(WithVectorFields.class);
	}

	@ParameterizedTest // GH-4706
	@MethodSource("vectorAggregations")
	void searchUsingArraysAddingScore(VectorSearchOperation searchOperation) {

		VectorSearchOperation $search = searchOperation.withSearchScore(SCORE_FIELD);

		AggregationResults<Document> results = template.aggregate(Aggregation.newAggregation($search),
				WithVectorFields.class, Document.class);

		assertThat(results).hasSize(10);
		assertScoreIsDecreasing(results);
		assertThat(results.iterator().next()).containsKey(SCORE_FIELD)
				.extracting(it -> it.get(SCORE_FIELD), InstanceOfAssertFactories.DOUBLE).isEqualByComparingTo(1D);
	}

	@ParameterizedTest // GH-4706
	@MethodSource("binaryVectorAggregations")
	void searchUsingBinaryVectorAddingScore(VectorSearchOperation searchOperation) {

		VectorSearchOperation $search = searchOperation.withSearchScore(SCORE_FIELD);

		AggregationResults<Document> results = template.aggregate(Aggregation.newAggregation($search),
				WithVectorFields.class, Document.class);

		assertThat(results).hasSize(10);
		assertScoreIsDecreasing(results);
		assertThat(results.iterator().next()).containsKey(SCORE_FIELD)
				.extracting(it -> it.get(SCORE_FIELD), InstanceOfAssertFactories.DOUBLE).isEqualByComparingTo(1D);
	}

	private static Stream<Arguments> binaryVectorAggregations() {

		return Stream.of(//
				Arguments.arguments(VectorSearchOperation.search("raw-index").path("rawInt8vector") //
						.vector(new byte[] { 0, 1, 2, 3, 4 }) //
						.limit(10)//
						.numCandidates(20) //
						.searchType(SearchType.ANN)),
				Arguments.arguments(VectorSearchOperation.search("wrapper-index").path("int8vector") //
						.vector(BinaryVector.int8Vector(new byte[] { 0, 1, 2, 3, 4 })) //
						.limit(10)//
						.numCandidates(20) //
						.searchType(SearchType.ANN)),
				Arguments.arguments(VectorSearchOperation.search("wrapper-index").path("float32vector") //
						.vector(BinaryVector.floatVector(new float[] { 0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f })) //
						.limit(10)//
						.numCandidates(20) //
						.searchType(SearchType.ANN)));
	}

	private static Stream<Arguments> vectorAggregations() {

		return Stream.of(//
				Arguments.arguments(VectorSearchOperation.search("raw-index").path("rawFloat32vector") //
						.vector(0.0001f, 1.12345f, 2.23456f, 3.34567f, 4.45678f) //
						.limit(10)//
						.numCandidates(20) //
						.searchType(SearchType.ANN)),
				Arguments.arguments(VectorSearchOperation.search("raw-index").path("rawFloat64vector") //
						.vector(1.0001d, 2.12345d, 3.23456d, 4.34567d, 5.45678d) //
						.limit(10)//
						.numCandidates(20) //
						.searchType(SearchType.ANN)),
				Arguments.arguments(VectorSearchOperation.search("wrapper-index").path("float64vector") //
						.vector(Vector.of(1.0001d, 2.12345d, 3.23456d, 4.34567d, 5.45678d)) //
						.limit(10)//
						.numCandidates(20) //
						.searchType(SearchType.ANN)));
	}

	static void initDocuments() {
		IntStream.range(0, 10).mapToObj(WithVectorFields::instance).forEach(template::save);
	}

	static void initIndexes() {

		VectorIndex rawIndex = new VectorIndex("raw-index")
				.addVector("rawInt8vector", it -> it.similarity(SimilarityFunction.COSINE).dimensions(5))
				.addVector("rawFloat32vector", it -> it.similarity(SimilarityFunction.COSINE).dimensions(5))
				.addVector("rawFloat64vector", it -> it.similarity(SimilarityFunction.COSINE).dimensions(5))
				.addFilter("justSomeArgument");

		VectorIndex wrapperIndex = new VectorIndex("wrapper-index")
				.addVector("int8vector", it -> it.similarity(SimilarityFunction.COSINE).dimensions(5))
				.addVector("float32vector", it -> it.similarity(SimilarityFunction.COSINE).dimensions(5))
				.addVector("float64vector", it -> it.similarity(SimilarityFunction.COSINE).dimensions(5))
				.addFilter("justSomeArgument");

		template.searchIndexOps(WithVectorFields.class).createIndex(rawIndex);
		template.searchIndexOps(WithVectorFields.class).createIndex(wrapperIndex);

		template.awaitSearchIndexCreation(WithVectorFields.class, rawIndex.getName());
		template.awaitSearchIndexCreation(WithVectorFields.class, wrapperIndex.getName());
	}

	private static void assertScoreIsDecreasing(Iterable<Document> documents) {

		double previousScore = Integer.MAX_VALUE;
		for (Document document : documents) {

			Double vectorSearchScore = document.getDouble(SCORE_FIELD);
			assertThat(vectorSearchScore).isGreaterThan(0D);
			assertThat(vectorSearchScore).isLessThan(previousScore);
			previousScore = vectorSearchScore;
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document(COLLECTION_NAME)
	static class WithVectorFields {

		String id;

		Vector int8vector;
		Vector float32vector;
		Vector float64vector;

		BinaryVector rawInt8vector;
		float[] rawFloat32vector;
		double[] rawFloat64vector;

		int justSomeArgument;

		static WithVectorFields instance(int offset) {

			WithVectorFields instance = new WithVectorFields();
			instance.id = "id-%s".formatted(offset);
			instance.rawFloat32vector = new float[5];
			instance.rawFloat64vector = new double[5];

			byte[] int8 = new byte[5];
			for (int i = 0; i < 5; i++) {

				int v = i + offset;
				int8[i] = (byte) v;
			}
			instance.rawInt8vector = BinaryVector.int8Vector(int8);

			if (offset == 0) {
				instance.rawFloat32vector[0] = 0.0001f;
				instance.rawFloat64vector[0] = 0.0001d;
			} else {
				instance.rawFloat32vector[0] = Float.parseFloat("%s.000%s".formatted(offset, offset));
				instance.rawFloat64vector[0] = Double.parseDouble("%s.000%s".formatted(offset, offset));
			}
			instance.rawFloat32vector[1] = Float.parseFloat("%s.12345".formatted(offset + 1));
			instance.rawFloat64vector[1] = Double.parseDouble("%s.12345".formatted(offset + 1));
			instance.rawFloat32vector[2] = Float.parseFloat("%s.23456".formatted(offset + 2));
			instance.rawFloat64vector[2] = Double.parseDouble("%s.23456".formatted(offset + 2));
			instance.rawFloat32vector[3] = Float.parseFloat("%s.34567".formatted(offset + 3));
			instance.rawFloat64vector[3] = Double.parseDouble("%s.34567".formatted(offset + 3));
			instance.rawFloat32vector[4] = Float.parseFloat("%s.45678".formatted(offset + 4));
			instance.rawFloat64vector[4] = Double.parseDouble("%s.45678".formatted(offset + 4));

			instance.justSomeArgument = offset;

			instance.int8vector = MongoVector.of(instance.rawInt8vector);
			instance.float32vector = MongoVector.of(BinaryVector.floatVector(instance.rawFloat32vector));
			instance.float64vector = Vector.of(instance.rawFloat64vector);

			return instance;
		}
	}

}
