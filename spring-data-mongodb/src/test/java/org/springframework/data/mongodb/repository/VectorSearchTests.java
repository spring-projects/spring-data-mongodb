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
package org.springframework.data.mongodb.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.SearchResult;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Similarity;
import org.springframework.data.domain.Vector;
import org.springframework.data.mongodb.core.TestMongoConfiguration;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;
import org.springframework.data.mongodb.core.index.VectorIndex;
import org.springframework.data.mongodb.core.index.VectorIndex.SimilarityFunction;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.mongodb.test.util.AtlasContainer;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBAtlasLocalContainer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Integration tests using Vector Search and Vector Indexes through local MongoDB Atlas.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringJUnitConfig(classes = { VectorSearchTests.Config.class })
public class VectorSearchTests {

	Vector VECTOR = Vector.of(0.2001f, 0.32345f, 0.43456f, 0.54567f, 0.65678f);

	private static final MongoDBAtlasLocalContainer atlasLocal = AtlasContainer.bestMatch().withReuse(true);
	private static final String COLLECTION_NAME = "collection-1";

	static MongoClient client;
	static MongoTestTemplate template;

	@Autowired VectorSearchRepository repository;

	@EnableMongoRepositories(
			includeFilters = {
					@ComponentScan.Filter(value = VectorSearchRepository.class, type = FilterType.ASSIGNABLE_TYPE) },
			considerNestedRepositories = true)
	static class Config extends TestMongoConfiguration {

		@Override
		public String getDatabaseName() {
			return "vector-search-tests";
		}

		@Override
		public MongoClient mongoClient() {
			atlasLocal.start();
			return MongoClients.create(atlasLocal.getConnectionString());
		}
	}

	@BeforeAll
	static void beforeAll() throws InterruptedException {
		atlasLocal.start();

		System.out.println(atlasLocal.getConnectionString());
		client = MongoClients.create(atlasLocal.getConnectionString());
		template = new MongoTestTemplate(client, "vector-search-tests");

		template.remove(WithVectorFields.class).all();
		initDocuments();
		initIndexes();

		Thread.sleep(500); // just wait a little or the index will be broken
	}

	@Test
	void shouldSearchEnnWithAnnotatedFilter() {

		SearchResults<WithVectorFields> results = repository.searchAnnotated("de", VECTOR,
				Score.of(0.4), Limit.of(10));

		assertThat(results).hasSize(3);
	}

	@Test
	void shouldSearchEnnWithDerivedFilter() {

		SearchResults<WithVectorFields> results = repository.searchByCountryAndEmbeddingNear("de", VECTOR, Score.of(0.98),
				Limit.of(10));

		assertThat(results).hasSize(2).extracting(SearchResult::getContent).extracting(WithVectorFields::getCountry)
				.containsOnly("de", "de");

		assertThat(results).extracting(SearchResult::getContent).extracting(WithVectorFields::getDescription)
				.containsExactlyInAnyOrder("two", "one");
	}

	@Test
	void shouldSearchEnnWithDerivedFilterWithoutScore() {

		SearchResults<WithVectorFields> de = repository.searchByCountryAndEmbeddingNear("de", VECTOR, Similarity.of(0.4),
				Limit.of(10));
		assertThat(de).hasSizeGreaterThanOrEqualTo(2);

		assertThat(repository.searchByCountryAndEmbeddingNear("de", VECTOR, Similarity.of(0.999), Limit.of(10))).hasSize(1);
	}

	@Test
	void shouldSearchEuclideanWithDerivedFilter() {

		SearchResults<WithVectorFields> results = repository.searchEuclideanByCountryAndEmbeddingNear("de", VECTOR,
				Limit.of(2));

		assertThat(results).hasSize(2).extracting(SearchResult::getContent).extracting(WithVectorFields::getCountry)
				.containsOnly("de", "de");

		assertThat(results).extracting(SearchResult::getContent).extracting(WithVectorFields::getDescription)
				.containsExactlyInAnyOrder("two", "one");
	}

	@Test
	void shouldSearchEnnWithDerivedFilterWithin() {

		SearchResults<WithVectorFields> results = repository.searchByCountryAndEmbeddingWithin("de", VECTOR,
				Score.between(0.93, 0.98));

		assertThat(results).hasSize(1);
		for (SearchResult<WithVectorFields> result : results) {
			assertThat(result.getScore().getValue()).isBetween(0.93, 0.98);
		}
	}

	@Test
	void shouldSearchEnnWithDerivedAndLimitedFilterWithin() {

		SearchResults<WithVectorFields> results = repository.searchTop1ByCountryAndEmbeddingWithin("de", VECTOR,
				Score.between(0.8, 1));

		assertThat(results).hasSize(1);

		for (SearchResult<WithVectorFields> result : results) {
			assertThat(result.getScore().getValue()).isBetween(0.8, 1.0);
		}
	}

	static void initDocuments() {

		WithVectorFields w1 = new WithVectorFields("de", "one", Vector.of(0.1001f, 0.22345f, 0.33456f, 0.44567f, 0.55678f));
		WithVectorFields w2 = new WithVectorFields("de", "two", Vector.of(0.2001f, 0.32345f, 0.43456f, 0.54567f, 0.65678f));
		WithVectorFields w3 = new WithVectorFields("en", "three",
				Vector.of(0.9001f, 0.82345f, 0.73456f, 0.64567f, 0.55678f));
		WithVectorFields w4 = new WithVectorFields("de", "four",
				Vector.of(0.9001f, 0.92345f, 0.93456f, 0.94567f, 0.95678f));

		template.insertAll(List.of(w1, w2, w3, w4));
	}

	static void initIndexes() {

		VectorIndex cosIndex = new VectorIndex("cos-index")
				.addVector("embedding", it -> it.similarity(SimilarityFunction.COSINE).dimensions(5)).addFilter("country");

		template.searchIndexOps(WithVectorFields.class).createIndex(cosIndex);

		VectorIndex euclideanIndex = new VectorIndex("euc-index")
				.addVector("embedding", it -> it.similarity(SimilarityFunction.EUCLIDEAN).dimensions(5)).addFilter("country");

		template.searchIndexOps(WithVectorFields.class).createIndex(cosIndex);
		template.searchIndexOps(WithVectorFields.class).createIndex(euclideanIndex);
		template.awaitIndexCreation(WithVectorFields.class, cosIndex.getName());
		template.awaitIndexCreation(WithVectorFields.class, euclideanIndex.getName());
	}

	interface VectorSearchRepository extends CrudRepository<WithVectorFields, String> {

		@VectorSearch(indexName = "cos-index", filter = "{country: ?0}", numCandidates = "#{10+10}",
				searchType = VectorSearchOperation.SearchType.ANN)
		SearchResults<WithVectorFields> searchAnnotated(String country, Vector vector,
				Score distance, Limit limit);

		@VectorSearch(indexName = "cos-index")
		SearchResults<WithVectorFields> searchByCountryAndEmbeddingNear(String country, Vector vector, Score similarity,
				Limit limit);

		@VectorSearch(indexName = "cos-index")
		SearchResults<WithVectorFields> searchByCountryAndEmbeddingNear(String country, Vector vector, Limit limit);

		@VectorSearch(indexName = "euc-index")
		SearchResults<WithVectorFields> searchEuclideanByCountryAndEmbeddingNear(String country, Vector vector,
				Limit limit);

		@VectorSearch(indexName = "cos-index", limit = "10")
		SearchResults<WithVectorFields> searchByCountryAndEmbeddingWithin(String country, Vector vector,
				Range<Score> distance);

		@VectorSearch(indexName = "cos-index")
		SearchResults<WithVectorFields> searchTop1ByCountryAndEmbeddingWithin(String country, Vector vector,
				Range<Score> distance);

	}

	@org.springframework.data.mongodb.core.mapping.Document(COLLECTION_NAME)
	static class WithVectorFields {

		String id;
		String country;
		String description;

		Vector embedding;

		public WithVectorFields(String country, String description, Vector embedding) {
			this.country = country;
			this.description = description;
			this.embedding = embedding;
		}

		public String getId() {
			return id;
		}

		public String getCountry() {
			return country;
		}

		public String getDescription() {
			return description;
		}

		public Vector getEmbedding() {
			return embedding;
		}

		@Override
		public String toString() {
			return "WithVectorFields{" + "id='" + id + '\'' + ", country='" + country + '\'' + ", description='" + description
					+ '\'' + '}';
		}
	}

}
