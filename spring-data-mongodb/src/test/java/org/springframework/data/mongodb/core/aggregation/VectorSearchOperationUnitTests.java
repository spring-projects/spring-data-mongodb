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

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation.SearchType;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.util.aggregation.TestAggregationContext;

/**
 * Unit tests for {@link VectorSearchOperation}.
 *
 * @author Christoph Strobl
 */
class VectorSearchOperationUnitTests {

	static final Document $VECTOR_SEARCH = Document.parse(
			"{'index' : 'vector_index', 'limit' : 10, 'path' : 'plot_embedding', 'queryVector' : [-0.0016261312, -0.028070757, -0.011342932]}");
	static final VectorSearchOperation SEARCH_OPERATION = VectorSearchOperation.search("vector_index")
			.path("plot_embedding").vector(-0.0016261312, -0.028070757, -0.011342932).limit(10);

	@Test // GH-4706
	void requiredArgs() {

		List<Document> stages = SEARCH_OPERATION.toPipelineStages(Aggregation.DEFAULT_CONTEXT);
		assertThat(stages).containsExactly(new Document("$vectorSearch", $VECTOR_SEARCH));
	}

	@Test // GH-4706
	void optionalArgs() {

		VectorSearchOperation $search = SEARCH_OPERATION.numCandidates(150).searchType(SearchType.ENN)
				.filter(new Criteria().andOperator(Criteria.where("year").gt(1955), Criteria.where("year").lt(1975)));

		List<Document> stages = $search.toPipelineStages(Aggregation.DEFAULT_CONTEXT);

		Document filter = new Document("$and",
				List.of(new Document("year", new Document("$gt", 1955)), new Document("year", new Document("$lt", 1975))));
		assertThat(stages).containsExactly(new Document("$vectorSearch",
				new Document($VECTOR_SEARCH).append("exact", true).append("filter", filter).append("numCandidates", 150)));
	}

	@Test // GH-4706
	void withScore() {

		List<Document> stages = SEARCH_OPERATION.withSearchScore().toPipelineStages(Aggregation.DEFAULT_CONTEXT);
		assertThat(stages).containsExactly(new Document("$vectorSearch", $VECTOR_SEARCH),
				new Document("$addFields", new Document("score", new Document("$meta", "vectorSearchScore"))));
	}

	@Test // GH-4706
	void withScoreFilter() {

		List<Document> stages = SEARCH_OPERATION.withFilterBySore(score -> score.gt(50))
				.toPipelineStages(Aggregation.DEFAULT_CONTEXT);
		assertThat(stages).containsExactly(new Document("$vectorSearch", $VECTOR_SEARCH),
				new Document("$addFields", new Document("score", new Document("$meta", "vectorSearchScore"))),
				new Document("$match", new Document("score", new Document("$gt", 50))));
	}

	@Test // GH-4706
	void withScoreFilterOnCustomFieldName() {

		List<Document> stages = SEARCH_OPERATION.withFilterBySore(score -> score.gt(50)).withSearchScore("s-c-o-r-e")
				.toPipelineStages(Aggregation.DEFAULT_CONTEXT);
		assertThat(stages).containsExactly(new Document("$vectorSearch", $VECTOR_SEARCH),
				new Document("$addFields", new Document("s-c-o-r-e", new Document("$meta", "vectorSearchScore"))),
				new Document("$match", new Document("s-c-o-r-e", new Document("$gt", 50))));
	}

	@Test // GH-4706
	void mapsCriteriaToDomainType() {

		VectorSearchOperation $search = SEARCH_OPERATION
				.filter(new Criteria().andOperator(Criteria.where("y").gt(1955), Criteria.where("y").lt(1975)));

		List<Document> stages = $search.toPipelineStages(TestAggregationContext.contextFor(Movie.class));

		Document filter = new Document("$and",
				List.of(new Document("year", new Document("$gt", 1955)), new Document("year", new Document("$lt", 1975))));
		assertThat(stages)
				.containsExactly(new Document("$vectorSearch", new Document($VECTOR_SEARCH).append("filter", filter)));
	}

	static class Movie {

		@Id String id;
		String title;

		@Field("year") String y;
	}

}
