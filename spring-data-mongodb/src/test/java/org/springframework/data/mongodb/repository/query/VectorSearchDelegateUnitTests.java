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
package org.springframework.data.mongodb.repository.query;

import static org.mockito.Mockito.mock;
import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.List;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Vector;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.repository.query.VectorSearchDelegate.QueryContainer;
import org.springframework.data.mongodb.util.aggregation.TestAggregationContext;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AnnotationRepositoryMetadata;
import org.springframework.data.repository.query.ValueExpressionDelegate;

/**
 * Unit tests for {@link VectorSearchDelegate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class VectorSearchDelegateUnitTests {

	MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, new MongoMappingContext());

	@Test
	void shouldConsiderDerivedLimit() throws ReflectiveOperationException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10ByEmbeddingNear", Vector.class, Score.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1));

		QueryContainer container = createQueryContainer(queryMethod, accessor);

		assertThat(container.query().getLimit()).isEqualTo(10);
		assertThat(numCandidates(container.pipeline())).isEqualTo(10 * 20);
	}

	@Test
	void shouldNotSetNumCandidates() throws ReflectiveOperationException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10EnnByEmbeddingNear", Vector.class, Score.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1));

		QueryContainer container = createQueryContainer(queryMethod, accessor);

		assertThat(container.query().getLimit()).isEqualTo(10);
		assertThat(numCandidates(container.pipeline())).isNull();
	}

	@Test
	void shouldConsiderProvidedLimit() throws ReflectiveOperationException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10ByEmbeddingNear", Vector.class, Score.class,
				Limit.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1), Limit.of(11));

		QueryContainer container = createQueryContainer(queryMethod, accessor);

		assertThat(container.query().getLimit()).isEqualTo(11);
		assertThat(numCandidates(container.pipeline())).isEqualTo(11 * 20);
	}

	@Test
	void considersDerivedQueryPart() throws ReflectiveOperationException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10ByFirstNameAndEmbeddingNear", String.class,
				Vector.class, Score.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, "spring", Vector.of(1, 2), Score.of(1));

		QueryContainer container = createQueryContainer(queryMethod, accessor);

		assertThat(vectorSearchStageOf(container.pipeline())).containsEntry("$vectorSearch.filter",
				new Document("first_name", "spring"));
	}

	@Test
	void considersDerivedQueryPartInDifferentOrder() throws ReflectiveOperationException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10ByEmbeddingNearAndFirstName", Vector.class,
				Score.class, String.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1), "spring");

		QueryContainer container = createQueryContainer(queryMethod, accessor);

		assertThat(vectorSearchStageOf(container.pipeline())).containsEntry("$vectorSearch.filter",
				new Document("first_name", "spring"));
	}

	@Test
	void defaultSortsByScore() throws NoSuchMethodException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10ByEmbeddingNear", Vector.class, Score.class,
				Limit.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1), Limit.of(10));

		QueryContainer container = createQueryContainer(queryMethod, accessor);

		List<Document> stages = container.pipeline().lastOperation()
				.toPipelineStages(TestAggregationContext.contextFor(WithVector.class));

		assertThat(stages).containsExactly(new Document("$sort", new Document("__score__", -1)));
	}

	@Test
	void usesDerivedSort() throws NoSuchMethodException {

		Method method = VectorSearchRepository.class.getMethod("searchByEmbeddingNearOrderByFirstName", Vector.class,
				Score.class, Limit.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1), Limit.of(11));

		QueryContainer container = createQueryContainer(queryMethod, accessor);
		AggregationPipeline aggregationPipeline = container.pipeline();

		List<Document> stages = aggregationPipeline.lastOperation()
				.toPipelineStages(TestAggregationContext.contextFor(WithVector.class));

		assertThat(stages).containsExactly(new Document("$sort", new Document("first_name", 1).append("__score__", -1)));
	}

	Document vectorSearchStageOf(AggregationPipeline pipeline) {
		return pipeline.firstOperation().toPipelineStages(TestAggregationContext.contextFor(WithVector.class)).get(0);
	}

	private QueryContainer createQueryContainer(MongoQueryMethod queryMethod, MongoParametersParameterAccessor accessor) {

		VectorSearchDelegate delegate = new VectorSearchDelegate(queryMethod, converter, ValueExpressionDelegate.create());

		return delegate.createQuery(mock(ValueExpressionEvaluator.class), queryMethod.getResultProcessor(), accessor, null,
				new ParameterBindingDocumentCodec(), mock(ParameterBindingContext.class));
	}

	private MongoQueryMethod getMongoQueryMethod(Method method) {
		RepositoryMetadata metadata = AnnotationRepositoryMetadata.getMetadata(method.getDeclaringClass());
		return new MongoQueryMethod(method, metadata, new SpelAwareProxyProjectionFactory(), converter.getMappingContext());
	}

	private static MongoParametersParameterAccessor getAccessor(MongoQueryMethod queryMethod, Object... values) {
		return new MongoParametersParameterAccessor(queryMethod, values);
	}

	@Nullable
	private static Integer numCandidates(AggregationPipeline pipeline) {

		Document $vectorSearch = pipeline.firstOperation().toPipelineStages(Aggregation.DEFAULT_CONTEXT).get(0);
		if ($vectorSearch.containsKey("$vectorSearch")) {
			Object value = $vectorSearch.get("$vectorSearch", Document.class).get("numCandidates");
			return value instanceof Number i ? i.intValue() : null;
		}
		return null;
	}

	interface VectorSearchRepository extends Repository<WithVector, String> {

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ANN)
		SearchResults<WithVector> searchTop10ByEmbeddingNear(Vector vector, Score similarity);

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ANN)
		SearchResults<WithVector> searchTop10ByFirstNameAndEmbeddingNear(String firstName, Vector vector, Score similarity);

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ANN)
		SearchResults<WithVector> searchTop10ByEmbeddingNearAndFirstName(Vector vector, Score similarity, String firstname);

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ENN)
		SearchResults<WithVector> searchTop10EnnByEmbeddingNear(Vector vector, Score similarity);

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ANN)
		SearchResults<WithVector> searchTop10ByEmbeddingNear(Vector vector, Score similarity, Limit limit);

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ANN)
		SearchResults<WithVector> searchByEmbeddingNearOrderByFirstName(Vector vector, Score similarity, Limit limit);

	}

	static class WithVector {

		Vector embedding;

		String lastName;

		@Field("first_name") String firstName;

		public Vector getEmbedding() {
			return embedding;
		}

		public void setEmbedding(Vector embedding) {
			this.embedding = embedding;
		}

		public String getLastName() {
			return lastName;
		}

		public void setLastName(String lastName) {
			this.lastName = lastName;
		}

		public String getFirstName() {
			return firstName;
		}

		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}
	}
}
