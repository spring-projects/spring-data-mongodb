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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Vector;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.VectorSearch;
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
 */
class VectorSearchDelegateUnitTests {

	MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, new MongoMappingContext());

	@Test
	void shouldConsiderDerivedLimit() throws ReflectiveOperationException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10ByEmbeddingNear", Vector.class, Score.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1));

		VectorSearchDelegate.QueryMetadata query = createQueryMetadata(queryMethod, accessor);

		assertThat(query.query().getLimit()).isEqualTo(10);
		assertThat(query.numCandidates()).isEqualTo(10 * 20);
	}

	@Test
	void shouldNotSetNumCandidates() throws ReflectiveOperationException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10EnnByEmbeddingNear", Vector.class, Score.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1));

		VectorSearchDelegate.QueryMetadata query = createQueryMetadata(queryMethod, accessor);

		assertThat(query.query().getLimit()).isEqualTo(10);
		assertThat(query.numCandidates()).isNull();
	}

	@Test
	void shouldConsiderProvidedLimit() throws ReflectiveOperationException {

		Method method = VectorSearchRepository.class.getMethod("searchTop10ByEmbeddingNear", Vector.class, Score.class,
				Limit.class);

		MongoQueryMethod queryMethod = getMongoQueryMethod(method);
		MongoParametersParameterAccessor accessor = getAccessor(queryMethod, Vector.of(1, 2), Score.of(1), Limit.of(11));

		VectorSearchDelegate.QueryMetadata query = createQueryMetadata(queryMethod, accessor);

		assertThat(query.query().getLimit()).isEqualTo(11);
		assertThat(query.numCandidates()).isEqualTo(11 * 20);
	}

	private VectorSearchDelegate.QueryMetadata createQueryMetadata(MongoQueryMethod queryMethod,
			MongoParametersParameterAccessor accessor) {

		VectorSearchDelegate delegate = new VectorSearchDelegate(queryMethod, converter, ValueExpressionDelegate.create());

		return delegate.createQuery(mock(ValueExpressionEvaluator.class), queryMethod.getResultProcessor(), accessor,
				Object.class, new ParameterBindingDocumentCodec(), mock(ParameterBindingContext.class));
	}

	private MongoQueryMethod getMongoQueryMethod(Method method) {
		RepositoryMetadata metadata = AnnotationRepositoryMetadata.getMetadata(method.getDeclaringClass());
		return new MongoQueryMethod(method, metadata, new SpelAwareProxyProjectionFactory(), converter.getMappingContext());
	}

	@NotNull
	private static MongoParametersParameterAccessor getAccessor(MongoQueryMethod queryMethod, Object... values) {
		return new MongoParametersParameterAccessor(queryMethod, values);
	}

	interface VectorSearchRepository extends Repository<WithVector, String> {

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ANN)
		SearchResults<WithVector> searchTop10ByEmbeddingNear(Vector vector, Score similarity);

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ENN)
		SearchResults<WithVector> searchTop10EnnByEmbeddingNear(Vector vector, Score similarity);

		@VectorSearch(indexName = "cos-index", searchType = VectorSearchOperation.SearchType.ANN)
		SearchResults<WithVector> searchTop10ByEmbeddingNear(Vector vector, Score similarity, Limit limit);

	}

	static class WithVector {

		Vector embedding;
	}
}
