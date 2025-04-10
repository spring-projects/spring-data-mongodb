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

import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Vector;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ValueExpressionDelegate;

/**
 * Unit tests for {@link VectorSearchAggregation}.
 *
 * @author Mark Paluch
 */
class VectorSearchAggregationUnitTests {

	MongoOperations operationsMock;
	MongoMappingContext context;
	MappingMongoConverter converter;

	@BeforeEach
	public void setUp() {

		context = new MongoMappingContext();
		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		operationsMock = Mockito.mock(MongoOperations.class);

		when(operationsMock.getConverter()).thenReturn(converter);
		when(operationsMock.execute(any())).thenReturn(Bson.DEFAULT_CODEC_REGISTRY);
	}

	@Test
	void derivesPrefilter() throws Exception {

		VectorSearchAggregation aggregation = aggregation(SampleRepository.class, "searchByCountryAndEmbeddingNear",
				String.class, Vector.class, Score.class, Limit.class);

		VectorSearchDelegate.QueryMetadata query = aggregation.createVectorSearchQuery(
				aggregation.getQueryMethod().getResultProcessor(),
				new MongoParametersParameterAccessor(aggregation.getQueryMethod(),
						new Object[] { "de", Vector.of(1f), Score.of(1), Limit.unlimited() }),
				Object.class);

		assertThat(query.query().getQueryObject()).containsEntry("country", "de");
	}

	private VectorSearchAggregation aggregation(Class<?> repository, String name, Class<?>... parameters)
			throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(repository), factory,
				context);
		return new VectorSearchAggregation(queryMethod, operationsMock, ValueExpressionDelegate.create());
	}

	interface SampleRepository extends CrudRepository<WithVectorFields, String> {

		@VectorSearch(indexName = "cos-index")
		SearchResults<WithVectorFields> searchByCountryAndEmbeddingNear(String country, Vector vector, Score similarity,
				Limit limit);

	}

	static class WithVectorFields {

		String id;
		String country;
		String description;

		Vector embedding;

	}

}
