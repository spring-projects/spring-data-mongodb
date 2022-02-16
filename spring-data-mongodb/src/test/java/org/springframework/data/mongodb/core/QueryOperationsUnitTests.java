/*
 * Copyright 2021-2022 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.QueryOperations.AggregationDefinition;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.RelaxedTypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Unit tests for {@link QueryOperations}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class QueryOperationsUnitTests {

	static final AggregationOptions NO_MAPPING = AggregationOptions.builder().noMapping().build();
	static final AggregationOptions STRICT_MAPPING = AggregationOptions.builder().strictMapping().build();

	@Mock QueryMapper queryMapper;
	@Mock UpdateMapper updateMapper;
	@Mock EntityOperations entityOperations;
	@Mock PropertyOperations propertyOperations;
	@Mock MongoDatabaseFactory mongoDbFactory;
	@Mock MongoMappingContext mappingContext;

	QueryOperations queryOperations;

	@BeforeEach
	void beforeEach() {

		when(queryMapper.getMappingContext()).thenReturn((MappingContext) mappingContext);

		queryOperations = new QueryOperations(queryMapper, updateMapper, entityOperations, propertyOperations,
				mongoDbFactory);
	}

	@Test // GH-3542
	void createAggregationContextUsesRelaxedOneForUntypedAggregationsWhenNoInputTypeProvided() {

		Aggregation aggregation = Aggregation.newAggregation(Aggregation.project("name"));
		AggregationDefinition ctx = queryOperations.createAggregation(aggregation, (Class<?>) null);

		assertThat(ctx.getAggregationOperationContext()).isInstanceOf(RelaxedTypeBasedAggregationOperationContext.class);
	}

	@Test // GH-3542
	void createAggregationContextUsesRelaxedOneForTypedAggregationsWhenNoInputTypeProvided() {

		Aggregation aggregation = Aggregation.newAggregation(Person.class, Aggregation.project("name"));
		AggregationDefinition ctx = queryOperations.createAggregation(aggregation, (Class<?>) null);

		assertThat(ctx.getAggregationOperationContext()).isInstanceOf(RelaxedTypeBasedAggregationOperationContext.class);
	}

	@Test // GH-3542
	void createAggregationContextUsesRelaxedOneForUntypedAggregationsWhenInputTypeProvided() {

		Aggregation aggregation = Aggregation.newAggregation(Aggregation.project("name"));
		AggregationDefinition ctx = queryOperations.createAggregation(aggregation, Person.class);

		assertThat(ctx.getAggregationOperationContext()).isInstanceOf(RelaxedTypeBasedAggregationOperationContext.class);
	}

	@Test // GH-3542
	void createAggregationContextUsesDefaultIfNoMappingDesired() {

		Aggregation aggregation = Aggregation.newAggregation(Aggregation.project("name")).withOptions(NO_MAPPING);
		AggregationDefinition ctx = queryOperations.createAggregation(aggregation, Person.class);

		assertThat(ctx.getAggregationOperationContext()).isEqualTo(Aggregation.DEFAULT_CONTEXT);
	}

	@Test // GH-3542
	void createAggregationContextUsesStrictlyTypedContextForTypedAggregationsWhenRequested() {

		Aggregation aggregation = Aggregation.newAggregation(Person.class, Aggregation.project("name"))
				.withOptions(STRICT_MAPPING);
		AggregationDefinition ctx = queryOperations.createAggregation(aggregation, (Class<?>) null);

		assertThat(ctx.getAggregationOperationContext()).isInstanceOf(TypeBasedAggregationOperationContext.class);
	}

	static class Person {

	}
}
