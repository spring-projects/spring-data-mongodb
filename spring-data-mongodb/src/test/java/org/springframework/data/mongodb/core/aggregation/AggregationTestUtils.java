/*
 * Copyright 2023. the original author or authors.
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

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.test.util.MongoTestMappingContext;

/**
 * @author Christoph Strobl
 */
public final class AggregationTestUtils {

	public static AggregationContextBuilder<TypeBasedAggregationOperationContext> strict(Class<?> type) {

		AggregationContextBuilder<AggregationOperationContext> builder = new AggregationContextBuilder<>();
		builder.strict = true;
		return builder.forType(type);
	}

	public static AggregationContextBuilder<TypeBasedAggregationOperationContext> relaxed(Class<?> type) {

		AggregationContextBuilder<AggregationOperationContext> builder = new AggregationContextBuilder<>();
		builder.strict = false;
		return builder.forType(type);
	}

	public static class AggregationContextBuilder<T extends AggregationOperationContext> {

		Class<?> targetType;
		MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
		QueryMapper queryMapper;
		boolean strict;

		public AggregationContextBuilder<TypeBasedAggregationOperationContext> forType(Class<?> type) {

			this.targetType = type;
			return (AggregationContextBuilder<TypeBasedAggregationOperationContext>) this;
		}

		public AggregationContextBuilder<T> using(
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

			this.mappingContext = mappingContext;
			return this;
		}

		public AggregationContextBuilder<T> using(QueryMapper queryMapper) {

			this.queryMapper = queryMapper;
			return this;
		}

		public T ctx() {
			//
			if (targetType == null) {
				return (T) Aggregation.DEFAULT_CONTEXT;
			}

			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> ctx = mappingContext != null
					? mappingContext
					: MongoTestMappingContext.newTestContext().init();
			QueryMapper qm = queryMapper != null ? queryMapper
					: new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, ctx));
			return (T) (strict ? new TypeBasedAggregationOperationContext(targetType, ctx, qm)
					: new RelaxedTypeBasedAggregationOperationContext(targetType, ctx, qm));
		}
	}
}
