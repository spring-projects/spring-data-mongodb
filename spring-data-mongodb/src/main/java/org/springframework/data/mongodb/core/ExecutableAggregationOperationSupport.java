/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link ExecutableAggregationOperation} operating directly on {@link MongoTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class ExecutableAggregationOperationSupport implements ExecutableAggregationOperation {

	private final MongoTemplate template;

	ExecutableAggregationOperationSupport(MongoTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableAggregation<T> aggregateAndReturn(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ExecutableAggregationSupport<>(template, domainType, QueryResultConverter.entity(), null, null);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class ExecutableAggregationSupport<S, T>
			implements AggregationWithAggregation<T>, ExecutableAggregation<T>, TerminatingAggregation<T> {

		private final MongoTemplate template;
		private final Class<S> domainType;
		private final QueryResultConverter<? super S, ? extends T> resultConverter;
		private final @Nullable Aggregation aggregation;
		private final @Nullable String collection;

		public ExecutableAggregationSupport(MongoTemplate template, Class<S> domainType,
				QueryResultConverter<? super S, ? extends T> resultConverter, @Nullable Aggregation aggregation,
				@Nullable String collection) {
			this.template = template;
			this.domainType = domainType;
			this.resultConverter = resultConverter;
			this.aggregation = aggregation;
			this.collection = collection;
		}

		@Override
		public AggregationWithAggregation<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty");

			return new ExecutableAggregationSupport<>(template, domainType, resultConverter, aggregation, collection);
		}

		@Override
		public TerminatingAggregation<T> by(Aggregation aggregation) {

			Assert.notNull(aggregation, "Aggregation must not be null");

			return new ExecutableAggregationSupport<>(template, domainType, resultConverter, aggregation, collection);
		}

		@Override
		public <R> TerminatingAggregation<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "QueryResultConverter must not be null");

			return new ExecutableAggregationSupport<>(template, domainType, this.resultConverter.andThen(converter),
					aggregation, collection);
		}

		@Override
		public AggregationResults<T> all() {

			Assert.notNull(aggregation, "Aggregation must be set first");
			return template.doAggregate(aggregation, getCollectionName(aggregation), domainType, resultConverter);
		}

		@Override
		public Stream<T> stream() {

			Assert.notNull(aggregation, "Aggregation must be set first");
			return template.doAggregateStream(aggregation, getCollectionName(aggregation), domainType, resultConverter, null);
		}

		private String getCollectionName(@Nullable Aggregation aggregation) {

			if (StringUtils.hasText(collection)) {
				return collection;
			}

			if (aggregation instanceof TypedAggregation<?> typedAggregation) {

				if (typedAggregation.getInputType() != null) {
					return template.getCollectionName(typedAggregation.getInputType());
				}
			}

			return template.getCollectionName(domainType);
		}
	}
}
