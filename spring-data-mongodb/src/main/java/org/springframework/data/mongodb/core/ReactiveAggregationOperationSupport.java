/*
 * Copyright 2017-present the original author or authors.
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

import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;

import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link ReactiveAggregationOperation} operating directly on {@link ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 * @autor Christoph Strobl
 * @since 2.0
 */
class ReactiveAggregationOperationSupport implements ReactiveAggregationOperation {

	private final ReactiveMongoTemplate template;

	/**
	 * Create new instance of {@link ReactiveAggregationOperationSupport}.
	 *
	 * @param template must not be {@literal null}.
	 * @throws IllegalArgumentException if template is {@literal null}.
	 */
	ReactiveAggregationOperationSupport(ReactiveMongoTemplate template) {

		Assert.notNull(template, "Template must not be null");

		this.template = template;
	}

	@Override
	public <T> ReactiveAggregation<T> aggregateAndReturn(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveAggregationSupport<>(template, domainType, QueryResultConverter.entity(), null, null);
	}

	static class ReactiveAggregationSupport<S, T>
			implements AggregationOperationWithAggregation<T>, ReactiveAggregation<T>, TerminatingAggregationOperation<T> {

		private final ReactiveMongoTemplate template;
		private final Class<S> domainType;
		private final QueryResultConverter<? super S, ? extends T> resultConverter;
		private final @Nullable Aggregation aggregation;
		private final @Nullable String collection;

		ReactiveAggregationSupport(ReactiveMongoTemplate template, Class<S> domainType,
				QueryResultConverter<? super S, ? extends T> resultConverter, @Nullable Aggregation aggregation,
				@Nullable String collection) {

			this.template = template;
			this.domainType = domainType;
			this.resultConverter = resultConverter;
			this.aggregation = aggregation;
			this.collection = collection;
		}

		@Override
		public AggregationOperationWithAggregation<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty");

			return new ReactiveAggregationSupport<>(template, domainType, resultConverter, aggregation, collection);
		}

		@Override
		public TerminatingAggregationOperation<T> by(Aggregation aggregation) {

			Assert.notNull(aggregation, "Aggregation must not be null");

			return new ReactiveAggregationSupport<>(template, domainType, resultConverter, aggregation, collection);
		}

		@Override
		public <R> TerminatingAggregationOperation<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "QueryResultConverter must not be null");

			return new ReactiveAggregationSupport<>(template, domainType, resultConverter.andThen(converter), aggregation,
					collection);
		}

		@Override
		public Flux<T> all() {

			Assert.notNull(aggregation, "Aggregation must be set first");

			return template.doAggregate(aggregation, getCollectionName(aggregation), domainType, domainType, resultConverter);
		}

		private String getCollectionName(Aggregation aggregation) {

			if (StringUtils.hasText(collection)) {
				return collection;
			}

			if (aggregation instanceof TypedAggregation typedAggregation) {

				if (typedAggregation.getInputType() != null) {
					return template.getCollectionName(typedAggregation.getInputType());
				}
			}

			return template.getCollectionName(domainType);
		}
	}
}
