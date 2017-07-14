/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Flux;

import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link ExecutableAggregationOperation} operating directly on {@link ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
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

		Assert.notNull(template, "Template must not be null!");

		this.template = template;
	}

	@Override
	public <T> ReactiveAggregation<T> aggregateAndReturn(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ReactiveAggregationSupport<>(template, domainType, null, null);
	}

	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ReactiveAggregationSupport<T>
			implements AggregationOperationWithAggregation<T>, ReactiveAggregation<T>, TerminatingAggregationOperation<T> {

		@NonNull ReactiveMongoTemplate template;
		@NonNull Class<T> domainType;
		Aggregation aggregation;
		String collection;

		@Override
		public AggregationOperationWithAggregation<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");

			return new ReactiveAggregationSupport<>(template, domainType, aggregation, collection);
		}

		@Override
		public TerminatingAggregationOperation<T> by(Aggregation aggregation) {

			Assert.notNull(aggregation, "Aggregation must not be null!");

			return new ReactiveAggregationSupport<>(template, domainType, aggregation, collection);
		}

		@Override
		public Flux<T> all() {
			return template.aggregate(aggregation, getCollectionName(aggregation), domainType);
		}

		private String getCollectionName(Aggregation aggregation) {

			if (StringUtils.hasText(collection)) {
				return collection;
			}

			if (aggregation instanceof TypedAggregation) {

				TypedAggregation<?> typedAggregation = (TypedAggregation<?>) aggregation;

				if (typedAggregation.getInputType() != null) {
					return template.determineCollectionName(typedAggregation.getInputType());
				}
			}

			return template.determineCollectionName(domainType);
		}
	}
}
