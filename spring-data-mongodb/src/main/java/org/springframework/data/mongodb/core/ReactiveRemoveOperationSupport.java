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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.result.DeleteResult;

/**
 * Implementation of {@link ReactiveRemoveOperation}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class ReactiveRemoveOperationSupport implements ReactiveRemoveOperation {

	private static final Query ALL_QUERY = new Query();

	private final ReactiveMongoTemplate template;

	ReactiveRemoveOperationSupport(ReactiveMongoTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveRemove<T> remove(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveRemoveSupport<>(template, domainType, ALL_QUERY, null, QueryResultConverter.entity());
	}

	static class ReactiveRemoveSupport<S, T> implements ReactiveRemove<T>, RemoveWithCollection<T> {

		private final ReactiveMongoTemplate template;
		private final Class<S> domainType;
		private final Query query;
		private final @Nullable String collection;
		private final QueryResultConverter<? super S, ? extends T> resultConverter;

		ReactiveRemoveSupport(ReactiveMongoTemplate template, Class<S> domainType, Query query, @Nullable String collection,
				QueryResultConverter<? super S, ? extends T> resultConverter) {

			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.collection = collection;
			this.resultConverter = resultConverter;
		}

		@Override
		public RemoveWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty");

			return new ReactiveRemoveSupport<>(template, domainType, query, collection, resultConverter);
		}

		@Override
		public TerminatingRemove<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveRemoveSupport<>(template, domainType, query, collection, resultConverter);
		}

		@Override
		public Mono<DeleteResult> all() {

			String collectionName = getCollectionName();

			return template.doRemove(collectionName, query, domainType);
		}

		@Override
		public Flux<T> findAndRemove() {

			String collectionName = getCollectionName();

			return template.doFindAndDelete(collectionName, query, domainType, resultConverter);
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {
			return new ReactiveRemoveSupport<>(template, (Class) domainType, query, collection, converter);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		}

	}
}
