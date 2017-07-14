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
import reactor.core.publisher.Mono;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.result.DeleteResult;

/**
 * Implementation of {@link ReactiveRemoveOperation}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class ReactiveRemoveOperationSupport implements ReactiveRemoveOperation {

	private static final Query ALL_QUERY = new Query();

	private final @NonNull ReactiveMongoTemplate tempate;

	@Override
	public <T> ReactiveRemove<T> remove(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ReactiveRemoveSupport<>(tempate, domainType, null, null);
	}

	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ReactiveRemoveSupport<T> implements ReactiveRemove<T>, RemoveWithCollection<T> {

		@NonNull ReactiveMongoTemplate template;
		@NonNull Class<T> domainType;
		Query query;
		String collection;

		@Override
		public RemoveWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");

			return new ReactiveRemoveSupport<>(template, domainType, query, collection);
		}

		@Override
		public TerminatingRemove<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ReactiveRemoveSupport<>(template, domainType, query, collection);
		}

		@Override
		public Mono<DeleteResult> all() {

			String collectionName = getCollectionName();

			return template.doRemove(collectionName, getQuery(), domainType);
		}

		@Override
		public Flux<T> findAndRemove() {

			String collectionName = getCollectionName();

			return template.doFindAndDelete(collectionName, getQuery(), domainType);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.determineCollectionName(domainType);
		}

		private Query getQuery() {
			return query != null ? query : ALL_QUERY;
		}
	}
}
