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

import org.bson.Document;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.reactivestreams.client.FindPublisher;

/**
 * Implementation of {@link ReactiveFindOperation}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class ReactiveFindOperationSupport implements ReactiveFindOperation {

	private static final Query ALL_QUERY = new Query();

	private final @NonNull ReactiveMongoTemplate template;

	@Override
	public <T> ReactiveFind<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ReactiveFindSupport<>(template, domainType, domainType, null, ALL_QUERY);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ReactiveFindSupport<T>
			implements ReactiveFind<T>, FindWithCollection<T>, FindWithProjection<T>, FindWithQuery<T> {

		@NonNull ReactiveMongoTemplate template;
		@NonNull Class<?> domainType;
		Class<T> returnType;
		String collection;
		Query query;

		@Override
		public FindWithProjection<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty!");

			return new ReactiveFindSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public <T1> FindWithQuery<T1> as(Class<T1> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null!");

			return new ReactiveFindSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public TerminatingFind<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ReactiveFindSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public Mono<T> first() {

			FindPublisherPreparer preparer = getCursorPreparer(query);
			Flux<T> result = doFind(new FindPublisherPreparer() {
				@Override
				public <D> FindPublisher<D> prepare(FindPublisher<D> publisher) {
					return preparer.prepare(publisher).limit(1);
				}
			});

			return result.next();
		}

		@Override
		public Mono<T> one() {

			FindPublisherPreparer preparer = getCursorPreparer(query);
			Flux<T> result = doFind(new FindPublisherPreparer() {
				@Override
				public <D> FindPublisher<D> prepare(FindPublisher<D> publisher) {
					return preparer.prepare(publisher).limit(2);
				}
			});

			return result.collectList().flatMap(it -> {

				if (it.isEmpty()) {
					return Mono.empty();
				}

				if (it.size() > 1) {
					return Mono.error(
							new IncorrectResultSizeDataAccessException("Query " + asString() + " returned non unique result.", 1));
				}

				return Mono.just(it.get(0));
			});
		}

		@Override
		public Flux<T> all() {
			return doFind(null);
		}

		@Override
		public TerminatingFindNear<T> near(NearQuery nearQuery) {
			return () -> template.geoNear(nearQuery, domainType, getCollectionName(), returnType);
		}

		@Override
		public Mono<Long> count() {
			return template.count(query, domainType, getCollectionName());
		}

		@Override
		public Mono<Boolean> exists() {
			return template.exists(query, domainType, getCollectionName());
		}

		private Flux<T> doFind(FindPublisherPreparer preparer) {

			Document queryObject = query.getQueryObject();
			Document fieldsObject = query.getFieldsObject();

			return template.doFind(getCollectionName(), queryObject, fieldsObject, domainType, returnType,
					preparer != null ? preparer : getCursorPreparer(query));
		}

		private FindPublisherPreparer getCursorPreparer(Query query) {
			return template.new QueryFindPublisherPreparer(query, domainType);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.determineCollectionName(domainType);
		}

		private String asString() {
			return SerializationUtils.serializeToJsonSafely(query);
		}
	}
}
