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

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Window;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.mongodb.core.CollectionPreparerSupport.ReactiveCollectionPreparerDelegate;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link ReactiveFindOperation}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class ReactiveFindOperationSupport implements ReactiveFindOperation {

	private static final Query ALL_QUERY = new Query();

	private final ReactiveMongoTemplate template;

	ReactiveFindOperationSupport(ReactiveMongoTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFind<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveFindSupport<>(template, domainType, domainType, QueryResultConverter.entity(), null, ALL_QUERY);
	}

	/**
	 * @param <T>
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class ReactiveFindSupport<S, T>
			implements ReactiveFind<T>, FindWithCollection<T>, FindWithProjection<T>, FindWithQuery<T> {

		private final ReactiveMongoTemplate template;
		private final Class<?> domainType;
		private final Class<S> returnType;
		private final QueryResultConverter<? super S, ? extends T> resultConverter;
		private final @Nullable String collection;
		private final Query query;

		ReactiveFindSupport(ReactiveMongoTemplate template, Class<?> domainType, Class<S> returnType,
				QueryResultConverter<? super S, ? extends T> resultConverter, @Nullable String collection,
				Query query) {

			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.resultConverter = resultConverter;
			this.collection = collection;
			this.query = query;
		}

		@Override
		public FindWithProjection<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty");

			return new ReactiveFindSupport<>(template, domainType, returnType, resultConverter, collection, query);
		}

		@Override
		public <T1> FindWithQuery<T1> as(Class<T1> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null");

			return new ReactiveFindSupport<>(template, domainType, returnType, QueryResultConverter.entity(), collection,
					query);
		}

		@Override
		public TerminatingFind<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveFindSupport<>(template, domainType, returnType, resultConverter, collection, query);
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "QueryResultConverter must not be null");

			return new ReactiveFindSupport<>(template, domainType, returnType, this.resultConverter.andThen(converter),
					collection, query);
		}

		@Override
		public Mono<T> first() {

			FindPublisherPreparer preparer = getCursorPreparer(query);
			Flux<T> result = doFind(publisher -> preparer.prepare(publisher).limit(1));

			return result.next();
		}

		@Override
		public Mono<T> one() {

			FindPublisherPreparer preparer = getCursorPreparer(query);
			Flux<T> result = doFind(publisher -> preparer.prepare(publisher).limit(2));

			return result.collectList().flatMap(it -> {

				if (it.isEmpty()) {
					return Mono.empty();
				}

				if (it.size() > 1) {
					return Mono.error(
							new IncorrectResultSizeDataAccessException("Query " + asString() + " returned non unique result", 1));
				}

				return Mono.just(it.get(0));
			});
		}

		@Override
		public Flux<T> all() {
			return doFind(null);
		}

		@Override
		public Mono<Window<T>> scroll(ScrollPosition scrollPosition) {
			return template.doScroll(query.with(scrollPosition), domainType, returnType, resultConverter,
					getCollectionName());
		}

		@Override
		public Flux<T> tail() {
			return doFind(template.new TailingQueryFindPublisherPreparer(query, domainType));
		}

		@Override
		public TerminatingFindNear<T> near(NearQuery nearQuery) {
			return new TerminatingFindNearSupport<>(nearQuery, resultConverter);
		}

		@Override
		public Mono<Long> count() {
			return template.count(query, domainType, getCollectionName());
		}

		@Override
		public Mono<Boolean> exists() {
			return template.exists(query, domainType, getCollectionName());
		}

		@Override
		public TerminatingDistinct<Object> distinct(String field) {

			Assert.notNull(field, "Field must not be null");

			return new DistinctOperationSupport<>(this, field);
		}

		private Flux<T> doFind(@Nullable FindPublisherPreparer preparer) {

			Document queryObject = query.getQueryObject();
			Document fieldsObject = query.getFieldsObject();

			return template.doFind(getCollectionName(), ReactiveCollectionPreparerDelegate.of(query), queryObject,
					fieldsObject, domainType, returnType, resultConverter,
					preparer != null ? preparer : getCursorPreparer(query));
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private Flux<T> doFindDistinct(String field) {

			return template.findDistinct(query, field, getCollectionName(), domainType,
					returnType == domainType ? (Class) Object.class : returnType);
		}

		private FindPublisherPreparer getCursorPreparer(Query query) {
			return template.new QueryFindPublisherPreparer(query, domainType);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		}

		private String asString() {
			return SerializationUtils.serializeToJsonSafely(query);
		}

		class TerminatingFindNearSupport<G> implements TerminatingFindNear<G> {

			private final NearQuery nearQuery;
			private final QueryResultConverter<? super S, ? extends G> resultConverter;

			public TerminatingFindNearSupport(NearQuery nearQuery,
					QueryResultConverter<? super S, ? extends G> resultConverter) {
				this.nearQuery = nearQuery;
				this.resultConverter = resultConverter;
			}

			@Override
			public <R> TerminatingFindNear<R> map(QueryResultConverter<? super G, ? extends R> converter) {

				Assert.notNull(converter, "QueryResultConverter must not be null");

				return new TerminatingFindNearSupport<>(nearQuery, this.resultConverter.andThen(converter));
			}

			@Override
			public Flux<GeoResult<G>> all() {
				return template.doGeoNear(nearQuery, domainType, getCollectionName(), returnType, resultConverter);
			}

			@Override
			public Mono<Long> count() {
				return template.doGeoNearCount(nearQuery, domainType, getCollectionName());
			}
		}

		/**
		 * @author Christoph Strobl
		 * @since 2.1
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		static class DistinctOperationSupport<T> implements TerminatingDistinct<T> {

			private final String field;
			private final ReactiveFindSupport delegate;

			public DistinctOperationSupport(ReactiveFindSupport delegate, String field) {

				this.delegate = delegate;
				this.field = field;
			}

			@Override
			public <R> TerminatingDistinct<R> as(Class<R> resultType) {

				Assert.notNull(resultType, "ResultType must not be null");

				return new DistinctOperationSupport<>((ReactiveFindSupport) delegate.as(resultType), field);
			}

			@Override
			public TerminatingDistinct<T> matching(Query query) {

				Assert.notNull(query, "Query must not be null");

				return new DistinctOperationSupport<>((ReactiveFindSupport) delegate.matching(query), field);
			}

			@Override
			public Flux<T> all() {
				return delegate.doFindDistinct(field);
			}
		}
	}
}
