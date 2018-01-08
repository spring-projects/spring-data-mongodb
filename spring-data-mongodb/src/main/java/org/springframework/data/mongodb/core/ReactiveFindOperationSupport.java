/*
 * Copyright 2017-2018 the original author or authors.
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
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.reactivestreams.client.FindPublisher;

/**
 * Implementation of {@link ReactiveFindOperation}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
@RequiredArgsConstructor
class ReactiveFindOperationSupport implements ReactiveFindOperation {

	private static final Query ALL_QUERY = new Query();

	private final @NonNull ReactiveMongoTemplate template;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveFindOperation#query(java.lang.Class)
	 */
	@Override
	public <T> ReactiveFind<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ReactiveFindSupport<>(template, domainType, domainType, null, ALL_QUERY);
	}

	/**
	 * @param <T>
	 * @author Mark Paluch
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.FindWithCollection#inCollection(java.lang.String)
		 */
		@Override
		public FindWithProjection<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty!");

			return new ReactiveFindSupport<>(template, domainType, returnType, collection, query);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.FindWithProjection#as(java.lang.Class)
		 */
		@Override
		public <T1> FindWithQuery<T1> as(Class<T1> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null!");

			return new ReactiveFindSupport<>(template, domainType, returnType, collection, query);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.FindWithQuery#matching(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public TerminatingFind<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ReactiveFindSupport<>(template, domainType, returnType, collection, query);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.TerminatingFind#first()
		 */
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.TerminatingFind#one()
		 */
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.TerminatingFind#all()
		 */
		@Override
		public Flux<T> all() {
			return doFind(null);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.FindWithQuery#near(org.springframework.data.mongodb.core.query.NearQuery)
		 */
		@Override
		public TerminatingFindNear<T> near(NearQuery nearQuery) {
			return () -> template.geoNear(nearQuery, domainType, getCollectionName(), returnType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.TerminatingFind#count()
		 */
		@Override
		public Mono<Long> count() {
			return template.count(query, domainType, getCollectionName());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.TerminatingFind#exists()
		 */
		@Override
		public Mono<Boolean> exists() {
			return template.exists(query, domainType, getCollectionName());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.FindDistinct#distinct(java.lang.String)
		 */
		@Override
		public TerminatingDistinct<Object> distinct(String field) {

			Assert.notNull(field, "Field must not be null!");

			return new DistinctOperationSupport<>(this, field);
		}

		private Flux<T> doFind(@Nullable FindPublisherPreparer preparer) {

			Document queryObject = query.getQueryObject();
			Document fieldsObject = query.getFieldsObject();

			return template.doFind(getCollectionName(), queryObject, fieldsObject, domainType, returnType,
					preparer != null ? preparer : getCursorPreparer(query));
		}

		@SuppressWarnings("unchecked")
		private Flux<T> doFindDistinct(String field) {

			return template.findDistinct(query, field, getCollectionName(), domainType,
					returnType == domainType ? (Class<T>) Object.class : returnType);
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

		/**
		 * @author Christoph Strobl
		 * @since 2.1
		 */
		static class DistinctOperationSupport<T> implements TerminatingDistinct<T> {

			private final String field;
			private final ReactiveFindSupport delegate;

			public DistinctOperationSupport(ReactiveFindSupport delegate, String field) {

				this.delegate = delegate;
				this.field = field;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.DistinctWithProjection#as(java.lang.Class)
			 */
			@Override
			public <R> TerminatingDistinct<R> as(Class<R> resultType) {

				Assert.notNull(resultType, "ResultType must not be null!");

				return new DistinctOperationSupport<>((ReactiveFindSupport) delegate.as(resultType), field);
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.ReactiveFindOperation.DistinctWithQuery#matching(org.springframework.data.mongodb.core.query.Query)
			 */
			@Override
			@SuppressWarnings("unchecked")
			public TerminatingDistinct<T> matching(Query query) {

				Assert.notNull(query, "Query must not be null!");

				return new DistinctOperationSupport<>((ReactiveFindSupport<T>) delegate.matching(query), field);
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core..ReactiveFindOperation.TerminatingDistinct#all()
			 */
			@Override
			public Flux<T> all() {
				return delegate.doFindDistinct(field);
			}
		}
	}
}
