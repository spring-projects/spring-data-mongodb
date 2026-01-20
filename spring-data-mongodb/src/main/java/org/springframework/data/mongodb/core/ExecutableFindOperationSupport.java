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

import java.util.List;
import java.util.stream.Stream;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.core.TypedPropertyPath;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Window;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;

/**
 * Implementation of {@link ExecutableFindOperation}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class ExecutableFindOperationSupport implements ExecutableFindOperation {

	private static final Query ALL_QUERY = new Query();

	private final MongoTemplate template;

	ExecutableFindOperationSupport(MongoTemplate template) {
		this.template = template;
	}

	@Override
	@Contract("_ -> new")
	public <T> ExecutableFind<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ExecutableFindSupport<>(template, domainType, domainType, QueryResultConverter.entity(), null,
				ALL_QUERY);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class ExecutableFindSupport<S, T>
			implements ExecutableFind<T>, FindWithCollection<T>, FindWithProjection<T>, FindWithQuery<T> {

		private final MongoTemplate template;
		private final Class<?> domainType;
		private final Class<S> returnType;
		private final QueryResultConverter<? super S, ? extends T> resultConverter;
		private final @Nullable String collection;
		private final Query query;

		ExecutableFindSupport(MongoTemplate template, Class<?> domainType, Class<S> returnType,
				QueryResultConverter<? super S, ? extends T> resultConverter, @Nullable String collection,
				Query query) {
			this.template = template;
			this.domainType = domainType;
			this.resultConverter = resultConverter;
			this.returnType = returnType;
			this.collection = collection;
			this.query = query;
		}

		@Override
		@Contract("_ -> new")
		public FindWithProjection<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty");

			return new ExecutableFindSupport<>(template, domainType, returnType, resultConverter, collection, query);
		}

		@Override
		@Contract("_ -> new")
		public <T1> FindWithQuery<T1> as(Class<T1> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null");

			return new ExecutableFindSupport<>(template, domainType, returnType, QueryResultConverter.entity(), collection,
					query);
		}

		@Override
		@Contract("_ -> new")
		public TerminatingFind<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ExecutableFindSupport<>(template, domainType, returnType, resultConverter, collection, query);
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "QueryResultConverter must not be null");

			return new ExecutableFindSupport<>(template, domainType, returnType, this.resultConverter.andThen(converter),
					collection, query);
		}

		@Override
		public @Nullable T oneValue() {

			List<T> result = doFind(new DelegatingQueryCursorPreparer(getCursorPreparer(query, null)).limit(2));

			if (ObjectUtils.isEmpty(result)) {
				return null;
			}

			if (result.size() > 1) {
				throw new IncorrectResultSizeDataAccessException("Query " + asString() + " returned non unique result", 1);
			}

			return result.iterator().next();
		}

		@Override
		public @Nullable T firstValue() {

			List<T> result = doFind(new DelegatingQueryCursorPreparer(getCursorPreparer(query, null)).limit(1));

			return ObjectUtils.isEmpty(result) ? null : result.iterator().next();
		}

		@Override
		public List<T> all() {
			return doFind(null);
		}

		@Override
		public Stream<T> stream() {
			return doStream();
		}

		@Override
		public Window<T> scroll(ScrollPosition scrollPosition) {
			return template.doScroll(query.with(scrollPosition), domainType, returnType, resultConverter,
					getCollectionName());
		}

		@Override
		public TerminatingFindNear<T> near(NearQuery nearQuery) {
			return new TerminatingFindNearSupport<>(nearQuery, this.resultConverter);
		}

		@Override
		public long count() {
			return template.count(query, domainType, getCollectionName());
		}

		@Override
		public boolean exists() {
			return template.exists(query, domainType, getCollectionName());
		}

		@SuppressWarnings("unchecked")
		@Override
		public TerminatingDistinct<Object> distinct(String field) {

			Assert.notNull(field, "Field must not be null");

			return new DistinctOperationSupport(this, field);
		}

		@Override
		public <V, R> TerminatingDistinct<R> distinct(TypedPropertyPath<V, R> path) {
			return new DistinctOperationSupport(this, path.toDotPath());
		}

		private List<T> doFind(@Nullable CursorPreparer preparer) {

			Document queryObject = query.getQueryObject();
			Document fieldsObject = query.getFieldsObject();

			return template.doFind(template.createDelegate(query), getCollectionName(), queryObject, fieldsObject, domainType,
					returnType, resultConverter, getCursorPreparer(query, preparer));
		}

		private List<T> doFindDistinct(String field) {

			return template.findDistinct(query, field, getCollectionName(), domainType,
					returnType == domainType ? (Class) Object.class : returnType);
		}

		private Stream<T> doStream() {
			return template.doStream(query, domainType, getCollectionName(), returnType, resultConverter);
		}

		private CursorPreparer getCursorPreparer(Query query, @Nullable CursorPreparer preparer) {
			return preparer != null ? preparer : template.new QueryCursorPreparer(query, domainType);
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
			public GeoResults<G> all() {
				return template.doGeoNear(nearQuery, domainType, getCollectionName(), returnType, resultConverter);
			}

			@Override
			public long count() {
				return template.doGeoNearCount(nearQuery, domainType, getCollectionName());
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class DelegatingQueryCursorPreparer implements CursorPreparer {

		private final @Nullable CursorPreparer delegate;
		private int limit = -1;

		DelegatingQueryCursorPreparer(@Nullable CursorPreparer delegate) {
			this.delegate = delegate;
		}

		@Override
		public FindIterable<Document> prepare(FindIterable<Document> iterable) {

			FindIterable<Document> target = delegate != null ? delegate.prepare(iterable) : iterable;
			if (limit >= 0) {
				target.limit(limit);
			}
			return target;
		}

		@Contract("_ -> this")
		CursorPreparer limit(int limit) {

			this.limit = limit;
			return this;
		}

		@Override
		public @Nullable ReadPreference getReadPreference() {
			return delegate != null ? delegate.getReadPreference() : null;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class DistinctOperationSupport<S, T> implements TerminatingDistinct<T> {

		private final String field;
		private final ExecutableFindSupport<S, T> delegate;

		public DistinctOperationSupport(ExecutableFindSupport<S, T> delegate, String field) {

			this.delegate = delegate;
			this.field = field;
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Contract("_ -> new")
		public <R> TerminatingDistinct<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "ResultType must not be null");

			return new DistinctOperationSupport<>((ExecutableFindSupport) delegate.as(resultType), field);
		}

		@Override
		@Contract("_ -> new")
		public TerminatingDistinct<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new DistinctOperationSupport<>((ExecutableFindSupport<S, T>) delegate.matching(query), field);
		}

		@Override
		public List<T> all() {
			return delegate.doFindDistinct(field);
		}

	}
}
