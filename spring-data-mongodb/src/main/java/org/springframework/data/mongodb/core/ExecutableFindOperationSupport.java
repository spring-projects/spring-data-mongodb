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

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.bson.Document;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.data.util.CloseableIterator;
import org.springframework.data.util.StreamUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

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

	/**
	 * Create new {@link ExecutableFindOperationSupport}.
	 *
	 * @param template must not be {@literal null}.
	 * @throws IllegalArgumentException if template is {@literal null}.
	 */
	ExecutableFindOperationSupport(MongoTemplate template) {

		Assert.notNull(template, "Template must not be null!");

		this.template = template;
	}

	@Override
	public <T> ExecutableFind<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableFindSupport<>(template, domainType, domainType, null, ALL_QUERY);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ExecutableFindSupport<T>
			implements ExecutableFind<T>, FindWithCollection<T>, FindWithProjection<T>, FindWithQuery<T> {

		@NonNull MongoTemplate template;
		@NonNull Class<?> domainType;
		Class<T> returnType;
		String collection;
		Query query;

		@Override
		public FindWithProjection<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty!");

			return new ExecutableFindSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public <T1> FindWithQuery<T1> as(Class<T1> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null!");

			return new ExecutableFindSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public TerminatingFind<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ExecutableFindSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public T oneValue() {

			List<T> result = doFind(new DelegatingQueryCursorPreparer(getCursorPreparer(query, null)).limit(2));

			if (ObjectUtils.isEmpty(result)) {
				return null;
			}

			if (result.size() > 1) {
				throw new IncorrectResultSizeDataAccessException("Query " + asString() + " returned non unique result.", 1);
			}

			return result.iterator().next();
		}

		@Override
		public T firstValue() {

			List<T> result = doFind(new DelegatingQueryCursorPreparer(getCursorPreparer(query, null)).limit(1));

			return ObjectUtils.isEmpty(result) ? null : result.iterator().next();
		}

		@Override
		public List<T> all() {
			return doFind(null);
		}

		@Override
		public Stream<T> stream() {
			return StreamUtils.createStreamFromIterator(doStream());
		}

		@Override
		public TerminatingFindNear<T> near(NearQuery nearQuery) {
			return () -> template.geoNear(nearQuery, domainType, getCollectionName(), returnType);
		}

		@Override
		public long count() {
			return template.count(query, domainType, getCollectionName());
		}

		@Override
		public boolean exists() {
			return template.exists(query, domainType, getCollectionName());
		}

		private List<T> doFind(CursorPreparer preparer) {

			Document queryObject = query.getQueryObject();
			Document fieldsObject = query.getFieldsObject();

			return template.doFind(getCollectionName(), queryObject, fieldsObject, domainType, returnType,
					getCursorPreparer(query, preparer));
		}

		private CloseableIterator<T> doStream() {
			return template.doStream(query, domainType, getCollectionName(), returnType);
		}

		private CursorPreparer getCursorPreparer(Query query, CursorPreparer preparer) {
			return query == null || preparer != null ? preparer : template.new QueryCursorPreparer(query, domainType);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.determineCollectionName(domainType);
		}

		private String asString() {
			return SerializationUtils.serializeToJsonSafely(query);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class DelegatingQueryCursorPreparer implements CursorPreparer {

		private final CursorPreparer delegate;
		private Optional<Integer> limit = Optional.empty();

		DelegatingQueryCursorPreparer(CursorPreparer delegate) {
			this.delegate = delegate;
		}

		@Override
		public FindIterable<Document> prepare(FindIterable<Document> cursor) {

			FindIterable<Document> target = delegate != null ? delegate.prepare(cursor) : cursor;
			return limit.map(target::limit).orElse(target);
		}

		CursorPreparer limit(int limit) {

			this.limit = Optional.of(limit);
			return this;
		}
	}
}
