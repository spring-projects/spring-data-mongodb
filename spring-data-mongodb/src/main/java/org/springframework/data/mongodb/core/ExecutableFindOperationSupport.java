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

import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.client.FindIterable;

/**
 * Implementation of {@link ExecutableFindOperation}.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
class ExecutableFindOperationSupport implements ExecutableFindOperation {

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
	public <T> FindOperation<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new FindOperationSupport<>(template, domainType, domainType, null, null);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	static class FindOperationSupport<T> implements FindOperation<T>, FindOperationWithCollection<T>,
			FindOperationWithProjection<T>, FindOperationWithQuery<T> {

		private final MongoTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final String collection;
		private final Query query;

		@Override
		public FindOperationWithProjection<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty!");

			return new FindOperationSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public <T1> FindOperationWithQuery<T1> as(Class<T1> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null!");

			return new FindOperationSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public TerminatingFindOperation<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new FindOperationSupport<>(template, domainType, returnType, collection, query);
		}

		@Override
		public Optional<T> one() {

			List<T> result = doFind(new DelegatingQueryCursorPreparer(getCursorPreparer(query, null)).limit(2));

			if (ObjectUtils.isEmpty(result)) {
				return Optional.empty();
			}

			if (result.size() > 1) {
				throw new IncorrectResultSizeDataAccessException("Query " + asString() + " returned non unique result.", 1);
			}

			return Optional.of(result.iterator().next());
		}

		@Override
		public Optional<T> first() {

			List<T> result = doFind(new DelegatingQueryCursorPreparer(getCursorPreparer(query, null)).limit(1));

			return ObjectUtils.isEmpty(result) ? Optional.empty() : Optional.of(result.iterator().next());
		}

		@Override
		public List<T> all() {
			return doFind(null);
		}

		@Override
		public CloseableIterator<T> stream() {
			return doStream();
		}

		@Override
		public TerminatingFindNearOperation<T> near(NearQuery nearQuery) {
			return () -> template.geoNear(nearQuery, domainType, getCollectionName(), returnType);
		}

		private List<T> doFind(CursorPreparer preparer) {

			Document queryObject = query != null ? query.getQueryObject() : new Document();
			Document fieldsObject = query != null ? query.getFieldsObject() : new Document();

			return template.doFind(getCollectionName(), queryObject, fieldsObject, domainType, returnType,
					getCursorPreparer(query, preparer));
		}

		private CloseableIterator<T> doStream() {

			return template.doStream(query != null ? query : new BasicQuery(new Document()), domainType, getCollectionName(),
					returnType);
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
