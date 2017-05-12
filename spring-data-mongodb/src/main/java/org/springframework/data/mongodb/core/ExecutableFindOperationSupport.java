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

import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.client.FindIterable;

/**
 * Implementation of {@link ExecutableFindOperationBuilder}.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
class ExecutableFindOperationSupport implements ExecutableFindOperationBuilder {

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
	public <T> FindOperationBuilder<T> find(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new FindBuilder<>(template, null, domainType, null, domainType);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class FindBuilder<T>
			implements FindOperationBuilder<T>, WithCollectionBuilder<T>, WithProjectionBuilder<T>, WithQueryBuilder<T> {

		private final MongoTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final String collection;
		private final Query query;

		private FindBuilder(MongoTemplate template, Query query, Class<?> domainType, String collection,
				Class<T> returnType) {

			this.template = template;
			this.query = query;
			this.returnType = returnType;
			this.domainType = domainType;
			this.collection = collection;
		}

		@Override
		public WithProjectionBuilder<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty!");

			return new FindBuilder<>(template, query, domainType, collection, returnType);
		}

		@Override
		public <T1> WithQueryBuilder<T1> as(Class<T1> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null!");

			return new FindBuilder<>(template, query, domainType, collection, returnType);
		}

		public String asString() {
			return "";
		}

		@Override
		public T one() {

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
		public T first() {

			List<T> result = doFind(new DelegatingQueryCursorPreparer(getCursorPreparer(query, null)).limit(1));
			return ObjectUtils.isEmpty(result) ? null : result.iterator().next();
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
		public FindOperationBuilderTerminatingOperations<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new FindBuilder<>(template, query, domainType, collection, returnType);
		}

		@Override
		public FindOperationBuilderTerminatingNearOperations near(NearQuery nearQuery) {
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
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class DelegatingQueryCursorPreparer implements CursorPreparer {

		private final CursorPreparer delegate;
		private Optional<Integer> limit = Optional.empty();

		public DelegatingQueryCursorPreparer(CursorPreparer delegate) {
			this.delegate = delegate;
		}

		@Override
		public FindIterable<Document> prepare(FindIterable<Document> cursor) {

			FindIterable<Document> target = delegate.prepare(cursor);

			if (limit.isPresent()) {
				target = target.limit(limit.get());
			}

			return target;
		}

		CursorPreparer limit(int limit) {

			this.limit = Optional.of(limit);
			return this;
		}
	}
}
