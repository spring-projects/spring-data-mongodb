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

import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.result.DeleteResult;

/**
 * Implementation of {@link ExecutableRemoveOperation}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class ExecutableRemoveOperationSupport implements ExecutableRemoveOperation {

	private static final Query ALL_QUERY = new Query();

	private final MongoTemplate tempate;

	public ExecutableRemoveOperationSupport(MongoTemplate tempate) {
		this.tempate = tempate;
	}

	@Override
	@Contract("_ -> new")
	public <T> ExecutableRemove<T> remove(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ExecutableRemoveSupport<>(tempate, domainType, ALL_QUERY, null, QueryResultConverter.entity());
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class ExecutableRemoveSupport<S, T> implements ExecutableRemove<T>, RemoveWithCollection<T> {

		private final MongoTemplate template;
		private final Class<S> domainType;
		private final Query query;
		@Nullable private final String collection;
		private final QueryResultConverter<? super S, ? extends T> resultConverter;

		public ExecutableRemoveSupport(MongoTemplate template, Class<S> domainType, Query query,
				@Nullable String collection, QueryResultConverter<? super S, ? extends T> resultConverter) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.collection = collection;
			this.resultConverter = resultConverter;
		}

		@Override
		@Contract("_ -> new")
		public RemoveWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty");

			return new ExecutableRemoveSupport<>(template, domainType, query, collection, resultConverter);
		}

		@Override
		@Contract("_ -> new")
		public TerminatingRemove<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ExecutableRemoveSupport<>(template, domainType, query, collection, resultConverter);
		}

		@Override
		public DeleteResult all() {
			return template.doRemove(getCollectionName(), query, domainType, true);
		}

		@Override
		public DeleteResult one() {
			return template.doRemove(getCollectionName(), query, domainType, false);
		}

		@Override
		public List<T> findAndRemove() {

			String collectionName = getCollectionName();

			return template.doFindAndDelete(collectionName, query, domainType, resultConverter);
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {
			return new ExecutableRemoveSupport<>(template, (Class) domainType, query, collection, converter);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		}
	}
}
