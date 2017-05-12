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

import org.bson.Document;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.result.UpdateResult;

/**
 * Implementation of {@link ExecutableUpdateOperationBuilder}.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
class ExecutableUpdateOperationSupport implements ExecutableUpdateOperationBuilder {

	private final MongoTemplate template;

	/**
	 * Creates new {@link ExecutableUpdateOperationSupport}.
	 *
	 * @param template must not be {@literal null}.
	 */
	ExecutableUpdateOperationSupport(MongoTemplate template) {

		Assert.notNull(template, "Template must not be null!");
		this.template = template;
	}

	@Override
	public <T> UpdateOperationBuilder<T> update(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");
		return new UpdateBuilder<T>(template, null, domainType, null, null, null);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class UpdateBuilder<T>
			implements WithOptionsBuilder<T>, UpdateOperationBuilder<T>, WithCollectionBuilder<T>, WithQueryBuilder<T> {

		private final MongoTemplate template;
		private final Query query;
		private final Class<T> domainType;
		private final Update update;
		private final String collection;
		private final FindAndModifyOptions options;

		private UpdateBuilder(MongoTemplate template, Query query, Class<T> domainType, Update update, String collection,
				FindAndModifyOptions options) {

			this.template = template;
			this.query = query;
			this.domainType = domainType;
			this.update = update;
			this.collection = collection;
			this.options = options;
		}

		@Override
		public WithOptionsBuilder apply(Update update) {

			Assert.notNull(update, "Update must not be null!");
			return new UpdateBuilder<T>(template, query, domainType, update, collection, options);
		}

		@Override
		public WithQueryBuilder<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");
			return new UpdateBuilder<T>(template, query, domainType, update, collection, options);
		}

		@Override
		public UpdateResult first() {
			return doUpdate(false, false);
		}

		@Override
		public UpdateResult upsert() {
			return doUpdate(true, true);
		}

		@Override
		public T findAndModify() {

			String collectionName = StringUtils.hasText(collection) ? collection
					: template.determineCollectionName(domainType);

			return template.findAndModify(query != null ? query : new BasicQuery(new Document()), update, options, domainType,
					collectionName);
		}

		@Override
		public UpdateOperationBuilderTerminatingOperations<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");
			return new UpdateBuilder<T>(template, query, domainType, update, collection, options);
		}

		@Override
		public UpdateResult all() {
			return doUpdate(true, false);
		}

		@Override
		public WithFindAndModifyBuilder withOptions(FindAndModifyOptions options) {

			Assert.notNull(options, "Options must not be null!");
			return new UpdateBuilder<T>(template, query, domainType, update, collection, options);
		}

		private UpdateResult doUpdate(boolean multi, boolean upsert) {

			String collectionName = StringUtils.hasText(collection) ? collection
					: template.determineCollectionName(domainType);

			Query query = this.query != null ? this.query : new BasicQuery(new Document());

			return template.doUpdate(collectionName, query, update, domainType, upsert, multi);
		}
	}
}
