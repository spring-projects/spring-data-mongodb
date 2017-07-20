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

import java.util.Optional;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.result.UpdateResult;

/**
 * Implementation of {@link ExecutableUpdateOperation}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class ExecutableUpdateOperationSupport implements ExecutableUpdateOperation {

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
	public <T> ExecutableUpdate<T> update(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableUpdateSupport<>(template, domainType, null, null, null, null);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ExecutableUpdateSupport<T>
			implements ExecutableUpdate<T>, UpdateWithCollection<T>, UpdateWithQuery<T>, TerminatingUpdate<T> {

		@NonNull MongoTemplate template;
		@NonNull Class<T> domainType;
		Query query;
		Update update;
		String collection;
		FindAndModifyOptions options;

		@Override
		public TerminatingUpdate<T> apply(Update update) {

			Assert.notNull(update, "Update must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, options);
		}

		@Override
		public UpdateWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, options);
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
		public Optional<T> findAndModify() {

			String collectionName = getCollectionName();

			return Optional.ofNullable(template.findAndModify(query != null ? query : new BasicQuery(new Document()), update,
					options, domainType, collectionName));
		}

		@Override
		public UpdateWithUpdate<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, options);
		}

		@Override
		public UpdateResult all() {
			return doUpdate(true, false);
		}

		@Override
		public TerminatingFindAndModify<T> withOptions(FindAndModifyOptions options) {

			Assert.notNull(options, "Options must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, options);
		}

		private UpdateResult doUpdate(boolean multi, boolean upsert) {

			String collectionName = getCollectionName();

			Query query = this.query != null ? this.query : new BasicQuery(new Document());

			return template.doUpdate(collectionName, query, update, domainType, upsert, multi);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.determineCollectionName(domainType);
		}
	}
}
