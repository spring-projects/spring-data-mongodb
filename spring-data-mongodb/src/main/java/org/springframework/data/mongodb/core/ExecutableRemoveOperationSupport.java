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

import org.bson.Document;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.result.DeleteResult;

/**
 * Implementation of {@link ExecutableRemoveOperation}.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
class ExecutableRemoveOperationSupport implements ExecutableRemoveOperation {

	private final MongoTemplate tempate;

	/**
	 * Create new {@link ExecutableRemoveOperationSupport}.
	 *
	 * @param template must not be {@literal null}.
	 * @throws IllegalArgumentException if template is {@literal null}.
	 */
	ExecutableRemoveOperationSupport(MongoTemplate template) {

		Assert.notNull(template, "Template must not be null!");
		this.tempate = template;
	}

	@Override
	public <T> RemoveOperation<T> remove(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");
		return new RemoveOperationSupport<>(tempate, null, domainType, null);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	static class RemoveOperationSupport<T> implements RemoveOperation<T>, RemoveOperationWithCollection<T> {

		private final MongoTemplate template;
		private final Query query;
		private final Class<T> domainType;
		private final String collection;

		@Override
		public RemoveOperationWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");
			return new RemoveOperationSupport<>(template, query, domainType, collection);
		}

		@Override
		public TerminatingRemoveOperation<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");
			return new RemoveOperationSupport<>(template, query, domainType, collection);
		}

		@Override
		public DeleteResult all() {

			String collectionName = StringUtils.hasText(collection) ? collection
					: template.determineCollectionName(domainType);

			return template.doRemove(collectionName, query != null ? query : new BasicQuery(new Document()), domainType);
		}

		@Override
		public List<T> findAndRemove() {

			String collectionName = StringUtils.hasText(collection) ? collection
					: template.determineCollectionName(domainType);

			return template.doFindAndDelete(collectionName, query != null ? query : new BasicQuery(new Document()),
					domainType);
		}
	}
}
