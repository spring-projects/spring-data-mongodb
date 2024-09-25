/*
 * Copyright 2017-2024 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.bulk.BulkWriteResult;

/**
 * Implementation of {@link ExecutableInsertOperation}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class ExecutableInsertOperationSupport implements ExecutableInsertOperation {

	private final MongoTemplate template;

	ExecutableInsertOperationSupport(MongoTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableInsert<T> insert(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ExecutableInsertSupport<>(template, domainType, null, null);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static class ExecutableInsertSupport<T> implements ExecutableInsert<T> {

		private final MongoTemplate template;
		private final Class<T> domainType;
		@Nullable private final String collection;
		@Nullable private final BulkMode bulkMode;

		ExecutableInsertSupport(MongoTemplate template, Class<T> domainType, String collection, BulkMode bulkMode) {

			this.template = template;
			this.domainType = domainType;
			this.collection = collection;
			this.bulkMode = bulkMode;
		}

		@Override
		public T one(T object) {

			Assert.notNull(object, "Object must not be null");

			return template.insert(object, getCollectionName());
		}

		@Override
		public Collection<T> all(Collection<? extends T> objects) {

			Assert.notNull(objects, "Objects must not be null");

			return template.insert(objects, getCollectionName());
		}

		@Override
		public BulkWriteResult bulk(Collection<? extends T> objects) {

			Assert.notNull(objects, "Objects must not be null");

			return template.bulkOps(bulkMode != null ? bulkMode : BulkMode.ORDERED, domainType, getCollectionName())
					.insert(new ArrayList<>(objects)).execute();
		}

		@Override
		public InsertWithBulkMode<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty");

			return new ExecutableInsertSupport<>(template, domainType, collection, bulkMode);
		}

		@Override
		public TerminatingBulkInsert<T> withBulkMode(BulkMode bulkMode) {

			Assert.notNull(bulkMode, "BulkMode must not be null");

			return new ExecutableInsertSupport<>(template, domainType, collection, bulkMode);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		}
	}
}
