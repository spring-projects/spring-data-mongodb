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

import java.util.ArrayList;
import java.util.Collection;

import lombok.experimental.FieldDefaults;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
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

	/**
	 * Create new {@link ExecutableInsertOperationSupport}.
	 *
	 * @param template must not be {@literal null}.
	 * @throws IllegalArgumentException if template is {@literal null}.
	 */
	ExecutableInsertOperationSupport(MongoTemplate template) {

		Assert.notNull(template, "Template must not be null!");

		this.template = template;
	}

	@Override
	public <T> ExecutableInsert<T> insert(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableInsertSupport<>(template, domainType, null, null);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ExecutableInsertSupport<T> implements ExecutableInsert<T> {

		@NonNull MongoTemplate template;
		@NonNull Class<T> domainType;
		String collection;
		BulkMode bulkMode;

		@Override
		public void one(T object) {

			Assert.notNull(object, "Object must not be null!");

			template.insert(object, getCollectionName());
		}

		@Override
		public void all(Collection<? extends T> objects) {

			Assert.notNull(objects, "Objects must not be null!");

			template.insert(objects, getCollectionName());
		}

		@Override
		public BulkWriteResult bulk(Collection<? extends T> objects) {

			Assert.notNull(objects, "Objects must not be null!");

			return template.bulkOps(bulkMode != null ? bulkMode : BulkMode.ORDERED, domainType, getCollectionName())
					.insert(new ArrayList<>(objects)).execute();
		}

		@Override
		public InsertWithBulkMode<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty.");

			return new ExecutableInsertSupport<>(template, domainType, collection, bulkMode);
		}

		@Override
		public TerminatingBulkInsert<T> withBulkMode(BulkMode bulkMode) {

			Assert.notNull(bulkMode, "BulkMode must not be null!");

			return new ExecutableInsertSupport<>(template, domainType, collection, bulkMode);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.determineCollectionName(domainType);
		}
	}
}
