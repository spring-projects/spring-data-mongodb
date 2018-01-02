/*
 * Copyright 2017-2018 the original author or authors.
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

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation#remove(java.lang.Class)
	 */
	@Override
	public <T> ExecutableRemove<T> remove(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableRemoveSupport<>(tempate, domainType, ALL_QUERY, null);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ExecutableRemoveSupport<T> implements ExecutableRemove<T>, RemoveWithCollection<T> {

		@NonNull MongoTemplate template;
		@NonNull Class<T> domainType;
		Query query;
		@Nullable String collection;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.RemoveWithCollection#inCollection(java.lang.String)
		 */
		@Override
		public RemoveWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");

			return new ExecutableRemoveSupport<>(template, domainType, query, collection);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.RemoveWithQuery#matching(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public TerminatingRemove<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ExecutableRemoveSupport<>(template, domainType, query, collection);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove#all()
		 */
		@Override
		public DeleteResult all() {

			String collectionName = getCollectionName();

			return template.doRemove(collectionName, query, domainType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove#findAndRemove()
		 */
		@Override
		public List<T> findAndRemove() {

			String collectionName = getCollectionName();

			return template.doFindAndDelete(collectionName, query, domainType);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.determineCollectionName(domainType);
		}
	}
}
