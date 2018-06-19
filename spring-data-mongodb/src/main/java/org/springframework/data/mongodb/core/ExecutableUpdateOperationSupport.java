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

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.lang.Nullable;
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
@RequiredArgsConstructor
class ExecutableUpdateOperationSupport implements ExecutableUpdateOperation {

	private static final Query ALL_QUERY = new Query();

	private final @NonNull MongoTemplate template;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation#update(java.lang.Class)
	 */
	@Override
	public <T> ExecutableUpdate<T> update(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableUpdateSupport<>(template, domainType, ALL_QUERY, null, null, null, null, null, domainType);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ExecutableUpdateSupport<T>
			implements ExecutableUpdate<T>, UpdateWithCollection<T>, UpdateWithQuery<T>, TerminatingUpdate<T>,
			FindAndReplaceWithOptions<T>, TerminatingFindAndReplace<T>, FindAndReplaceWithProjection<T> {

		@NonNull MongoTemplate template;
		@NonNull Class domainType;
		Query query;
		@Nullable Update update;
		@Nullable String collection;
		@Nullable FindAndModifyOptions findAndModifyOptions;
		@Nullable FindAndReplaceOptions findAndReplaceOptions;
		@Nullable Object replacement;
		@NonNull Class<T> targetType;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.UpdateWithUpdate#apply(Update)
		 */
		@Override
		public TerminatingUpdate<T> apply(Update update) {

			Assert.notNull(update, "Update must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.UpdateWithCollection#inCollection(java.lang.String)
		 */
		@Override
		public UpdateWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.FindAndModifyWithOptions#withOptions(org.springframework.data.mongodb.core.FindAndModifyOptions)
		 */
		@Override
		public TerminatingFindAndModify<T> withOptions(FindAndModifyOptions options) {

			Assert.notNull(options, "Options must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, options,
					findAndReplaceOptions, replacement, targetType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.UpdateWithUpdate#replaceWith(Object)
		 */
		@Override
		public FindAndReplaceWithProjection<T> replaceWith(T replacement) {

			Assert.notNull(replacement, "Replacement must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.FindAndReplaceWithOptions#withOptions(org.springframework.data.mongodb.core.FindAndReplaceOptions)
		 */
		@Override
		public FindAndReplaceWithProjection<T> withOptions(FindAndReplaceOptions options) {

			Assert.notNull(options, "Options must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					options, replacement, targetType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveUpdateOperation.UpdateWithQuery#matching(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public UpdateWithUpdate<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ReactiveUpdateOperation.FindAndReplaceWithProjection#as(java.lang.Class)
		 */
		@Override
		public <R> FindAndReplaceWithOptions<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "ResultType must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, resultType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingUpdate#all()
		 */
		@Override
		public UpdateResult all() {
			return doUpdate(true, false);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingUpdate#first()
		 */
		@Override
		public UpdateResult first() {
			return doUpdate(false, false);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingUpdate#upsert()
		 */
		@Override
		public UpdateResult upsert() {
			return doUpdate(true, true);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingFindAndModify#findAndModifyValue()
		 */
		@Override
		public @Nullable T findAndModifyValue() {

			return template.findAndModify(query, update,
					findAndModifyOptions != null ? findAndModifyOptions : new FindAndModifyOptions(), targetType,
					getCollectionName());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingFindAndReplace#findAndReplaceValue()
		 */
		@Override
		public @Nullable T findAndReplaceValue() {

			return (T) template.findAndReplace(query, replacement,
					findAndReplaceOptions != null ? findAndReplaceOptions : FindAndReplaceOptions.empty(), domainType,
					getCollectionName(), targetType);
		}

		private UpdateResult doUpdate(boolean multi, boolean upsert) {
			return template.doUpdate(getCollectionName(), query, update, domainType, upsert, multi);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		}
	}
}
