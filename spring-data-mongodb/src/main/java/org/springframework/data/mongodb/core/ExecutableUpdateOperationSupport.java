/*
 * Copyright 2017-present the original author or authors.
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

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.Contract;
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

	private static final Query ALL_QUERY = new Query();

	private final MongoTemplate template;

	ExecutableUpdateOperationSupport(MongoTemplate template) {
		this.template = template;
	}

	@Override
	@Contract("_ -> new")
	public <T> ExecutableUpdate<T> update(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ExecutableUpdateSupport<>(template, domainType, ALL_QUERY, null, null, null, null, null, domainType, QueryResultConverter.entity());
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@SuppressWarnings("rawtypes")
	static class ExecutableUpdateSupport<S, T>
			implements ExecutableUpdate<T>, UpdateWithCollection<T>, UpdateWithQuery<T>, TerminatingUpdate<T>,
			FindAndReplaceWithOptions<T>, TerminatingFindAndReplace<T>, FindAndReplaceWithProjection<T> {

		private final MongoTemplate template;
		private final Class<?> domainType;
		private final Query query;
		@Nullable private final UpdateDefinition update;
		@Nullable private final String collection;
		@Nullable private final FindAndModifyOptions findAndModifyOptions;
		@Nullable private final FindAndReplaceOptions findAndReplaceOptions;
		@Nullable private final Object replacement;
		private final QueryResultConverter<? super S, ? extends T> resultConverter;
		private final Class<S> targetType;

		ExecutableUpdateSupport(MongoTemplate template, Class<?> domainType, Query query, @Nullable UpdateDefinition update,
				@Nullable String collection, @Nullable FindAndModifyOptions findAndModifyOptions,
				@Nullable FindAndReplaceOptions findAndReplaceOptions, @Nullable Object replacement, Class<S> targetType,
			QueryResultConverter<? super S, ? extends T> resultConverter) {

			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.update = update;
			this.collection = collection;
			this.findAndModifyOptions = findAndModifyOptions;
			this.findAndReplaceOptions = findAndReplaceOptions;
			this.replacement = replacement;
			this.targetType = targetType;
			this.resultConverter = resultConverter;
		}

		@Override
		@Contract("_ -> new")
		public TerminatingUpdate<T> apply(UpdateDefinition update) {

			Assert.notNull(update, "Update must not be null");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType, resultConverter);
		}

		@Override
		@Contract("_ -> new")
		public UpdateWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType, resultConverter);
		}

		@Override
		@Contract("_ -> new")
		public TerminatingFindAndModify<T> withOptions(FindAndModifyOptions options) {

			Assert.notNull(options, "Options must not be null");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, options,
					findAndReplaceOptions, replacement, targetType, resultConverter);
		}

		@Override
		@Contract("_ -> new")
		public FindAndReplaceWithProjection<T> replaceWith(T replacement) {

			Assert.notNull(replacement, "Replacement must not be null");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType, resultConverter);
		}

		@Override
		@Contract("_ -> new")
		public FindAndReplaceWithProjection<T> withOptions(FindAndReplaceOptions options) {

			Assert.notNull(options, "Options must not be null");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					options, replacement, targetType, resultConverter);
		}

		@Override
		@Contract("_ -> new")
		public TerminatingReplace withOptions(ReplaceOptions options) {

			FindAndReplaceOptions target = new FindAndReplaceOptions();
			if (options.isUpsert()) {
				target.upsert();
			}
			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					target, replacement, targetType, resultConverter);
		}

		@Override
		@Contract("_ -> new")
		public UpdateWithUpdate<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType, resultConverter);
		}

		@Override
		@Contract("_ -> new")
		public <R> FindAndReplaceWithOptions<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "ResultType must not be null");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, resultType, QueryResultConverter.entity());
		}

		@Override
		public UpdateResult all() {
			return doUpdate(true, false);
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
		public <R> ExecutableUpdateSupport<S, R> map(QueryResultConverter<? super T, ? extends R> converter) {
			return new ExecutableUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType, this.resultConverter.andThen(converter));
		}

		@Override
		@SuppressWarnings("NullAway")
		public @Nullable T findAndModifyValue() {

			return template.findAndModify(query, update,
					findAndModifyOptions != null ? findAndModifyOptions : new FindAndModifyOptions(), targetType,
					getCollectionName(), resultConverter);
		}

		@Override
		@SuppressWarnings({ "unchecked", "NullAway" })
		public @Nullable T findAndReplaceValue() {

			return (T) template.findAndReplace(query, replacement,
					findAndReplaceOptions != null ? findAndReplaceOptions : FindAndReplaceOptions.empty(), (Class) domainType,
					getCollectionName(), targetType, (QueryResultConverter) resultConverter);
		}

		@Override
		@SuppressWarnings({ "unchecked", "NullAway" })
		public UpdateResult replaceFirst() {

			if (replacement != null) {
				return template.replace(query, domainType, replacement,
						findAndReplaceOptions != null ? findAndReplaceOptions : ReplaceOptions.none(), getCollectionName());
			}

			return template.replace(query, domainType, update,
					findAndReplaceOptions != null ? findAndReplaceOptions : ReplaceOptions.none(), getCollectionName());
		}

		@SuppressWarnings("NullAway")
		private UpdateResult doUpdate(boolean multi, boolean upsert) {
			return template.doUpdate(getCollectionName(), query, update, domainType, upsert, multi);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		}
	}
}
