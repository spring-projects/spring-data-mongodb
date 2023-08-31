/*
 * Copyright 2017-2023 the original author or authors.
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

import reactor.core.publisher.Mono;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.result.UpdateResult;

/**
 * Implementation of {@link ReactiveUpdateOperation}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class ReactiveUpdateOperationSupport implements ReactiveUpdateOperation {

	private static final Query ALL_QUERY = new Query();

	private final ReactiveMongoTemplate template;

	ReactiveUpdateOperationSupport(ReactiveMongoTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveUpdate<T> update(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveUpdateSupport<>(template, domainType, ALL_QUERY, null, null, null, null, null, domainType);
	}

	static class ReactiveUpdateSupport<T>
			implements ReactiveUpdate<T>, UpdateWithCollection<T>, UpdateWithQuery<T>, TerminatingUpdate<T>,
			FindAndReplaceWithOptions<T>, FindAndReplaceWithProjection<T>, TerminatingFindAndReplace<T> {

		private final ReactiveMongoTemplate template;
		private final Class<?> domainType;
		private final Query query;
		private final org.springframework.data.mongodb.core.query.UpdateDefinition update;
		@Nullable private final String collection;
		@Nullable private final FindAndModifyOptions findAndModifyOptions;
		@Nullable private final FindAndReplaceOptions findAndReplaceOptions;
		@Nullable private final Object replacement;
		private final Class<T> targetType;

		ReactiveUpdateSupport(ReactiveMongoTemplate template, Class<?> domainType, Query query, UpdateDefinition update,
				String collection, FindAndModifyOptions findAndModifyOptions, FindAndReplaceOptions findAndReplaceOptions,
				Object replacement, Class<T> targetType) {

			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.update = update;
			this.collection = collection;
			this.findAndModifyOptions = findAndModifyOptions;
			this.findAndReplaceOptions = findAndReplaceOptions;
			this.replacement = replacement;
			this.targetType = targetType;
		}

		@Override
		public TerminatingUpdate<T> apply(org.springframework.data.mongodb.core.query.UpdateDefinition update) {

			Assert.notNull(update, "Update must not be null");

			return new ReactiveUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType);
		}

		@Override
		public UpdateWithQuery<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection must not be null nor empty");

			return new ReactiveUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType);
		}

		@Override
		public Mono<UpdateResult> first() {
			return doUpdate(false, false);
		}

		@Override
		public Mono<UpdateResult> upsert() {
			return doUpdate(true, true);
		}

		@Override
		public Mono<T> findAndModify() {

			String collectionName = getCollectionName();

			return template.findAndModify(query, update,
					findAndModifyOptions != null ? findAndModifyOptions : FindAndModifyOptions.none(), targetType,
					collectionName);
		}

		@Override
		public Mono<T> findAndReplace() {
			return template.findAndReplace(query, replacement,
					findAndReplaceOptions != null ? findAndReplaceOptions : FindAndReplaceOptions.none(), (Class) domainType,
					getCollectionName(), targetType);
		}

		@Override
		public UpdateWithUpdate<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType);
		}

		@Override
		public Mono<UpdateResult> all() {
			return doUpdate(true, false);
		}

		@Override
		public TerminatingFindAndModify<T> withOptions(FindAndModifyOptions options) {

			Assert.notNull(options, "Options must not be null");

			return new ReactiveUpdateSupport<>(template, domainType, query, update, collection, options,
					findAndReplaceOptions, replacement, targetType);
		}

		@Override
		public FindAndReplaceWithProjection<T> replaceWith(T replacement) {

			Assert.notNull(replacement, "Replacement must not be null");

			return new ReactiveUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, targetType);
		}

		@Override
		public FindAndReplaceWithProjection<T> withOptions(FindAndReplaceOptions options) {

			Assert.notNull(options, "Options must not be null");

			return new ReactiveUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions, options,
					replacement, targetType);
		}

		@Override
		public TerminatingReplace withOptions(ReplaceOptions options) {

			FindAndReplaceOptions target = new FindAndReplaceOptions();
			if (options.isUpsert()) {
				target.upsert();
			}
			return new ReactiveUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					target, replacement, targetType);
		}

		@Override
		public <R> FindAndReplaceWithOptions<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "ResultType must not be null");

			return new ReactiveUpdateSupport<>(template, domainType, query, update, collection, findAndModifyOptions,
					findAndReplaceOptions, replacement, resultType);
		}

		@Override
		public Mono <UpdateResult> replaceFirst() {

			if (replacement != null) {
				return template.replace(query, domainType, replacement,
						findAndReplaceOptions != null ? findAndReplaceOptions : ReplaceOptions.none(), getCollectionName());
			}

			return template.replace(query, domainType, update,
					findAndReplaceOptions != null ? findAndReplaceOptions : ReplaceOptions.none(), getCollectionName());
		}

		private Mono<UpdateResult> doUpdate(boolean multi, boolean upsert) {
			return template.doUpdate(getCollectionName(), query, update, domainType, upsert, multi);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		}
	}
}
