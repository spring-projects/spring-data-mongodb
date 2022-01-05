/*
 * Copyright 2019-2021 the original author or authors.
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

import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 2.2
 */
class ReactiveChangeStreamOperationSupport implements ReactiveChangeStreamOperation {

	private final ReactiveMongoTemplate template;

	/**
	 * @param template must not be {@literal null}.
	 */
	ReactiveChangeStreamOperationSupport(ReactiveMongoTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveChangeStream<T> changeStream(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");
		return new ReactiveChangeStreamSupport<>(template, domainType, domainType, null, null);
	}

	static class ReactiveChangeStreamSupport<T>
			implements ReactiveChangeStream<T>, ChangeStreamWithFilterAndProjection<T> {

		private final ReactiveMongoTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final @Nullable String collection;
		private final @Nullable ChangeStreamOptions options;

		private ReactiveChangeStreamSupport(ReactiveMongoTemplate template, Class<?> domainType, Class<T> returnType,
				@Nullable String collection, @Nullable ChangeStreamOptions options) {

			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.collection = collection;
			this.options = options;
		}

		@Override
		public ChangeStreamWithFilterAndProjection<T> watchCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty!");

			return new ReactiveChangeStreamSupport<>(template, domainType, returnType, collection, options);
		}

		@Override
		public ChangeStreamWithFilterAndProjection<T> watchCollection(Class<?> entityClass) {

			Assert.notNull(entityClass, "Collection type not be null!");

			return watchCollection(template.getCollectionName(entityClass));
		}

		@Override
		public TerminatingChangeStream<T> resumeAt(Object token) {

			return withOptions(builder -> {

				if (token instanceof Instant) {
					builder.resumeAt((Instant) token);
				} else if (token instanceof BsonTimestamp) {
					builder.resumeAt((BsonTimestamp) token);
				}
			});
		}

		@Override
		public TerminatingChangeStream<T> resumeAfter(Object token) {

			Assert.isInstanceOf(BsonValue.class, token, "Token must be a BsonValue");

			return withOptions(builder -> builder.resumeAfter((BsonValue) token));
		}

		@Override
		public TerminatingChangeStream<T> startAfter(Object token) {

			Assert.isInstanceOf(BsonValue.class, token, "Token must be a BsonValue");

			return withOptions(builder -> builder.startAfter((BsonValue) token));
		}

		@Override
		public ReactiveChangeStreamSupport<T> withOptions(Consumer<ChangeStreamOptionsBuilder> optionsConsumer) {

			ChangeStreamOptionsBuilder builder = initOptionsBuilder();
			optionsConsumer.accept(builder);

			return new ReactiveChangeStreamSupport<>(template, domainType, returnType, collection, builder.build());
		}

		@Override
		public <R> ChangeStreamWithFilterAndProjection<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "ResultType must not be null!");

			return new ReactiveChangeStreamSupport<>(template, domainType, resultType, collection, options);
		}

		@Override
		public ChangeStreamWithFilterAndProjection<T> filter(Aggregation filter) {
			return withOptions(builder -> builder.filter(filter));
		}

		@Override
		public ChangeStreamWithFilterAndProjection<T> filter(CriteriaDefinition by) {

			MatchOperation $match = Aggregation.match(by);
			Aggregation aggregation = !Document.class.equals(domainType) ? Aggregation.newAggregation(domainType, $match)
					: Aggregation.newAggregation($match);
			return filter(aggregation);
		}

		@Override
		public Flux<ChangeStreamEvent<T>> listen() {
			return template.changeStream(collection, options != null ? options : ChangeStreamOptions.empty(), returnType);
		}

		private ChangeStreamOptionsBuilder initOptionsBuilder() {

			ChangeStreamOptionsBuilder builder = ChangeStreamOptions.builder();
			if (options == null) {
				return builder;
			}

			options.getFilter().ifPresent(it -> {
				if (it instanceof Aggregation) {
					builder.filter((Aggregation) it);
				} else {
					builder.filter(((List<Document>) it).toArray(new Document[0]));
				}
			});
			options.getFullDocumentLookup().ifPresent(builder::fullDocumentLookup);
			options.getCollation().ifPresent(builder::collation);

			if (options.isResumeAfter()) {
				options.getResumeToken().ifPresent(builder::resumeAfter);
				options.getResumeBsonTimestamp().ifPresent(builder::resumeAfter);
			} else if (options.isStartAfter()) {
				options.getResumeToken().ifPresent(builder::startAfter);
			} else {
				options.getResumeTimestamp().ifPresent(builder::resumeAt);
				options.getResumeBsonTimestamp().ifPresent(builder::resumeAt);
			}

			return builder;
		}
	}
}
