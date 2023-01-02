/*
 * Copyright 2019-2023 the original author or authors.
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
import java.util.function.Consumer;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;

/**
 * {@link ReactiveChangeStreamOperation} allows creation and execution of reactive MongoDB
 * <a href="https://docs.mongodb.com/manual/changeStreams/">Change Stream</a> operations in a fluent API style. <br />
 * The starting {@literal domainType} is used for mapping a potentially given
 * {@link org.springframework.data.mongodb.core.aggregation.TypedAggregation} used for filtering. By default, the
 * originating {@literal domainType} is also used for mapping back the result from the {@link org.bson.Document}.
 * However, it is possible to define an different {@literal returnType} via {@code as}.<br />
 * The collection to operate on is optional in which case call collection with the actual database are watched, use
 * {@literal watchCollection} to define a fixed collection.
 *
 * <pre>
 *     <code>
 *         changeStream(Jedi.class)
 *             .watchCollection("star-wars")
 *             .filter(where("operationType").is("insert"))
 *             .resumeAt(Instant.now())
 *             .listen();
 *     </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @since 2.2
 */
public interface ReactiveChangeStreamOperation {

	/**
	 * Start creating a change stream operation for the given {@literal domainType} watching all collections within the
	 * database. <br />
	 * Consider limiting events be defining a {@link ChangeStreamWithCollection#watchCollection(String) collection} and/or
	 * {@link ChangeStreamWithFilterAndProjection#filter(CriteriaDefinition) filter}.
	 *
	 * @param domainType must not be {@literal null}. Use {@link org.bson.Document} to obtain raw elements.
	 * @return new instance of {@link ReactiveChangeStream}. Never {@literal null}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ReactiveChangeStream<T> changeStream(Class<T> domainType);

	/**
	 * Compose change stream execution by calling one of the terminating methods.
	 */
	interface TerminatingChangeStream<T> {

		/**
		 * Start listening to changes. The stream will not be completed unless the {@link org.reactivestreams.Subscription}
		 * is {@link org.reactivestreams.Subscription#cancel() canceled}.
		 * <br />
		 * However, the stream may become dead, or invalid, if all watched collections, databases are dropped.
		 */
		Flux<ChangeStreamEvent<T>> listen();
	}

	/**
	 * Collection override (optional).
	 */
	interface ChangeStreamWithCollection<T> {

		/**
		 * Explicitly set the name of the collection to watch.<br />
		 * Skip this step to watch all collections within the database.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link ChangeStreamWithFilterAndProjection}.
		 * @throws IllegalArgumentException if {@code collection} is {@literal null}.
		 */
		ChangeStreamWithFilterAndProjection<T> watchCollection(String collection);

		/**
		 * Set the the collection to watch. Collection name is derived from the {@link Class entityClass}.<br />
		 * Skip this step to watch all collections within the database.
		 *
		 * @param entityClass must not be {@literal null}.
		 * @return new instance of {@link ChangeStreamWithFilterAndProjection}.
		 * @throws IllegalArgumentException if {@code entityClass} is {@literal null}.
		 */
		ChangeStreamWithFilterAndProjection<T> watchCollection(Class<?> entityClass);
	}

	/**
	 * Provide a filter for limiting results (optional).
	 */
	interface ChangeStreamWithFilterAndProjection<T> extends ResumingChangeStream<T>, TerminatingChangeStream<T> {

		/**
		 * Use an {@link Aggregation} to filter matching events.
		 *
		 * @param by must not be {@literal null}.
		 * @return new instance of {@link ChangeStreamWithFilterAndProjection}.
		 * @throws IllegalArgumentException if the given {@link Aggregation} is {@literal null}.
		 */
		ChangeStreamWithFilterAndProjection<T> filter(Aggregation by);

		/**
		 * Use a {@link CriteriaDefinition critera} to filter matching events via an
		 * {@link org.springframework.data.mongodb.core.aggregation.MatchOperation}.
		 *
		 * @param by must not be {@literal null}.
		 * @return new instance of {@link ChangeStreamWithFilterAndProjection}.
		 * @throws IllegalArgumentException if the given {@link CriteriaDefinition} is {@literal null}.
		 */
		ChangeStreamWithFilterAndProjection<T> filter(CriteriaDefinition by);

		/**
		 * Define the target type fields should be mapped to.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link ChangeStreamWithFilterAndProjection}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> ChangeStreamWithFilterAndProjection<R> as(Class<R> resultType);
	}

	/**
	 * Resume a change stream. (optional).
	 */
	interface ResumingChangeStream<T> extends TerminatingChangeStream<T> {

		/**
		 * Resume the change stream at a given point.
		 *
		 * @param token an {@link Instant} or {@link BsonTimestamp}
		 * @return new instance of {@link TerminatingChangeStream}.
		 * @see ChangeStreamOptionsBuilder#resumeAt(Instant)
		 * @see ChangeStreamOptionsBuilder#resumeAt(BsonTimestamp)
		 * @throws IllegalArgumentException if the given beacon is neither {@link Instant} nor {@link BsonTimestamp}.
		 */
		TerminatingChangeStream<T> resumeAt(Object token);

		/**
		 * Resume the change stream after a given point.
		 *
		 * @param token an {@link Instant} or {@link BsonTimestamp}
		 * @return new instance of {@link TerminatingChangeStream}.
		 * @see ChangeStreamOptionsBuilder#resumeAfter(BsonValue)
		 * @see ChangeStreamOptionsBuilder#resumeToken(BsonValue)
		 * @throws IllegalArgumentException if the given beacon not a {@link BsonValue}.
		 */
		TerminatingChangeStream<T> resumeAfter(Object token);

		/**
		 * Start the change stream after a given point.
		 *
		 * @param token an {@link Instant} or {@link BsonTimestamp}
		 * @return new instance of {@link TerminatingChangeStream}.
		 * @see ChangeStreamOptionsBuilder#startAfter(BsonValue) (BsonValue)
		 * @throws IllegalArgumentException if the given beacon not a {@link BsonValue}.
		 */
		TerminatingChangeStream<T> startAfter(Object token);
	}

	/**
	 * Provide some options.
	 */
	interface ChangeStreamWithOptions<T> {

		/**
		 * Provide some options via the callback by modifying the given {@link ChangeStreamOptionsBuilder}. Previously
		 * defined options like a {@link ResumingChangeStream#resumeAfter(Object) resumeToken} are carried over to the
		 * builder and can be overwritten via eg. {@link ChangeStreamOptionsBuilder#resumeToken(BsonValue)}.
		 *
		 * @param optionsConsumer never {@literal null}.
		 * @return new instance of {@link ReactiveChangeStream}.
		 */
		ReactiveChangeStream<T> withOptions(Consumer<ChangeStreamOptionsBuilder> optionsConsumer);
	}

	/**
	 * {@link ReactiveChangeStream} provides methods for constructing change stream operations in a fluent way.
	 */
	interface ReactiveChangeStream<T> extends ChangeStreamWithOptions<T>, ChangeStreamWithCollection<T>,
			TerminatingChangeStream<T>, ResumingChangeStream<T>, ChangeStreamWithFilterAndProjection<T> {}
}
