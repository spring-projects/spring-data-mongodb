/*
 * Copyright 2017-2025 the original author or authors.
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
import reactor.core.publisher.Mono;

import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Window;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Contract;

/**
 * {@link ReactiveFindOperation} allows creation and execution of reactive MongoDB find operations in a fluent API
 * style. <br />
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching} into the
 * MongoDB specific representation. By default, the originating {@literal domainType} is also used for mapping back the
 * result from the {@link org.bson.Document}. However, it is possible to define an different {@literal returnType} via
 * {@code as} to mapping the result.<br />
 * The collection to operate on is by default derived from the initial {@literal domainType} and can be defined there
 * via {@link org.springframework.data.mongodb.core.mapping.Document}. Using {@code inCollection} allows to override the
 * collection name for the execution.
 *
 * <pre>
 *     <code>
 *         query(Human.class)
 *             .inCollection("star-wars")
 *             .as(Jedi.class)
 *             .matching(where("firstname").is("luke"))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Juergen Zimmermann
 * @since 2.0
 */
public interface ReactiveFindOperation {

	/**
	 * Start creating a find operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ReactiveFind}. Never {@literal null}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ReactiveFind<T> query(Class<T> domainType);

	/**
	 * Compose find execution by calling one of the terminating methods.
	 */
	interface TerminatingFind<T> extends TerminatingResults<T>, TerminatingProjection {

	}

	/**
	 * Compose find execution by calling one of the terminating methods.
	 *
	 * @since 5.0
	 */
	interface TerminatingResults<T> {

		/**
		 * Map the query result to a different type using {@link QueryResultConverter}.
		 *
		 * @param <R> {@link Class type} of the result.
		 * @param converter the converter, must not be {@literal null}.
		 * @return new instance of {@link TerminatingResults}.
		 * @throws IllegalArgumentException if {@link QueryResultConverter converter} is {@literal null}.
		 * @since 5.0
		 */
		@Contract("_ -> new")
		<R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter);

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@link Mono#empty()} if no match found. Never {@literal null}.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 */
		Mono<T> one();

		/**
		 * Get the first or no result.
		 *
		 * @return {@link Mono#empty()} if no match found. Never {@literal null}.
		 */
		Mono<T> first();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		Flux<T> all();

		/**
		 * Return a scroll of elements either starting or resuming at {@link ScrollPosition}.
		 * <p>
		 * When using {@link KeysetScrollPosition}, make sure to use non-nullable
		 * {@link org.springframework.data.domain.Sort sort properties} as MongoDB does not support criteria to reconstruct
		 * a query result from absent document fields or {@literal null} values through {@code $gt/$lt} operators.
		 *
		 * @param scrollPosition the scroll position.
		 * @return a scroll of the resulting elements.
		 * @since 4.1
		 * @see org.springframework.data.domain.OffsetScrollPosition
		 * @see org.springframework.data.domain.KeysetScrollPosition
		 */
		Mono<Window<T>> scroll(ScrollPosition scrollPosition);

		/**
		 * Get all matching elements using a {@link com.mongodb.CursorType#TailableAwait tailable cursor}. The stream will
		 * not be completed unless the {@link org.reactivestreams.Subscription} is
		 * {@link org.reactivestreams.Subscription#cancel() canceled}. <br />
		 * However, the stream may become dead, or invalid, if either the query returns no match or the cursor returns the
		 * document at the "end" of the collection and then the application deletes that document. <br />
		 * A stream that is no longer in use must be {@link reactor.core.Disposable#dispose()} disposed} otherwise the
		 * streams will linger and exhaust resources. <br/>
		 * <strong>NOTE:</strong> Requires a capped collection.
		 *
		 * @return the {@link Flux} emitting converted objects.
		 * @since 2.1
		 */
		Flux<T> tail();

	}

	/**
	 * Compose find execution by calling one of the terminating methods.
	 *
	 * @since 5.0
	 */
	interface TerminatingProjection {

		/**
		 * Get the number of matching elements. <br />
		 * This method uses an
		 * {@link com.mongodb.reactivestreams.client.MongoCollection#countDocuments(org.bson.conversions.Bson, com.mongodb.client.model.CountOptions)
		 * aggregation execution} even for empty {@link Query queries} which may have an impact on performance, but
		 * guarantees shard, session and transaction compliance. In case an inaccurate count satisfies the applications
		 * needs use {@link ReactiveMongoOperations#estimatedCount(String)} for empty queries instead.
		 *
		 * @return {@link Mono} emitting total number of matching elements. Never {@literal null}.
		 */
		Mono<Long> count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@link Mono} emitting {@literal true} if at least one matching element exists. Never {@literal null}.
		 */
		Mono<Boolean> exists();
	}

	/**
	 * Compose geonear execution by calling one of the terminating methods.
	 */
	interface TerminatingFindNear<T> {

		/**
		 * Map the query result to a different type using {@link QueryResultConverter}.
		 *
		 * @param <R> {@link Class type} of the result.
		 * @param converter the converter, must not be {@literal null}.
		 * @return new instance of {@link ExecutableFindOperation.TerminatingFindNear}.
		 * @throws IllegalArgumentException if {@link QueryResultConverter converter} is {@literal null}.
		 * @since 5.0
		 */
		@Contract("_ -> new")
		<R> TerminatingFindNear<R> map(QueryResultConverter<? super T, ? extends R> converter);

		/**
		 * Find all matching elements and return them as {@link org.springframework.data.geo.GeoResult}.
		 *
		 * @return never {@literal null}.
		 */
		Flux<GeoResult<T>> all();

		/**
		 * Count matching elements.
		 *
		 * @return number of elements matching the query.
		 * @since 5.0
		 */
		Mono<Long> count();
	}

	/**
	 * Provide a {@link Query} override (optional).
	 */
	interface FindWithQuery<T> extends TerminatingFind<T> {

		/**
		 * Set the filter query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingFind}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingFind<T> matching(Query query);

		/**
		 * Set the filter {@link CriteriaDefinition criteria} to be used.
		 *
		 * @param criteria must not be {@literal null}.
		 * @return new instance of {@link TerminatingFind}.
		 * @throws IllegalArgumentException if criteria is {@literal null}.
		 * @since 3.0
		 */
		default TerminatingFind<T> matching(CriteriaDefinition criteria) {
			return matching(Query.query(criteria));
		}

		/**
		 * Set the filter query for the geoNear execution.
		 *
		 * @param nearQuery must not be {@literal null}.
		 * @return new instance of {@link TerminatingFindNear}.
		 * @throws IllegalArgumentException if nearQuery is {@literal null}.
		 */
		TerminatingFindNear<T> near(NearQuery nearQuery);
	}

	/**
	 * Collection override (optional).
	 */
	interface FindWithCollection<T> extends FindWithQuery<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link FindWithProjection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		FindWithProjection<T> inCollection(String collection);
	}

	/**
	 * Result type override (optional).
	 */
	interface FindWithProjection<T> extends FindWithQuery<T>, FindDistinct {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link FindWithProjection}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> FindWithQuery<R> as(Class<R> resultType);
	}

	/**
	 * Distinct Find support.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface FindDistinct {

		/**
		 * Finds the distinct values for a specified {@literal field} across a single
		 * {@link com.mongodb.reactivestreams.client.MongoCollection} or view.
		 *
		 * @param field name of the field. Must not be {@literal null}.
		 * @return new instance of {@link TerminatingDistinct}.
		 * @throws IllegalArgumentException if field is {@literal null}.
		 */
		TerminatingDistinct<Object> distinct(String field);
	}

	/**
	 * Result type override. Optional.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface DistinctWithProjection {

		/**
		 * Define the target type the result should be mapped to. <br />
		 * Skip this step if you are anyway fine with the default conversion.
		 * <dl>
		 * <dt>{@link Object} (the default)</dt>
		 * <dd>Result is mapped according to the {@link org.bson.BsonType} converting eg. {@link org.bson.BsonString} into
		 * plain {@link String}, {@link org.bson.BsonInt64} to {@link Long}, etc. always picking the most concrete type with
		 * respect to the domain types property.<br />
		 * Any {@link org.bson.BsonType#DOCUMENT} is run through the {@link org.springframework.data.convert.EntityReader}
		 * to obtain the domain type. <br />
		 * Using {@link Object} also works for non strictly typed fields. Eg. a mixture different types like fields using
		 * {@link String} in one {@link org.bson.Document} while {@link Long} in another.</dd>
		 * <dt>Any Simple type like {@link String}, {@link Long}, ...</dt>
		 * <dd>The result is mapped directly by the MongoDB Java driver and the {@link org.bson.codecs.CodeCodec Codecs} in
		 * place. This works only for results where all documents considered for the operation use the very same type for
		 * the field.</dd>
		 * <dt>Any Domain type</dt>
		 * <dd>Domain types can only be mapped if the if the result of the actual {@code distinct()} operation returns
		 * {@link org.bson.BsonType#DOCUMENT}.</dd>
		 * <dt>{@link org.bson.BsonValue}</dt>
		 * <dd>Using {@link org.bson.BsonValue} allows retrieval of the raw driver specific format, which returns eg.
		 * {@link org.bson.BsonString}.</dd>
		 * </dl>
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link TerminatingDistinct}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> TerminatingDistinct<R> as(Class<R> resultType);
	}

	/**
	 * Result restrictions. Optional.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface DistinctWithQuery<T> extends DistinctWithProjection {

		/**
		 * Set the filter query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingDistinct}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingDistinct<T> matching(Query query);

		/**
		 * Set the filter {@link CriteriaDefinition criteria} to be used.
		 *
		 * @param criteria must not be {@literal null}.
		 * @return new instance of {@link TerminatingDistinct}.
		 * @throws IllegalArgumentException if criteria is {@literal null}.
		 * @since 3.0
		 */
		default TerminatingDistinct<T> matching(CriteriaDefinition criteria) {
			return matching(Query.query(criteria));
		}
	}

	/**
	 * Terminating distinct find operations.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface TerminatingDistinct<T> extends DistinctWithQuery<T> {

		/**
		 * Get all matching distinct field values.
		 *
		 * @return empty {@link Flux} if not match found. Never {@literal null}.
		 */
		Flux<T> all();
	}

	/**
	 * {@link ReactiveFind} provides methods for constructing lookup operations in a fluent way.
	 */
	interface ReactiveFind<T> extends FindWithCollection<T>, FindWithProjection<T>, FindDistinct {}
}
