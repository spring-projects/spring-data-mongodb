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

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;

import com.mongodb.client.MongoCollection;

/**
 * {@link ExecutableFindOperation} allows creation and execution of MongoDB find operations in a fluent API style.
 * <br />
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
 *             .matching(query(where("firstname").is("luke")))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ExecutableFindOperation {

	/**
	 * Start creating a find operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ExecutableFind}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ExecutableFind<T> query(Class<T> domainType);

	/**
	 * Trigger find execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingFind<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 */
		default Optional<T> one() {
			return Optional.ofNullable(oneValue());
		}

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@literal null} if no match found.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 */
		@Nullable
		T oneValue();

		/**
		 * Get the first or no result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 */
		default Optional<T> first() {
			return Optional.ofNullable(firstValue());
		}

		/**
		 * Get the first or no result.
		 *
		 * @return {@literal null} if no match found.
		 */
		@Nullable
		T firstValue();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		List<T> all();

		/**
		 * Stream all matching elements.
		 *
		 * @return a {@link Stream} that wraps the a Mongo DB {@link com.mongodb.Cursor} that needs to be closed. Never
		 *         {@literal null}.
		 */
		Stream<T> stream();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		long count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		boolean exists();
	}

	/**
	 * Trigger geonear execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingFindNear<T> {

		/**
		 * Find all matching elements and return them as {@link org.springframework.data.geo.GeoResult}.
		 *
		 * @return never {@literal null}.
		 */
		GeoResults<T> all();
	}

	/**
	 * Terminating operations invoking the actual query execution.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
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
		 * Set the filter query for the geoNear execution.
		 *
		 * @param nearQuery must not be {@literal null}.
		 * @return new instance of {@link TerminatingFindNear}.
		 * @throws IllegalArgumentException if nearQuery is {@literal null}.
		 */
		TerminatingFindNear<T> near(NearQuery nearQuery);
	}

	/**
	 * Collection override (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.0
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
	 * Result type override (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.0
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
		 * Finds the distinct values for a specified {@literal field} across a single {@link MongoCollection} or view.
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
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		TerminatingDistinct<T> matching(Query query);
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
		 * @return empty {@link List} if not match found. Never {@literal null}.
		 * @throws DataAccessException if eg. result cannot be converted correctly which may happen if the document contains
		 *           {@link String} whereas the result type is specified as {@link Long}.
		 */
		List<T> all();
	}

	/**
	 * {@link ExecutableFind} provides methods for constructing lookup operations in a fluent way.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface ExecutableFind<T> extends FindWithCollection<T>, FindWithProjection<T>, FindDistinct {}
}
