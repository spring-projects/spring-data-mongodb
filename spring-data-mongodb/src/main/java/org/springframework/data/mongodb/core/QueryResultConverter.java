/*
 * Copyright 2025 the original author or authors.
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

import org.bson.Document;

/**
 * Converter for MongoDB query results.
 * <p>
 * This is a functional interface that allows for mapping a {@link Document} to a result type.
 * {@link #mapDocument(Document, ConversionResultSupplier) row mapping} can obtain upstream a
 * {@link ConversionResultSupplier upstream converter} to enrich the final result object. This is useful when e.g.
 * wrapping result objects where the wrapper needs to obtain information from the actual {@link Document}.
 *
 * @param <T> object type accepted by this converter.
 * @param <R> the returned result type.
 * @author Mark Paluch
 * @since 5.0
 */
@FunctionalInterface
public interface QueryResultConverter<T, R> {

	/**
	 * Returns a function that returns the materialized entity.
	 *
	 * @param <T> the type of the input and output entity to the function.
	 * @return a function that returns the materialized entity.
	 */
	@SuppressWarnings("unchecked")
	static <T> QueryResultConverter<T, T> entity() {
		return (QueryResultConverter<T, T>) EntityResultConverter.INSTANCE;
	}

	/**
	 * Map a {@link Document} that is read from the MongoDB query/aggregation operation to a query result.
	 *
	 * @param document the raw document from the MongoDB query/aggregation result.
	 * @param reader reader object that supplies an upstream result from an earlier converter.
	 * @return the mapped result.
	 */
	R mapDocument(Document document, ConversionResultSupplier<T> reader);

	/**
	 * Returns a composed function that first applies this function to its input, and then applies the {@code after}
	 * function to the result. If evaluation of either function throws an exception, it is relayed to the caller of the
	 * composed function.
	 *
	 * @param <V> the type of output of the {@code after} function, and of the composed function.
	 * @param after the function to apply after this function is applied.
	 * @return a composed function that first applies this function and then applies the {@code after} function.
	 */
	default <V> QueryResultConverter<T, V> andThen(QueryResultConverter<? super R, ? extends V> after) {
		return (row, reader) -> after.mapDocument(row, () -> mapDocument(row, reader));
	}

	/**
	 * A supplier that converts a {@link Document} into {@code T}. Allows for lazy reading of query results.
	 *
	 * @param <T> type of the returned result.
	 */
	interface ConversionResultSupplier<T> {

		/**
		 * Obtain the upstream conversion result.
		 *
		 * @return the upstream conversion result.
		 */
		T get();

	}

}
