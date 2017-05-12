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

import org.springframework.data.mongodb.core.ExecutableAggregationOperationBuilder.AggregationOperationBuilder;
import org.springframework.data.mongodb.core.ExecutableFindOperationBuilder.FindOperationBuilder;
import org.springframework.data.mongodb.core.ExecutableRemoveOperationBuilder.RemoveOperationBuilder;
import org.springframework.data.mongodb.core.ExecutableUpdateOperationBuilder.UpdateOperationBuilder;

/**
 * Stripped down interface providing access to a {@literal builder} based fluent API that specifies a basic set of
 * MongoDB operations.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface FluentMongoOperations {

	/**
	 * Entry point for constructing and executing queries for a given domain type.
	 *
	 * @param domainType must not be {@literal null}.
	 * @param <S>
	 * @return new instance of {@link FindExecutionBuilder}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 * @since 2.0
	 */
	<T> FindOperationBuilder<T> query(Class<T> domainType);

	/**
	 * Entry point for constructing and executing updates for a given domain type.
	 *
	 * @param domainType must not be {@literal null}.
	 * @param <T>
	 * @return new instance of {@link ExecutableUpdateOperationBuilder}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 * @since 2.0
	 */
	<T> UpdateOperationBuilder<T> update(Class<T> domainType);

	/**
	 * Entry point for constructing and executing deletes for a given domain type.
	 *
	 * @param domainType must not be {@literal null}.
	 * @param <T>
	 * @return new instance of {@link RemoveOperationBuilder}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 * @since 2.0
	 */
	<T> RemoveOperationBuilder<T> remove(Class<T> domainType);

	/**
	 * Entry point for constructing and executing aggregation operations.
	 *
	 * @param domainType must not be {@literal null}.
	 * @param <T>
	 * @return new instance of {@link AggregationOperation}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 * @since 2.0
	 */
	<T> AggregationOperationBuilder<T> aggregateAndReturn(Class<T> domainType);
}
