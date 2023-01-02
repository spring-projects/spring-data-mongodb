/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.List;

import org.bson.Document;

/**
 * Interface fixing must have operations for {@literal updates} as implemented via {@link Update}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.2
 */
public interface UpdateDefinition {

	/**
	 * If {@literal true} prevents a write operation that affects <strong>multiple</strong> documents from yielding to
	 * other reads or writes once the first document is written. <br />
	 *
	 * @return {@literal true} if update isolated is set.
	 */
	Boolean isIsolated();

	/**
	 * @return the actual update in its native {@link Document} format. Never {@literal null}.
	 */
	Document getUpdateObject();

	/**
	 * Check if a given {@literal key} is modified by applying the update.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal true} if the actual {@link UpdateDefinition} attempts to modify the given {@literal key}.
	 */
	boolean modifies(String key);

	/**
	 * Increment the value of a given {@literal key} by {@code 1}.
	 *
	 * @param key must not be {@literal null}.
	 */
	void inc(String key);

	/**
	 * Get the specification which elements to modify in an array field. {@link ArrayFilter} are passed directly to the
	 * driver without further type or field mapping.
	 *
	 * @return never {@literal null}.
	 * @since 2.2
	 */
	List<ArrayFilter> getArrayFilters();

	/**
	 * @return {@literal true} if {@link UpdateDefinition} contains {@link #getArrayFilters() array filters}.
	 * @since 2.2
	 */
	default boolean hasArrayFilters() {
		return !getArrayFilters().isEmpty();
	}

	/**
	 * A filter to specify which elements to modify in an array field.
	 *
	 * @since 2.2
	 */
	interface ArrayFilter {

		/**
		 * Get the {@link Document} representation of the filter to apply. The returned {@link Document} is used directly
		 * with the driver without further type or field mapping.
		 *
		 * @return never {@literal null}.
		 */
		Document asDocument();
	}
}
