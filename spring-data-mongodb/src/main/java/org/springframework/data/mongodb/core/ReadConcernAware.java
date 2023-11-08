/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.lang.Nullable;

import com.mongodb.ReadConcern;

/**
 * Interface to be implemented by any object that wishes to expose the {@link ReadConcern}.
 * <p>
 * Typically implemented by cursor or query preparer objects.
 *
 * @author Mark Paluch
 * @since 4.1
 * @see org.springframework.data.mongodb.core.query.Query
 * @see org.springframework.data.mongodb.core.aggregation.AggregationOptions
 */
public interface ReadConcernAware {

	/**
	 * @return {@literal true} if a {@link ReadConcern} is set.
	 */
	default boolean hasReadConcern() {
		return getReadConcern() != null;
	}

	/**
	 * @return the {@link ReadConcern} to apply or {@literal null} if none set.
	 */
	@Nullable
	ReadConcern getReadConcern();
}
