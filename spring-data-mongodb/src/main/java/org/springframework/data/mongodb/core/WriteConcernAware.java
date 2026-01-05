/*
 * Copyright 2024-present the original author or authors.
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

import com.mongodb.WriteConcern;

/**
 * Interface indicating a component that contains and exposes an {@link WriteConcern}.
 *
 * @author Christoph Strobl
 * @since 4.3
 */
public interface WriteConcernAware {

	/**
	 * @return the {@link WriteConcern} to apply or {@literal null} if none set.
	 */
	@Nullable
	WriteConcern getWriteConcern();

	/**
	 * @return {@literal true} if a {@link com.mongodb.WriteConcern} is set.
	 */
	default boolean hasWriteConcern() {
		return getWriteConcern() != null;
	}
}
