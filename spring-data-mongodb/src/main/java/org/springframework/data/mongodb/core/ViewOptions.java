/*
 * Copyright 2022-2023 the original author or authors.
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

import java.util.Optional;

import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.lang.Nullable;

/**
 * Immutable object holding additional options to be applied when creating a MongoDB
 * <a href="https://www.mongodb.com/docs/manual/core/views/">views</a>.
 *
 * @author Christoph Strobl
 * @since 4.0
 */
public class ViewOptions {

	private final @Nullable Collation collation;

	static ViewOptions none() {
		return new ViewOptions();
	}

	/**
	 * Creates new instance of {@link ViewOptions}.
	 */
	public ViewOptions() {
		this(null);
	}

	private ViewOptions(@Nullable Collation collation) {
		this.collation = collation;
	}

	/**
	 * Get the {@link Collation} to be set.
	 *
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Collation> getCollation() {
		return Optional.ofNullable(collation);
	}

	/**
	 * @param collation the {@link Collation} to use for language-specific string comparison.
	 * @return new instance of {@link ViewOptions}.
	 */
	public ViewOptions collation(Collation collation) {
		return new ViewOptions(collation);
	}
}
