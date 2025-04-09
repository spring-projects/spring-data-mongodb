/*
 * Copyright 2021-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import org.jspecify.annotations.Nullable;
import org.springframework.lang.Contract;

/**
 * The source object to resolve document references upon. Encapsulates the actual source and the reference specific
 * values.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public class DocumentReferenceSource {

	private final Object self;

	private final @Nullable Object targetSource;

	/**
	 * Create a new instance of {@link DocumentReferenceSource}.
	 *
	 * @param self the entire wrapper object holding references. Must not be {@literal null}.
	 * @param targetSource the reference value source.
	 */
	DocumentReferenceSource(Object self, @Nullable Object targetSource) {

		this.self = self;
		this.targetSource = targetSource;
	}

	/**
	 * Get the outer document.
	 *
	 * @return never {@literal null}.
	 */
	public Object getSelf() {
		return self;
	}

	/**
	 * Get the actual (property specific) reference value.
	 *
	 * @return can be {@literal null}.
	 */
	public @Nullable Object getTargetSource() {
		return targetSource;
	}

	/**
	 * Dereference a {@code targetSource} if it is a {@link DocumentReferenceSource} or return {@code source} otherwise.
	 *
	 * @param source
	 * @return
	 */
	static @Nullable Object getTargetSource(@Nullable Object source) {
		return source instanceof DocumentReferenceSource referenceSource ? referenceSource.getTargetSource() : source;
	}

	/**
	 * Dereference a {@code self} object if it is a {@link DocumentReferenceSource} or return {@code self} otherwise.
	 *
	 * @param self
	 * @return
	 */
	@Contract("null -> null")
	static @Nullable Object getSelf(@Nullable Object self) {
		return self instanceof DocumentReferenceSource referenceSource ? referenceSource.getSelf() : self;
	}
}
