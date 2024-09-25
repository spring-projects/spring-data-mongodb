/*
 * Copyright 2010-2024 the original author or authors.
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
 * @author Mark Pollak
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class FindAndModifyOptions {

	private boolean returnNew;
	private boolean upsert;
	private boolean remove;

	private @Nullable Collation collation;

	private static final FindAndModifyOptions NONE = new FindAndModifyOptions() {

		private static final String ERROR_MSG = "FindAndModifyOptions.none() cannot be changed; Please use FindAndModifyOptions.options() instead";

		@Override
		public FindAndModifyOptions returnNew(boolean returnNew) {
			throw new UnsupportedOperationException(ERROR_MSG);
		}

		@Override
		public FindAndModifyOptions upsert(boolean upsert) {
			throw new UnsupportedOperationException(ERROR_MSG);
		}

		@Override
		public FindAndModifyOptions remove(boolean remove) {
			throw new UnsupportedOperationException(ERROR_MSG);
		}

		@Override
		public FindAndModifyOptions collation(@Nullable Collation collation) {
			throw new UnsupportedOperationException(ERROR_MSG);
		}
	};

	/**
	 * Static factory method to create a FindAndModifyOptions instance
	 *
	 * @return new instance of {@link FindAndModifyOptions}.
	 */
	public static FindAndModifyOptions options() {
		return new FindAndModifyOptions();
	}

	/**
	 * Static factory method returning an unmodifiable {@link FindAndModifyOptions} instance.
	 *
	 * @return unmodifiable {@link FindAndModifyOptions} instance.
	 * @since 2.2
	 */
	public static FindAndModifyOptions none() {
		return NONE;
	}

	/**
	 * Create new {@link FindAndModifyOptions} based on option of given {@literal source}.
	 *
	 * @param source can be {@literal null}.
	 * @return new instance of {@link FindAndModifyOptions}.
	 * @since 2.0
	 */
	public static FindAndModifyOptions of(@Nullable FindAndModifyOptions source) {

		FindAndModifyOptions options = new FindAndModifyOptions();
		if (source == null) {
			return options;
		}

		options.returnNew = source.returnNew;
		options.upsert = source.upsert;
		options.remove = source.remove;
		options.collation = source.collation;

		return options;
	}

	public FindAndModifyOptions returnNew(boolean returnNew) {
		this.returnNew = returnNew;
		return this;
	}

	public FindAndModifyOptions upsert(boolean upsert) {
		this.upsert = upsert;
		return this;
	}

	public FindAndModifyOptions remove(boolean remove) {
		this.remove = remove;
		return this;
	}

	/**
	 * Define the {@link Collation} specifying language-specific rules for string comparison.
	 *
	 * @param collation can be {@literal null}.
	 * @return this.
	 * @since 2.0
	 */
	public FindAndModifyOptions collation(@Nullable Collation collation) {

		this.collation = collation;
		return this;
	}

	public boolean isReturnNew() {
		return returnNew;
	}

	public boolean isUpsert() {
		return upsert;
	}

	public boolean isRemove() {
		return remove;
	}

	/**
	 * Get the {@link Collation} specifying language-specific rules for string comparison.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	public Optional<Collation> getCollation() {
		return Optional.ofNullable(collation);
	}

}
