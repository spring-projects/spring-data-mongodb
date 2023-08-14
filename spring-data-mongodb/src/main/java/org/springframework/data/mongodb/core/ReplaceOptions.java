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

import org.springframework.data.mongodb.core.query.Query;

/**
 * Options for {@link org.springframework.data.mongodb.core.MongoOperations#replace(Query, Object) replace operations}. Defaults to
 * <dl>
 * <dt>upsert</dt>
 * <dd>false</dd>
 * </dl>
 *
 * @author Jakub Zurawa
 * @author Christoph Strob
 * @since 4.2
 */
public class ReplaceOptions {

	private boolean upsert;

	private static final ReplaceOptions NONE = new ReplaceOptions() {

		private static final String ERROR_MSG = "ReplaceOptions.none() cannot be changed; Please use ReplaceOptions.options() instead";

		@Override
		public ReplaceOptions upsert() {
			throw new UnsupportedOperationException(ERROR_MSG);
		}
	};

	/**
	 * Static factory method to create a {@link ReplaceOptions} instance.
	 * <dl>
	 * <dt>upsert</dt>
	 * <dd>false</dd>
	 * </dl>
	 *
	 * @return new instance of {@link ReplaceOptions}.
	 */
	public static ReplaceOptions replaceOptions() {
		return new ReplaceOptions();
	}

	/**
	 * Static factory method returning an unmodifiable {@link ReplaceOptions} instance.
	 *
	 * @return unmodifiable {@link ReplaceOptions} instance.
	 */
	public static ReplaceOptions none() {
		return NONE;
	}

	/**
	 * Insert a new document if not exists.
	 *
	 * @return this.
	 */
	public ReplaceOptions upsert() {

		this.upsert = true;
		return this;
	}

	/**
	 * Get the bit indicating if to create a new document if not exists.
	 *
	 * @return {@literal true} if set.
	 */
	public boolean isUpsert() {
		return upsert;
	}

}
