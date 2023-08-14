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
package org.springframework.data.mongodb.core;

/**
 * Options for
 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace</a>.
 * <br />
 * Defaults to
 * <dl>
 * <dt>returnNew</dt>
 * <dd>false</dd>
 * <dt>upsert</dt>
 * <dd>false</dd>
 * </dl>
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public class FindAndReplaceOptions extends ReplaceOptions {

	private boolean returnNew;

	private static final FindAndReplaceOptions NONE = new FindAndReplaceOptions() {

		private static final String ERROR_MSG = "FindAndReplaceOptions.none() cannot be changed; Please use FindAndReplaceOptions.options() instead";

		@Override
		public FindAndReplaceOptions returnNew() {
			throw new UnsupportedOperationException(ERROR_MSG);
		}

		@Override
		public FindAndReplaceOptions upsert() {
			throw new UnsupportedOperationException(ERROR_MSG);
		}
	};

	/**
	 * Static factory method to create a {@link FindAndReplaceOptions} instance.
	 * <dl>
	 * <dt>returnNew</dt>
	 * <dd>false</dd>
	 * <dt>upsert</dt>
	 * <dd>false</dd>
	 * </dl>
	 *
	 * @return new instance of {@link FindAndReplaceOptions}.
	 */
	public static FindAndReplaceOptions options() {
		return new FindAndReplaceOptions();
	}

	/**
	 * Static factory method returning an unmodifiable {@link FindAndReplaceOptions} instance.
	 *
	 * @return unmodifiable {@link FindAndReplaceOptions} instance.
	 * @since 2.2
	 */
	public static FindAndReplaceOptions none() {
		return NONE;
	}

	/**
	 * Static factory method to create a {@link FindAndReplaceOptions} instance with
	 * <dl>
	 * <dt>returnNew</dt>
	 * <dd>false</dd>
	 * <dt>upsert</dt>
	 * <dd>false</dd>
	 * </dl>
	 *
	 * @return new instance of {@link FindAndReplaceOptions}.
	 */
	public static FindAndReplaceOptions empty() {
		return new FindAndReplaceOptions();
	}

	/**
	 * Return the replacement document.
	 *
	 * @return this.
	 */
	public FindAndReplaceOptions returnNew() {

		this.returnNew = true;
		return this;
	}

	/**
	 * Insert a new document if not exists.
	 *
	 * @return this.
	 */
	public FindAndReplaceOptions upsert() {

		super.upsert();
		return this;
	}

	/**
	 * Get the bit indicating to return the replacement document.
	 *
	 * @return {@literal true} if set.
	 */
	public boolean isReturnNew() {
		return returnNew;
	}

}
