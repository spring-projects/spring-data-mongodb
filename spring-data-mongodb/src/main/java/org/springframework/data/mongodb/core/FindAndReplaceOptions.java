/*
 * Copyright 2018 the original author or authors.
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

/**
 * Options for
 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>.
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
public class FindAndReplaceOptions {

	private boolean returnNew;
	private boolean upsert;

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

		this.upsert = true;
		return this;
	}

	/**
	 * Get the bit indicating to return the replacement document.
	 *
	 * @return
	 */
	public boolean isReturnNew() {
		return returnNew;
	}

	/**
	 * Get the bit indicating if to create a new document if not exists.
	 *
	 * @return
	 */
	public boolean isUpsert() {
		return upsert;
	}

}
