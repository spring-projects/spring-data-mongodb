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

import java.util.Optional;

import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.lang.Nullable;

/**
 * Options for
 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public class FindAndReplaceOptions {

	private boolean returnNew;
	private boolean upsert;

	private @Nullable Collation collation;

	/**
	 * Static factory method to create a {@link FindAndReplaceOptions} instance.
	 *
	 * @return a new instance
	 */
	public static FindAndReplaceOptions options() {
		return new FindAndReplaceOptions();
	}

	/**
	 * @param options
	 * @return
	 */
	public static FindAndReplaceOptions of(@Nullable FindAndReplaceOptions source) {

		FindAndReplaceOptions options = new FindAndReplaceOptions();

		if (source == null) {
			return options;
		}

		options.returnNew = source.returnNew;
		options.upsert = source.upsert;
		options.collation = source.collation;

		return options;
	}

	public FindAndReplaceOptions returnNew(boolean returnNew) {
		this.returnNew = returnNew;
		return this;
	}

	public FindAndReplaceOptions upsert(boolean upsert) {
		this.upsert = upsert;
		return this;
	}

	/**
	 * Define the {@link Collation} specifying language-specific rules for string comparison.
	 *
	 * @param collation
	 * @return
	 */
	public FindAndReplaceOptions collation(@Nullable Collation collation) {

		this.collation = collation;
		return this;
	}

	public boolean isReturnNew() {
		return returnNew;
	}

	public boolean isUpsert() {
		return upsert;
	}

	/**
	 * Get the {@link Collation} specifying language-specific rules for string comparison.
	 *
	 * @return
	 */
	public Optional<Collation> getCollation() {
		return Optional.ofNullable(collation);
	}

}
