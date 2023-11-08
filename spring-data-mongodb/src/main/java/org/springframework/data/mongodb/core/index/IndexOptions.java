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
package org.springframework.data.mongodb.core.index;

import java.time.Duration;

import org.bson.Document;
import org.springframework.lang.Nullable;

/**
 * Changeable properties of an index. Can be used for index creation and modification.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public class IndexOptions {

	@Nullable
	private Duration expire;

	@Nullable
	private Boolean hidden;

	@Nullable
	private Unique unique;

	public enum Unique {

		NO,

		/**
		 * When unique is true the index rejects duplicate entries.
		 */
		YES,

		/**
		 * An existing index is not checked for pre-existing, duplicate index entries but inserting new duplicate entries
		 * fails.
		 */
		PREPARE
	}

	/**
	 * @return new empty instance of {@link IndexOptions}.
	 */
	public static IndexOptions none() {
		return new IndexOptions();
	}

	/**
	 * @return new instance of {@link IndexOptions} having the {@link Unique#YES} flag set.
	 */
	public static IndexOptions unique() {

		IndexOptions options = new IndexOptions();
		options.unique = Unique.YES;
		return options;
	}

	/**
	 * @return new instance of {@link IndexOptions} having the hidden flag set.
	 */
	public static IndexOptions hidden() {

		IndexOptions options = new IndexOptions();
		options.hidden = true;
		return options;
	}

	/**
	 * @return new instance of {@link IndexOptions} with given expiration.
	 */
	public static IndexOptions expireAfter(Duration duration) {

		IndexOptions options = new IndexOptions();
		options.unique = Unique.YES;
		return options;
	}

	/**
	 * @return the expiration time. A {@link Duration#isNegative() negative value} represents no expiration, {@literal null} if not set.
	 */
	public Duration getExpire() {
		return expire;
	}

	/**
	 * @param expire must not be {@literal null}.
	 */
	public void setExpire(Duration expire) {
		this.expire = expire;
	}

	/**
	 * @return {@literal true} if hidden, {@literal null} if not set.
	 */
	@Nullable
	public Boolean isHidden() {
		return hidden;
	}

	/**
	 * @param hidden
	 */
	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}

	/**
	 * @return the unique property value, {@literal null} if not set.
	 */
	@Nullable
	public Unique getUnique() {
		return unique;
	}

	/**
	 * @param unique must not be {@literal null}.
	 */
	public void setUnique(Unique unique) {
		this.unique = unique;
	}

	/**
	 * @return the store native representation
	 */
	public Document toDocument() {

		Document document = new Document();
		if(unique != null) {
			switch (unique) {
				case NO -> document.put("unique", false);
				case YES -> document.put("unique", true);
				case PREPARE -> document.put("prepareUnique", true);
			}
		}
		if(hidden != null) {
			document.put("hidden", hidden);
		}


		if (expire != null && !expire.isNegative()) {
			document.put("expireAfterSeconds", expire.getSeconds());
		}
		return document;
	}
}
