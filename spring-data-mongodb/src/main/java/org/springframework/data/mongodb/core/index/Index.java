/*
 * Copyright 2010-2023 the original author or authors.
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.IndexOptions.Unique;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@SuppressWarnings("deprecation")
public class Index implements IndexDefinition {

	private final Map<String, Direction> fieldSpec = new LinkedHashMap<String, Direction>();
	private @Nullable String name;
	private boolean sparse = false;
	private boolean background = false;
	private final IndexOptions options = IndexOptions.none();
	private Optional<IndexFilter> filter = Optional.empty();
	private Optional<Collation> collation = Optional.empty();

	public Index() {}

	public Index(String key, Direction direction) {
		fieldSpec.put(key, direction);
	}

	public Index on(String key, Direction direction) {
		fieldSpec.put(key, direction);
		return this;
	}

	public Index named(String name) {
		this.name = name;
		return this;
	}

	/**
	 * Reject all documents that contain a duplicate value for the indexed field.
	 *
	 * @return this.
	 * @see <a href=
	 *      "https://docs.mongodb.org/manual/core/index-unique/">https://docs.mongodb.org/manual/core/index-unique/</a>
	 */
	public Index unique() {

		this.options.setUnique(Unique.YES);
		return this;
	}

	/**
	 * Skip over any document that is missing the indexed field.
	 *
	 * @return this.
	 * @see <a href=
	 *      "https://docs.mongodb.org/manual/core/index-sparse/">https://docs.mongodb.org/manual/core/index-sparse/</a>
	 */
	public Index sparse() {
		this.sparse = true;
		return this;
	}

	/**
	 * Build the index in background (non blocking).
	 *
	 * @return this.
	 * @since 1.5
	 */
	public Index background() {

		this.background = true;
		return this;
	}

	/**
	 * Hidden indexes are not visible to the query planner and cannot be used to support a query.
	 *
	 * @return this.
	 * @see <a href=
	 *      "https://www.mongodb.com/docs/manual/core/index-hidden/">https://www.mongodb.com/docs/manual/core/index-hidden/</a>
	 * @since 4.1
	 */
	public Index hidden() {

		options.setHidden(true);
		return this;
	}

	/**
	 * Specifies TTL in seconds.
	 *
	 * @param value
	 * @return this.
	 * @since 1.5
	 */
	public Index expire(long value) {
		return expire(value, TimeUnit.SECONDS);
	}

	/**
	 * Specifies the TTL.
	 *
	 * @param timeout must not be {@literal null}.
	 * @return this.
	 * @throws IllegalArgumentException if given {@literal timeout} is {@literal null}.
	 * @since 2.2
	 */
	public Index expire(Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		return expire(timeout.getSeconds());
	}

	/**
	 * Specifies TTL with given {@link TimeUnit}.
	 *
	 * @param value
	 * @param unit must not be {@literal null}.
	 * @return this.
	 * @since 1.5
	 */
	public Index expire(long value, TimeUnit unit) {

		Assert.notNull(unit, "TimeUnit for expiration must not be null");
		options.setExpire(Duration.ofSeconds(unit.toSeconds(value)));
		return this;
	}

	/**
	 * Only index the documents in a collection that meet a specified {@link IndexFilter filter expression}.
	 *
	 * @param filter can be {@literal null}.
	 * @return this.
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/core/index-partial/">https://docs.mongodb.com/manual/core/index-partial/</a>
	 * @since 1.10
	 */
	public Index partial(@Nullable IndexFilter filter) {

		this.filter = Optional.ofNullable(filter);
		return this;
	}

	/**
	 * Set the {@link Collation} to specify language-specific rules for string comparison, such as rules for lettercase
	 * and accent marks.<br />
	 * <strong>NOTE:</strong> Only queries using the same {@link Collation} as the {@link Index} actually make use of the
	 * index.
	 *
	 * @param collation can be {@literal null}.
	 * @return this.
	 * @since 2.0
	 */
	public Index collation(@Nullable Collation collation) {

		this.collation = Optional.ofNullable(collation);
		return this;
	}

	public Document getIndexKeys() {

		Document document = new Document();

		for (Entry<String, Direction> entry : fieldSpec.entrySet()) {
			document.put(entry.getKey(), Direction.ASC.equals(entry.getValue()) ? 1 : -1);
		}

		return document;
	}

	public Document getIndexOptions() {

		Document document = new Document();
		if (StringUtils.hasText(name)) {
			document.put("name", name);
		}
		if (sparse) {
			document.put("sparse", true);
		}
		if (background) {
			document.put("background", true);
		}
		document.putAll(options.toDocument());

		filter.ifPresent(val -> document.put("partialFilterExpression", val.getFilterObject()));
		collation.ifPresent(val -> document.append("collation", val.toDocument()));

		return document;
	}

	@Override
	public String toString() {
		return String.format("Index: %s - Options: %s", getIndexKeys(), getIndexOptions());
	}
}
