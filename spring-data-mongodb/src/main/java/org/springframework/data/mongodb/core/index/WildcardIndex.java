/*
 * Copyright 2021-2023 the original author or authors.
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
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@link WildcardIndex} is a specific {@link Index} that can be used to include all fields into an index based on the
 * {@code $**" : 1} pattern on a root object (the one typically carrying the
 * {@link org.springframework.data.mongodb.core.mapping.Document} annotation). On those it is possible to use
 * {@link #wildcardProjectionInclude(String...)} and {@link #wildcardProjectionExclude(String...)} to define specific
 * paths for in-/exclusion.
 * <br />
 * It can also be used to define an index on a specific field path and its subfields, e.g.
 * {@code "path.to.field.$**" : 1}. <br />
 * Note that {@literal wildcardProjections} are not allowed in this case.
 * <br />
 * <strong>LIMITATIONS</strong><br />
 * <ul>
 * <li>{@link #unique() Unique} and {@link #expire(long) ttl} options are not supported.</li>
 * <li>Keys used for sharding must not be included</li>
 * <li>Cannot be used to generate any type of geo index.</li>
 * </ul>
 *
 * @author Christoph Strobl
 * @see <a href= "https://docs.mongodb.com/manual/core/index-wildcard/">MongoDB Reference Documentation: Wildcard
 *      Indexes/</a>
 * @since 3.3
 */
public class WildcardIndex extends Index {

	private @Nullable String fieldName;
	private final Map<String, Object> wildcardProjection = new LinkedHashMap<>();

	/**
	 * Create a new instance of {@link WildcardIndex} using {@code $**}.
	 */
	public WildcardIndex() {}

	/**
	 * Create a new instance of {@link WildcardIndex} for the given {@literal path}. If no {@literal path} is provided the
	 * index will be considered a root one using {@code $**}. <br />
	 * <strong>NOTE:</strong> {@link #wildcardProjectionInclude(String...)}, {@link #wildcardProjectionExclude(String...)}
	 * can only be used for top level index definitions having an {@literal empty} or {@literal null} path.
	 *
	 * @param path can be {@literal null}. If {@literal null} all fields will be indexed.
	 */
	public WildcardIndex(@Nullable String path) {
		this.fieldName = path;
	}

	/**
	 * Include the {@code _id} field in {@literal wildcardProjection}.
	 *
	 * @return this.
	 */
	public WildcardIndex includeId() {

		wildcardProjection.put(FieldName.ID.name(), 1);
		return this;
	}

	/**
	 * Set the index name to use.
	 *
	 * @param name
	 * @return this.
	 */
	@Override
	public WildcardIndex named(String name) {

		super.named(name);
		return this;
	}

	/**
	 * Unique option is not supported.
	 *
	 * @throws UnsupportedOperationException not supported for wildcard indexes.
	 */
	@Override
	public Index unique() {
		throw new UnsupportedOperationException("Wildcard Index does not support 'unique'");
	}

	/**
	 * ttl option is not supported.
	 *
	 * @throws UnsupportedOperationException not supported for wildcard indexes.
	 */
	@Override
	public Index expire(long seconds) {
		throw new UnsupportedOperationException("Wildcard Index does not support 'ttl'");
	}

	/**
	 * ttl option is not supported.
	 *
	 * @throws UnsupportedOperationException not supported for wildcard indexes.
	 */
	@Override
	public Index expire(long value, TimeUnit timeUnit) {
		throw new UnsupportedOperationException("Wildcard Index does not support 'ttl'");
	}

	/**
	 * ttl option is not supported.
	 *
	 * @throws UnsupportedOperationException not supported for wildcard indexes.
	 */
	@Override
	public Index expire(Duration duration) {
		throw new UnsupportedOperationException("Wildcard Index does not support 'ttl'");
	}

	/**
	 * Add fields to be included from indexing via {@code wildcardProjection}. <br />
	 * This option is only allowed on {@link WildcardIndex#WildcardIndex() top level} wildcard indexes.
	 *
	 * @param paths must not be {@literal null}.
	 * @return this.
	 */
	public WildcardIndex wildcardProjectionInclude(String... paths) {

		for (String path : paths) {
			wildcardProjection.put(path, 1);
		}
		return this;
	}

	/**
	 * Add fields to be excluded from indexing via {@code wildcardProjection}. <br />
	 * This option is only allowed on {@link WildcardIndex#WildcardIndex() top level} wildcard indexes.
	 *
	 * @param paths must not be {@literal null}.
	 * @return this.
	 */
	public WildcardIndex wildcardProjectionExclude(String... paths) {

		for (String path : paths) {
			wildcardProjection.put(path, 0);
		}
		return this;
	}

	/**
	 * Set the fields to be in-/excluded from indexing via {@code wildcardProjection}. <br />
	 * This option is only allowed on {@link WildcardIndex#WildcardIndex() top level} wildcard indexes.
	 *
	 * @param includeExclude must not be {@literal null}.
	 * @return this.
	 */
	public WildcardIndex wildcardProjection(Map<String, Object> includeExclude) {

		wildcardProjection.putAll(includeExclude);
		return this;
	}

	private String getTargetFieldName() {
		return StringUtils.hasText(fieldName) ? (fieldName + ".$**") : "$**";
	}

	@Override
	public Document getIndexKeys() {
		return new Document(getTargetFieldName(), 1);
	}

	@Override
	public Document getIndexOptions() {

		Document options = new Document(super.getIndexOptions());
		if (!CollectionUtils.isEmpty(wildcardProjection)) {
			options.put("wildcardProjection", new Document(wildcardProjection));
		}
		return options;
	}
}
