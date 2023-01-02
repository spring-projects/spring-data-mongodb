/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Value object representing an entities <a href="https://docs.mongodb.com/manual/core/sharding-shard-key/">Shard
 * Key</a> used to distribute documents across a sharded MongoDB cluster.
 * <br />
 * {@link ShardKey#isImmutable() Immutable} shard keys indicates a fixed value that is not updated (see
 * <a href="https://docs.mongodb.com/manual/core/sharding-shard-key/#change-a-document-s-shard-key-value">MongoDB
 * Reference: Change a Document's Shard Key Value</a>), which allows to skip server round trips in cases where a
 * potential shard key change might have occurred.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.0
 */
public class ShardKey {

	private static final ShardKey NONE = new ShardKey(Collections.emptyList(), null, true);

	private final List<String> propertyNames;
	private final @Nullable ShardingStrategy shardingStrategy;
	private final boolean immutable;

	private ShardKey(List<String> propertyNames, @Nullable ShardingStrategy shardingStrategy, boolean immutable) {

		this.propertyNames = propertyNames;
		this.shardingStrategy = shardingStrategy;
		this.immutable = immutable;
	}

	/**
	 * @return the number of properties used to form the shard key.
	 */
	public int size() {
		return propertyNames.size();
	}

	/**
	 * @return the unmodifiable collection of property names forming the shard key.
	 */
	public Collection<String> getPropertyNames() {
		return propertyNames;
	}

	/**
	 * @return {@literal true} if the shard key of an document does not change.
	 * @see <a href="https://docs.mongodb.com/manual/core/sharding-shard-key/#change-a-document-s-shard-key-value">MongoDB
	 *      Reference: Change a Document's Shard Key Value</a>
	 */
	public boolean isImmutable() {
		return immutable;
	}

	/**
	 * Return whether the shard key represents a sharded key. Return {@literal false} if the key is not sharded.
	 *
	 * @return {@literal true} if the key is sharded; {@literal false} otherwise.
	 */
	public boolean isSharded() {
		return !propertyNames.isEmpty();
	}

	/**
	 * Get the raw MongoDB representation of the {@link ShardKey}.
	 *
	 * @return never {@literal null}.
	 */
	public Document getDocument() {

		Document doc = new Document();
		for (String field : propertyNames) {
			doc.append(field, shardingValue());
		}
		return doc;
	}

	private Object shardingValue() {
		return ObjectUtils.nullSafeEquals(ShardingStrategy.HASH, shardingStrategy) ? "hash" : 1;
	}

	/**
	 * {@link ShardKey} indicating no shard key has been defined.
	 *
	 * @return {@link #NONE}
	 */
	public static ShardKey none() {
		return NONE;
	}

	/**
	 * Create a new {@link ShardingStrategy#RANGE} shard key.
	 *
	 * @param propertyNames must not be {@literal null}.
	 * @return new instance of {@link ShardKey}.
	 */
	public static ShardKey range(String... propertyNames) {
		return new ShardKey(Arrays.asList(propertyNames), ShardingStrategy.RANGE, false);
	}

	/**
	 * Create a new {@link ShardingStrategy#RANGE} shard key.
	 *
	 * @param propertyNames must not be {@literal null}.
	 * @return new instance of {@link ShardKey}.
	 */
	public static ShardKey hash(String... propertyNames) {
		return new ShardKey(Arrays.asList(propertyNames), ShardingStrategy.HASH, false);
	}

	/**
	 * Turn the given {@link ShardKey} into an {@link #isImmutable() immutable} one.
	 *
	 * @param shardKey must not be {@literal null}.
	 * @return new instance of {@link ShardKey} if the given shard key is not already immutable.
	 */
	public static ShardKey immutable(ShardKey shardKey) {

		if (shardKey.isImmutable()) {
			return shardKey;
		}

		return new ShardKey(shardKey.propertyNames, shardKey.shardingStrategy, true);
	}
}
