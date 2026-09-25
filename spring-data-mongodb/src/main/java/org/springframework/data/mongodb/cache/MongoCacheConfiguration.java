/*
 * Copyright 2026 the original author or authors.
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
package org.springframework.data.mongodb.cache;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

import java.time.Duration;

/**
 * Immutable configuration for {@link MongoCache} instances.
 *
 * @author Anıl Şenocak
 * @since 5.2
 */
public class MongoCacheConfiguration {

	/**
	 * The default collection name used to store cache entries.
	 */
	public static final String DEFAULT_COLLECTION_NAME = "spring_cache";

	private final String collectionName;
	private final boolean allowNullValues;
	private final boolean createIndexes;
	private final @Nullable Duration timeToLive;

	private MongoCacheConfiguration(String collectionName, boolean allowNullValues, boolean createIndexes,
			@Nullable Duration timeToLive) {

		this.collectionName = collectionName;
		this.allowNullValues = allowNullValues;
		this.createIndexes = createIndexes;
		this.timeToLive = timeToLive;
	}

	/**
	 * Create a {@link MongoCacheConfiguration} using the default settings.
	 *
	 * @return a new {@link MongoCacheConfiguration}.
	 */
	public static MongoCacheConfiguration defaultCacheConfig() {
		return new MongoCacheConfiguration(DEFAULT_COLLECTION_NAME, true, true, null);
	}

	/**
	 * Return a new configuration that stores cache entries in {@code collectionName}.
	 *
	 * @param collectionName must not be {@literal null} or empty.
	 * @return a new {@link MongoCacheConfiguration}.
	 */
	public MongoCacheConfiguration withCollectionName(String collectionName) {

		Assert.hasText(collectionName, "Collection name must not be null or empty");
		return new MongoCacheConfiguration(collectionName, this.allowNullValues, this.createIndexes, this.timeToLive);
	}

	/**
	 * Return a new configuration that expires entries after {@code timeToLive}. A zero duration disables expiry.
	 *
	 * @param timeToLive must not be {@literal null} or negative.
	 * @return a new {@link MongoCacheConfiguration}.
	 */
	public MongoCacheConfiguration entryTtl(Duration timeToLive) {

		Assert.notNull(timeToLive, "Time to live must not be null");
		Assert.isTrue(!timeToLive.isNegative(), "Time to live must not be negative");
		return new MongoCacheConfiguration(this.collectionName, this.allowNullValues, this.createIndexes, timeToLive);
	}

	/**
	 * Return a new configuration that rejects {@literal null} cache values.
	 *
	 * @return a new {@link MongoCacheConfiguration}.
	 */
	public MongoCacheConfiguration disableCachingNullValues() {
		return new MongoCacheConfiguration(this.collectionName, false, this.createIndexes, this.timeToLive);
	}

	/**
	 * Return a new configuration that does not create indexes when the cache manager is initialized.
	 *
	 * @return a new {@link MongoCacheConfiguration}.
	 */
	public MongoCacheConfiguration disableIndexCreation() {
		return new MongoCacheConfiguration(this.collectionName, this.allowNullValues, false, this.timeToLive);
	}

	/**
	 * Return the name of the collection used for cache entries.
	 *
	 * @return never {@literal null}.
	 */
	public String getCollectionName() {
		return this.collectionName;
	}

	/**
	 * Return whether {@literal null} cache values may be stored.
	 *
	 * @return {@literal true} if {@literal null} values may be stored.
	 */
	public boolean isAllowNullValues() {
		return this.allowNullValues;
	}

	/**
	 * Return whether the cache manager creates the cache indexes automatically.
	 *
	 * @return {@literal true} if the cache manager creates indexes.
	 */
	public boolean isCreateIndexes() {
		return this.createIndexes;
	}

	/**
	 * Return the configured entry TTL. A {@literal null} value or {@link Duration#ZERO} denotes entries that do not
	 * expire.
	 *
	 * @return can be {@literal null}.
	 */
	public @Nullable Duration getTimeToLive() {
		return this.timeToLive;
	}
}
