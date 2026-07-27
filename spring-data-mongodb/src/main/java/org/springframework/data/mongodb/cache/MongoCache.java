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

import com.mongodb.client.result.DeleteResult;
import org.jspecify.annotations.Nullable;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link Cache} backed by a MongoDB collection.
 *
 * @author Anıl Şenocak
 * @since 5.2
 */
public class MongoCache implements Cache {

	private final String name;
	private final MongoOperations mongoOperations;
	private final MongoCacheConfiguration configuration;
	private final Clock clock;
	private final ConcurrentMap<String, Object> valueLoaderLocks = new ConcurrentHashMap<>();

	/**
	 * Create a cache using {@link MongoCacheConfiguration#defaultCacheConfig()}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public MongoCache(String name, MongoOperations mongoOperations) {
		this(name, mongoOperations, MongoCacheConfiguration.defaultCacheConfig());
	}

	/**
	 * Create a cache with the given configuration.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param mongoOperations must not be {@literal null}.
	 * @param configuration must not be {@literal null}.
	 */
	public MongoCache(String name, MongoOperations mongoOperations, MongoCacheConfiguration configuration) {
		this(name, mongoOperations, configuration, Clock.systemUTC());
	}

	MongoCache(String name, MongoOperations mongoOperations, MongoCacheConfiguration configuration, Clock clock) {

		Assert.hasText(name, "Cache name must not be empty");
		Assert.notNull(mongoOperations, "MongoOperations must not be null");
		Assert.notNull(configuration, "MongoCacheConfiguration must not be null");
		Assert.notNull(clock, "Clock must not be null");

		this.name = name;
		this.mongoOperations = mongoOperations;
		this.configuration = configuration;
		this.clock = clock;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Object getNativeCache() {
		return this.mongoOperations;
	}

	@Override
	public @Nullable ValueWrapper get(Object key) {

		MongoCacheEntry entry = lookup(key);
		return entry != null ? new SimpleValueWrapper(fromStoreValue(entry)) : null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> @Nullable T get(Object key, @Nullable Class<T> type) {

		MongoCacheEntry entry = lookup(key);
		if (entry == null) {
			return null;
		}

		Object value = fromStoreValue(entry);
		if (value != null && type != null && !type.isInstance(value)) {
			throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + value);
		}

		return type != null ? type.cast(value) : (T) value;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> @Nullable T get(Object key, Callable<T> valueLoader) {

		Assert.notNull(valueLoader, "Value loader must not be null");

		ValueWrapper wrapper = get(key);
		if (wrapper != null) {
			return (T) wrapper.get();
		}

		String keyId = MongoCacheKey.id(this.name, key);
		Object lock = this.valueLoaderLocks.computeIfAbsent(keyId, ignored -> new Object());

		try {
			synchronized (lock) {

				wrapper = get(key);
				if (wrapper != null) {
					return (T) wrapper.get();
				}

				T value = valueLoader.call();
				put(key, value);
				return value;
			}
		} catch (Exception ex) {
			throw new ValueRetrievalException(key, valueLoader, ex);
		} finally {
			this.valueLoaderLocks.remove(keyId, lock);
		}
	}

	@Override
	public void put(Object key, @Nullable Object value) {

		Assert.notNull(key, "Cache key must not be null");
		if (value == null && !this.configuration.isAllowNullValues()) {
			throw new IllegalArgumentException("This MongoCache is configured to not allow null values");
		}

		Instant now = this.clock.instant();
		MongoCacheEntry entry = new MongoCacheEntry();
		entry.setId(MongoCacheKey.id(this.name, key));
		entry.setCacheName(this.name);
		entry.setCacheKey(MongoCacheKey.description(key));
		entry.setValue(value);
		entry.setNullValue(value == null);
		entry.setCreatedAt(now);
		entry.setUpdatedAt(now);
		entry.setExpiresAt(expiresAt(now));

		this.mongoOperations.save(entry, this.configuration.getCollectionName());
	}

	@Override
	public @Nullable ValueWrapper putIfAbsent(Object key, @Nullable Object value) {

		ValueWrapper existing = get(key);
		if (existing != null) {
			return existing;
		}

		put(key, value);
		return null;
	}

	@Override
	public void evict(Object key) {
		removeById(MongoCacheKey.id(this.name, key));
	}

	@Override
	public boolean evictIfPresent(Object key) {
		return removeById(MongoCacheKey.id(this.name, key)).getDeletedCount() > 0;
	}

	@Override
	public void clear() {
		invalidate();
	}

	@Override
	public boolean invalidate() {
		return this.mongoOperations.remove(Query.query(Criteria.where("cacheName").is(this.name)),
				this.configuration.getCollectionName()).getDeletedCount() > 0;
	}

	private @Nullable MongoCacheEntry lookup(Object key) {

		Assert.notNull(key, "Cache key must not be null");
		MongoCacheEntry entry = this.mongoOperations.findById(MongoCacheKey.id(this.name, key), MongoCacheEntry.class,
				this.configuration.getCollectionName());

		if (entry == null) {
			return null;
		}

		if (entry.isExpired(this.clock.instant())) {
			removeById(Objects.requireNonNull(entry.getId()));
			return null;
		}

		return entry;
	}

	private @Nullable Object fromStoreValue(MongoCacheEntry entry) {
		return entry.isNullValue() ? null : entry.getValue();
	}

	private @Nullable Instant expiresAt(Instant createdAt) {

		Duration timeToLive = this.configuration.getTimeToLive();
		return timeToLive == null || timeToLive.isZero() ? null : createdAt.plus(timeToLive);
	}

	private DeleteResult removeById(String id) {
		return this.mongoOperations.remove(Query.query(Criteria.where("_id").is(id)), this.configuration.getCollectionName());
	}
}
