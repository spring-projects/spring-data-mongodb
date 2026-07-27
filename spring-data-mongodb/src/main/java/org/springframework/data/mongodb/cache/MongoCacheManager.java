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

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link CacheManager} that creates {@link MongoCache MongoCaches} backed by a shared MongoDB collection.
 *
 * @author Anıl Şenocak
 * @since 5.2
 */
public class MongoCacheManager implements CacheManager {

	private static final String CACHE_NAME_INDEX = "mongo_cache_cache_name";
	private static final String EXPIRY_INDEX = "mongo_cache_expires_at";

	private final MongoOperations mongoOperations;
	private final MongoCacheConfiguration configuration;
	private final Clock clock;
	private final ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<>();

	/**
	 * Create a cache manager using {@link MongoCacheConfiguration#defaultCacheConfig()}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 */
	public MongoCacheManager(MongoOperations mongoOperations) {
		this(mongoOperations, MongoCacheConfiguration.defaultCacheConfig());
	}

	/**
	 * Create a cache manager with the given configuration.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param configuration must not be {@literal null}.
	 */
	public MongoCacheManager(MongoOperations mongoOperations, MongoCacheConfiguration configuration) {
		this(mongoOperations, configuration, Clock.systemUTC());
	}

	MongoCacheManager(MongoOperations mongoOperations, MongoCacheConfiguration configuration, Clock clock) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null");
		Assert.notNull(configuration, "MongoCacheConfiguration must not be null");
		Assert.notNull(clock, "Clock must not be null");

		this.mongoOperations = mongoOperations;
		this.configuration = configuration;
		this.clock = clock;

		if (configuration.isCreateIndexes()) {
			createIndexes();
		}
	}

	@Override
	public Cache getCache(String name) {

		Assert.hasText(name, "Cache name must not be empty");
		return this.caches.computeIfAbsent(name,
				cacheName -> new MongoCache(cacheName, this.mongoOperations, this.configuration, this.clock));
	}

	@Override
	public Collection<String> getCacheNames() {
		return Collections.unmodifiableSet(new LinkedHashSet<>(this.caches.keySet()));
	}

	/**
	 * Remove all entries managed by this cache manager from its configured collection.
	 *
	 * @return {@literal true} if at least one cache entry was removed.
	 */
	public boolean clearAll() {
		return this.mongoOperations.remove(new Query(), this.configuration.getCollectionName()).getDeletedCount() > 0;
	}

	private void createIndexes() {

		IndexOperations indexOperations = this.mongoOperations.indexOps(this.configuration.getCollectionName());
		indexOperations.ensureIndex(new Index().on("cacheName", Direction.ASC).named(CACHE_NAME_INDEX));
		indexOperations.ensureIndex(new Index().on("expiresAt", Direction.ASC).expire(Duration.ZERO).named(EXPIRY_INDEX));
	}
}
