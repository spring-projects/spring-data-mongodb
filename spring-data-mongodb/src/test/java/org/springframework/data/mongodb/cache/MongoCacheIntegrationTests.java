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

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * Integration tests for {@link MongoCache} using a real MongoDB server.
 *
 * @author Anıl Şenocak
 */
class MongoCacheIntegrationTests {

	private static final String COLLECTION_NAME = "mongo_cache_integration_tests";

	@Template(initialEntitySet = CachedValue.class) static MongoTestTemplate mongoTemplate;

	private MongoCacheManager cacheManager;

	@BeforeEach
	void setUp() {

		mongoTemplate.dropCollection(COLLECTION_NAME);
		cacheManager = new MongoCacheManager(mongoTemplate,
				MongoCacheConfiguration.defaultCacheConfig().withCollectionName(COLLECTION_NAME));
	}

	@AfterEach
	void tearDown() {
		mongoTemplate.dropCollection(COLLECTION_NAME);
	}

	@Test
	void shouldPersistAndRehydrateValuesThroughMongoDB() {

		Cache cache = cacheManager.getCache("users");
		CachedValue expected = new CachedValue("Ada Lovelace");

		cache.put("ada", expected);

		Document entry = mongoTemplate.findById(MongoCacheKey.id("users", "ada"), Document.class, COLLECTION_NAME);
		assertThat(entry).isNotNull().containsEntry("cacheName", "users").containsEntry("cacheKey", "java.lang.String:ada")
				.containsEntry("nullValue", false);
		assertThat(cache.get("ada", CachedValue.class)).isEqualTo(expected);
	}

	@Test
	void shouldRetainCachedNullValuesAcrossMongoDBReads() {

		Cache cache = cacheManager.getCache("users");

		cache.put("missing", null);

		Cache.ValueWrapper cached = cache.get("missing");
		Document entry = mongoTemplate.findById(MongoCacheKey.id("users", "missing"), Document.class, COLLECTION_NAME);
		assertThat(cached).isNotNull();
		assertThat(cached.get()).isNull();
		assertThat(entry).isNotNull().containsEntry("nullValue", true);
	}

	@Test
	void shouldRemoveExpiredEntriesOnRead() {

		Cache cache = cacheManager.getCache("users");
		String id = MongoCacheKey.id("users", "expired");
		cache.put("expired", "value");
		mongoTemplate.updateFirst(query(where("_id").is(id)), Update.update("expiresAt", Instant.now().minusSeconds(1)),
				COLLECTION_NAME);

		assertThat(cache.get("expired")).isNull();
		assertThat(mongoTemplate.count(query(where("_id").is(id)), COLLECTION_NAME)).isZero();
	}

	@Test
	void shouldPersistConfiguredEntryExpiry() {

		Duration ttl = Duration.ofMinutes(5);
		MongoCacheManager ttlCacheManager = new MongoCacheManager(mongoTemplate,
				MongoCacheConfiguration.defaultCacheConfig().withCollectionName(COLLECTION_NAME).entryTtl(ttl));
		Cache cache = ttlCacheManager.getCache("users");
		Instant beforePut = Instant.now();

		cache.put("ada", new CachedValue("Ada Lovelace"));

		Instant afterPut = Instant.now();
		MongoCacheEntry entry = mongoTemplate.findById(MongoCacheKey.id("users", "ada"), MongoCacheEntry.class,
				COLLECTION_NAME);
		assertThat(entry).isNotNull();
		assertThat(entry.getExpiresAt()).isBetween(beforePut.plus(ttl).minusMillis(1), afterPut.plus(ttl).plusMillis(1));
	}

	@Test
	void shouldKeepExistingValueWhenPuttingIfAbsentAndEvictIt() {

		Cache cache = cacheManager.getCache("users");
		CachedValue initial = new CachedValue("Ada Lovelace");

		assertThat(cache.putIfAbsent("ada", initial)).isNull();
		Cache.ValueWrapper existing = cache.putIfAbsent("ada", new CachedValue("Grace Hopper"));

		assertThat(existing).isNotNull();
		assertThat(existing.get()).isEqualTo(initial);
		assertThat(cache.evictIfPresent("ada")).isTrue();
		assertThat(cache.get("ada")).isNull();
		assertThat(cache.evictIfPresent("ada")).isFalse();
	}

	@Test
	void shouldInvalidateOneCacheWithoutAffectingOthersAndClearAllEntries() {

		Cache users = cacheManager.getCache("users");
		Cache products = cacheManager.getCache("products");
		users.put("ada", new CachedValue("Ada Lovelace"));
		products.put("book", new CachedValue("Analytical Engine"));

		assertThat(users.invalidate()).isTrue();
		assertThat(mongoTemplate.count(query(where("cacheName").is("users")), COLLECTION_NAME)).isZero();
		assertThat(mongoTemplate.count(query(where("cacheName").is("products")), COLLECTION_NAME)).isEqualTo(1);
		assertThat(cacheManager.clearAll()).isTrue();
		assertThat(mongoTemplate.count(new org.springframework.data.mongodb.core.query.Query(), COLLECTION_NAME)).isZero();
	}

	static class CachedValue {

		private String name;

		CachedValue() {}

		CachedValue(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CachedValue that = (CachedValue) o;
			return Objects.equals(this.name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.name);
		}
	}
}
