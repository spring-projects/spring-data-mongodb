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
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.Cache;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MongoCache}.
 *
 * @author Anıl Şenocak
 */
@ExtendWith(MockitoExtension.class)
class MongoCacheUnitTests {

	@Mock MongoOperations mongoOperations;

	@Test
	void shouldStoreEntryWithComputedExpiry() {

		Instant now = Instant.parse("2026-07-21T12:00:00Z");
		MongoCache cache = cache(MongoCacheConfiguration.defaultCacheConfig().withCollectionName("cache_entries")
				.entryTtl(Duration.ofMinutes(5)), now);
		ArgumentCaptor<MongoCacheEntry> entry = ArgumentCaptor.forClass(MongoCacheEntry.class);

		cache.put("id-1", "Ada");

		verify(this.mongoOperations).save(entry.capture(), eq("cache_entries"));
		assertThat(entry.getValue().getId()).isEqualTo(MongoCacheKey.id("users", "id-1"));
		assertThat(entry.getValue().getCacheName()).isEqualTo("users");
		assertThat(entry.getValue().getCacheKey()).isEqualTo(MongoCacheKey.description("id-1"));
		assertThat(entry.getValue().getValue()).isEqualTo("Ada");
		assertThat(entry.getValue().isNullValue()).isFalse();
		assertThat(entry.getValue().getCreatedAt()).isEqualTo(now);
		assertThat(entry.getValue().getUpdatedAt()).isEqualTo(now);
		assertThat(entry.getValue().getExpiresAt()).isEqualTo(now.plus(Duration.ofMinutes(5)));
	}

	@Test
	void shouldRejectNullValuesWhenConfigured() {

		MongoCache cache = cache(MongoCacheConfiguration.defaultCacheConfig().disableCachingNullValues(), Instant.now());

		assertThatIllegalArgumentException().isThrownBy(() -> cache.put("id-1", null))
				.withMessage("This MongoCache is configured to not allow null values");
	}

	@Test
	void shouldReturnCachedNullValue() {

		MongoCache cache = cache(MongoCacheConfiguration.defaultCacheConfig(), Instant.now());
		MongoCacheEntry entry = entry("users::entry", null, true, null);
		when(this.mongoOperations.findById(MongoCacheKey.id("users", "id-1"), MongoCacheEntry.class,
				MongoCacheConfiguration.DEFAULT_COLLECTION_NAME)).thenReturn(entry);

		Cache.ValueWrapper value = cache.get("id-1");

		assertThat(value).isNotNull();
		assertThat(value.get()).isNull();
	}

	@Test
	void shouldRejectMismatchedRequiredType() {

		MongoCache cache = cache(MongoCacheConfiguration.defaultCacheConfig(), Instant.now());
		when(this.mongoOperations.findById(MongoCacheKey.id("users", "id-1"), MongoCacheEntry.class,
				MongoCacheConfiguration.DEFAULT_COLLECTION_NAME)).thenReturn(entry("users::entry", 42, false, null));

		assertThatIllegalStateException().isThrownBy(() -> cache.get("id-1", String.class))
				.withMessageContaining("Cached value is not of required type");
	}

	@Test
	void shouldTreatExpiredEntriesAsCacheMissesAndRemoveThem() {

		Instant now = Instant.parse("2026-07-21T12:00:00Z");
		MongoCache cache = cache(MongoCacheConfiguration.defaultCacheConfig().withCollectionName("cache_entries"), now);
		MongoCacheEntry entry = entry("users::expired", "value", false, now.minusSeconds(1));
		when(this.mongoOperations.findById(MongoCacheKey.id("users", "id-1"), MongoCacheEntry.class, "cache_entries"))
				.thenReturn(entry);
		when(this.mongoOperations.remove(any(Query.class), eq("cache_entries"))).thenReturn(DeleteResult.acknowledged(1));

		assertThat(cache.get("id-1")).isNull();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(this.mongoOperations).remove(query.capture(), eq("cache_entries"));
		assertThat(query.getValue().getQueryObject()).containsEntry("_id", "users::expired");
	}

	@Test
	void shouldLoadAValueOnlyOnceForTheSameKey() {

		Instant now = Instant.parse("2026-07-21T12:00:00Z");
		MongoCache cache = cache(MongoCacheConfiguration.defaultCacheConfig(), now);
		when(this.mongoOperations.findById(MongoCacheKey.id("users", "id-1"), MongoCacheEntry.class,
				MongoCacheConfiguration.DEFAULT_COLLECTION_NAME)).thenReturn(null, null,
						entry("users::stored", "Ada", false, now.plusSeconds(120)));
		AtomicInteger invocations = new AtomicInteger();

		String first = cache.get("id-1", () -> {
			invocations.incrementAndGet();
			return "Ada";
		});
		String second = cache.get("id-1", () -> {
			invocations.incrementAndGet();
			return "Grace";
		});

		assertThat(first).isEqualTo("Ada");
		assertThat(second).isEqualTo("Ada");
		assertThat(invocations).hasValue(1);
		verify(this.mongoOperations).save(any(MongoCacheEntry.class), eq(MongoCacheConfiguration.DEFAULT_COLLECTION_NAME));
	}

	@Test
	void shouldInvalidateOnlyEntriesFromItsCache() {

		MongoCache cache = cache(MongoCacheConfiguration.defaultCacheConfig().withCollectionName("cache_entries"), Instant.now());
		when(this.mongoOperations.remove(any(Query.class), eq("cache_entries"))).thenReturn(DeleteResult.acknowledged(3));

		assertThat(cache.invalidate()).isTrue();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(this.mongoOperations).remove(query.capture(), eq("cache_entries"));
		Document queryObject = query.getValue().getQueryObject();
		assertThat(queryObject).containsEntry("cacheName", "users");
	}

	private MongoCache cache(MongoCacheConfiguration configuration, Instant now) {
		return new MongoCache("users", this.mongoOperations, configuration, Clock.fixed(now, ZoneOffset.UTC));
	}

	private MongoCacheEntry entry(String id, Object value, boolean nullValue, Instant expiresAt) {

		MongoCacheEntry entry = new MongoCacheEntry();
		entry.setId(id);
		entry.setCacheName("users");
		entry.setValue(value);
		entry.setNullValue(nullValue);
		entry.setExpiresAt(expiresAt);
		return entry;
	}
}
