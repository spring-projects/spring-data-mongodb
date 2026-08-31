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
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MongoCacheManager}.
 *
 * @author Anıl Şenocak
 */
@ExtendWith(MockitoExtension.class)
class MongoCacheManagerUnitTests {

	@Mock MongoOperations mongoOperations;
	@Mock IndexOperations indexOperations;

	@Test
	void shouldReturnTheSameCacheForTheSameName() {

		MongoCacheManager manager = new MongoCacheManager(this.mongoOperations,
				MongoCacheConfiguration.defaultCacheConfig().disableIndexCreation());

		Cache first = manager.getCache("users");
		Cache second = manager.getCache("users");

		assertThat(first).isSameAs(second);
		assertThat(manager.getCacheNames()).containsExactly("users");
	}

	@Test
	void shouldClearAllEntriesFromTheConfiguredCollection() {

		when(this.mongoOperations.remove(any(Query.class), eq("cache_entries"))).thenReturn(DeleteResult.acknowledged(2));
		MongoCacheManager manager = new MongoCacheManager(this.mongoOperations,
				MongoCacheConfiguration.defaultCacheConfig().withCollectionName("cache_entries").disableIndexCreation());

		assertThat(manager.clearAll()).isTrue();

		ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
		verify(this.mongoOperations).remove(query.capture(), eq("cache_entries"));
		assertThat(query.getValue().getQueryObject()).isEmpty();
	}

	@Test
	void shouldCreateIndexesForEvictionAndExpiry() {

		when(this.mongoOperations.indexOps("cache_entries")).thenReturn(this.indexOperations);
		when(this.indexOperations.ensureIndex(any(IndexDefinition.class))).thenReturn("index");

		new MongoCacheManager(this.mongoOperations,
				MongoCacheConfiguration.defaultCacheConfig().withCollectionName("cache_entries").entryTtl(Duration.ofSeconds(45)));

		ArgumentCaptor<IndexDefinition> indexes = ArgumentCaptor.forClass(IndexDefinition.class);
		verify(this.indexOperations, times(2)).ensureIndex(indexes.capture());
		IndexDefinition cacheName = indexes.getAllValues().stream()
				.filter(index -> index.getIndexKeys().containsKey("cacheName")).findFirst().orElseThrow();
		IndexDefinition expiry = indexes.getAllValues().stream()
				.filter(index -> index.getIndexKeys().containsKey("expiresAt")).findFirst().orElseThrow();

		assertThat(cacheName.getIndexKeys()).isEqualTo(new Document("cacheName", 1));
		assertThat(cacheName.getIndexOptions()).containsEntry("name", "mongo_cache_cache_name");
		assertThat(expiry.getIndexKeys()).isEqualTo(new Document("expiresAt", 1));
		assertThat(expiry.getIndexOptions()).containsEntry("name", "mongo_cache_expires_at")
				.containsEntry("expireAfterSeconds", Duration.ZERO.getSeconds());
	}

	@Test
	void shouldNotCreateIndexesWhenDisabled() {

		new MongoCacheManager(this.mongoOperations,
				MongoCacheConfiguration.defaultCacheConfig().disableIndexCreation());

		verify(this.mongoOperations, never()).indexOps(any(String.class));
	}
}
