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
import org.springframework.data.annotation.Id;

import java.time.Instant;

/**
 * Persistent representation of a cache entry.
 *
 * @author Anıl Şenocak
 */
class MongoCacheEntry {

	@Id private @Nullable String id;
	private @Nullable String cacheName;
	private @Nullable String cacheKey;
	private @Nullable Object value;
	private boolean nullValue;
	private @Nullable Instant createdAt;
	private @Nullable Instant updatedAt;
	private @Nullable Instant expiresAt;

	@Nullable String getId() {
		return this.id;
	}

	void setId(String id) {
		this.id = id;
	}

	@Nullable String getCacheName() {
		return this.cacheName;
	}

	void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}

	@Nullable String getCacheKey() {
		return this.cacheKey;
	}

	void setCacheKey(String cacheKey) {
		this.cacheKey = cacheKey;
	}

	@Nullable Object getValue() {
		return this.value;
	}

	void setValue(@Nullable Object value) {
		this.value = value;
	}

	boolean isNullValue() {
		return this.nullValue;
	}

	void setNullValue(boolean nullValue) {
		this.nullValue = nullValue;
	}

	@Nullable Instant getCreatedAt() {
		return this.createdAt;
	}

	void setCreatedAt(Instant createdAt) {
		this.createdAt = createdAt;
	}

	@Nullable Instant getUpdatedAt() {
		return this.updatedAt;
	}

	void setUpdatedAt(Instant updatedAt) {
		this.updatedAt = updatedAt;
	}

	@Nullable Instant getExpiresAt() {
		return this.expiresAt;
	}

	void setExpiresAt(@Nullable Instant expiresAt) {
		this.expiresAt = expiresAt;
	}

	boolean isExpired(Instant now) {
		return this.expiresAt != null && !this.expiresAt.isAfter(now);
	}
}
