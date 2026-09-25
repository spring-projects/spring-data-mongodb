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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link MongoCacheConfiguration}.
 *
 * @author Anıl Şenocak
 */
class MongoCacheConfigurationUnitTests {

	@Test
	void shouldUseDefaults() {

		MongoCacheConfiguration configuration = MongoCacheConfiguration.defaultCacheConfig();

		assertThat(configuration.getCollectionName()).isEqualTo(MongoCacheConfiguration.DEFAULT_COLLECTION_NAME);
		assertThat(configuration.isAllowNullValues()).isTrue();
		assertThat(configuration.isCreateIndexes()).isTrue();
		assertThat(configuration.getTimeToLive()).isNull();
	}

	@Test
	void shouldCreateIndependentConfiguredInstances() {

		MongoCacheConfiguration defaults = MongoCacheConfiguration.defaultCacheConfig();
		MongoCacheConfiguration configuration = defaults.withCollectionName("cache_entries").entryTtl(Duration.ofMinutes(5))
				.disableCachingNullValues().disableIndexCreation();

		assertThat(defaults.getCollectionName()).isEqualTo(MongoCacheConfiguration.DEFAULT_COLLECTION_NAME);
		assertThat(defaults.getTimeToLive()).isNull();
		assertThat(defaults.isAllowNullValues()).isTrue();
		assertThat(defaults.isCreateIndexes()).isTrue();
		assertThat(configuration.getCollectionName()).isEqualTo("cache_entries");
		assertThat(configuration.getTimeToLive()).isEqualTo(Duration.ofMinutes(5));
		assertThat(configuration.isAllowNullValues()).isFalse();
		assertThat(configuration.isCreateIndexes()).isFalse();
	}

	@Test
	void shouldRejectInvalidSettings() {

		MongoCacheConfiguration configuration = MongoCacheConfiguration.defaultCacheConfig();

		assertThatIllegalArgumentException().isThrownBy(() -> configuration.withCollectionName(" "));
		assertThatIllegalArgumentException().isThrownBy(() -> configuration.entryTtl(Duration.ofSeconds(-1)));
	}
}
