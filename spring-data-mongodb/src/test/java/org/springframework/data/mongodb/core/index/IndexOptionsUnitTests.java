/*
 * Copyright 2024-present the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import java.time.Duration;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.index.IndexOptions.Unique;

/**
 * @author Christoph Strobl
 */
class IndexOptionsUnitTests {

	@Test // GH-4851
	void noneIsEmpty() {

		IndexOptions options = IndexOptions.none();

		assertThat(options.getExpire()).isNull();
		assertThat(options.getUnique()).isNull();
		assertThat(options.isHidden()).isNull();
		assertThat(options.toDocument()).isEqualTo(new Document());
	}

	@Test // GH-4851
	void uniqueSetsFlag() {

		IndexOptions options = IndexOptions.unique();

		assertThat(options.getUnique()).isEqualTo(Unique.YES);
		assertThat(options.toDocument()).containsEntry("unique", true);

		options.setUnique(Unique.NO);
		assertThat(options.toDocument()).containsEntry("unique", false);

		options.setUnique(Unique.PREPARE);
		assertThat(options.toDocument()).containsEntry("prepareUnique", true);
	}

	@Test // GH-4851
	void hiddenSetsFlag() {

		IndexOptions options = IndexOptions.hidden();

		assertThat(options.isHidden()).isTrue();
		assertThat(options.toDocument()).containsEntry("hidden", true);
	}

	@Test // GH-4851
	void expireAfterSetsExpiration() {

		Duration duration = Duration.ofMinutes(2);
		IndexOptions options = IndexOptions.expireAfter(duration);

		assertThat(options.getExpire()).isEqualTo(duration);
		assertThat(options.toDocument()).containsEntry("expireAfterSeconds", duration.toSeconds());
	}

	@Test // GH-4851
	void expireAfterForZeroAndNegativeDuration() {

		assertThat(IndexOptions.expireAfter(Duration.ZERO).toDocument()).containsEntry("expireAfterSeconds", 0L);
		assertThat(IndexOptions.expireAfter(Duration.ofSeconds(-1)).toDocument()).isEmpty();
	}
}
