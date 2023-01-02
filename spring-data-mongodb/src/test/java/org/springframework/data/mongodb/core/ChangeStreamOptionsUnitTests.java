/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChangeStreamOptions}.
 *
 * @author Mark Paluch
 */
public class ChangeStreamOptionsUnitTests {

	@Test // DATAMONGO-2258
	public void shouldReportResumeAfter() {

		ChangeStreamOptions options = ChangeStreamOptions.builder().resumeAfter(new BsonDocument()).build();

		assertThat(options.isResumeAfter()).isTrue();
		assertThat(options.isStartAfter()).isFalse();
	}

	@Test // DATAMONGO-2258
	public void shouldReportStartAfter() {

		ChangeStreamOptions options = ChangeStreamOptions.builder().startAfter(new BsonDocument()).build();

		assertThat(options.isResumeAfter()).isFalse();
		assertThat(options.isStartAfter()).isTrue();
	}

	@Test // DATAMONGO-2258
	public void shouldNotReportResumeStartAfter() {

		ChangeStreamOptions options = ChangeStreamOptions.empty();

		assertThat(options.isResumeAfter()).isFalse();
		assertThat(options.isStartAfter()).isFalse();
	}
}
