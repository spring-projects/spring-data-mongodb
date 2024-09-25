/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.util;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.test.util.ClassPathExclusions;
import org.springframework.util.ClassUtils;

import com.mongodb.internal.build.MongoDriverVersion;

/**
 * Tests for {@link MongoClientVersion}.
 *
 * @author Christoph Strobl
 */
class MongoClientVersionUnitTests {

	@Test // GH-4578
	void parsesClientVersionCorrectly() {
		assertThat(MongoClientVersion.isVersion5orNewer()).isEqualTo(MongoDriverVersion.VERSION.startsWith("5"));
	}

	@Test // GH-4578
	@ClassPathExclusions(packages = { "com.mongodb.internal.build" })
	void fallsBackToClassLookupIfDriverVersionNotPresent() {
		assertThat(MongoClientVersion.isVersion5orNewer()).isEqualTo(
				ClassUtils.isPresent("com.mongodb.internal.connection.StreamFactoryFactory", this.getClass().getClassLoader()));
	}
}
