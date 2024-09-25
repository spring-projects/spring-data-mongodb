/*
 * Copyright 2010-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.mapreduce;

import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class MapReduceOptionsTests {

	@Test
	public void testFinalize() {
		new MapReduceOptions().finalizeFunction("code");
	}

	@Test // DATAMONGO-1334
	public void limitShouldBeIncludedCorrectly() {

		MapReduceOptions options = new MapReduceOptions();
		options.limit(10);

		assertThat(options.getOptionsObject()).containsEntry("limit", 10);
	}

	@Test // DATAMONGO-1334
	public void limitShouldNotBePresentInDocumentWhenNotSet() {
		assertThat(new MapReduceOptions().getOptionsObject()).doesNotContainKey("limit");
	}
}
