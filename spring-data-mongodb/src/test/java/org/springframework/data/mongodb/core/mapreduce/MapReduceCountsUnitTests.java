/*
 * Copyright 2012-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MapReduceCounts}.
 *
 * @author Oliver Gierke
 */
public class MapReduceCountsUnitTests {

	@Test // DATACMNS-378
	public void equalsForSameNumberValues() {

		MapReduceCounts left = new MapReduceCounts(1L, 1L, 1L);
		MapReduceCounts right = new MapReduceCounts(1L, 1L, 1L);

		assertThat(left).isEqualTo(right);
		assertThat(right).isEqualTo(left);
		assertThat(left.hashCode()).isEqualTo(right.hashCode());
	}

	@Test // DATACMNS-378
	public void notEqualForDifferentNumberValues() {

		MapReduceCounts left = new MapReduceCounts(1L, 1L, 1L);
		MapReduceCounts right = new MapReduceCounts(1L, 2L, 1L);

		assertThat(left).isNotEqualTo(right);
		assertThat(right).isNotEqualTo(left);
		assertThat(left.hashCode()).isNotEqualTo(right.hashCode());
	}
}
