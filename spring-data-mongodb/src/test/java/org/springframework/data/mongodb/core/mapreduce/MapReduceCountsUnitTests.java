/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for {@link MapReduceCounts}.
 * 
 * @author Oliver Gierke
 */
public class MapReduceCountsUnitTests {

	/**
	 * @see DATACMNS-378
	 */
	@Test
	public void equalsForSameNumberValues() {

		MapReduceCounts left = new MapReduceCounts(1L, 1L, 1L);
		MapReduceCounts right = new MapReduceCounts(1L, 1L, 1L);

		assertThat(left, is(right));
		assertThat(right, is(left));
		assertThat(left.hashCode(), is(right.hashCode()));
	}

	/**
	 * @see DATACMNS-378
	 */
	@Test
	public void notEqualForDifferentNumberValues() {

		MapReduceCounts left = new MapReduceCounts(1L, 1L, 1L);
		MapReduceCounts right = new MapReduceCounts(1L, 2L, 1L);

		assertThat(left, is(not(right)));
		assertThat(right, is(not(left)));
		assertThat(left.hashCode(), is(not(right.hashCode())));
	}
}
