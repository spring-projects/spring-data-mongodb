/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.geo;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.geo.Box;
import org.springframework.data.mongodb.core.geo.Point;

/**
 * Unit tests for {@link Box}.
 * 
 * @author Oliver Gierke
 */
public class BoxUnitTests {

	Box first = new Box(new Point(1d, 1d), new Point(2d, 2d));
	Box second = new Box(new Point(1d, 1d), new Point(2d, 2d));
	Box third = new Box(new Point(3d, 3d), new Point(1d, 1d));

	@Test
	public void equalsWorksCorrectly() {

		assertThat(first.equals(second), is(true));
		assertThat(second.equals(first), is(true));
		assertThat(first.equals(third), is(false));
	}

	@Test
	public void hashCodeWorksCorrectly() {

		assertThat(first.hashCode(), is(second.hashCode()));
		assertThat(first.hashCode(), is(not(third.hashCode())));
	}
}
