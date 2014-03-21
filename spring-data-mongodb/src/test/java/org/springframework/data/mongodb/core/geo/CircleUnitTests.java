/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.mongodb.core.geo;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for {@link Circle}.
 * 
 * @author Oliver Gierke
 */
@SuppressWarnings("deprecation")
public class CircleUnitTests {

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullOrigin() {
		new Circle(null, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNegativeRadius() {
		new Circle(1, 1, -1);
	}

	@Test
	public void considersTwoCirclesEqualCorrectly() {

		Circle left = new Circle(1, 1, 1);
		Circle right = new Circle(1, 1, 1);

		assertThat(left, is(right));
		assertThat(right, is(left));

		right = new Circle(new Point(1, 1), 1);

		assertThat(left, is(right));
		assertThat(right, is(left));
	}
}
