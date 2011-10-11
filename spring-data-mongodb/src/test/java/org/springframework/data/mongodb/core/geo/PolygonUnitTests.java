/*
 * Copyright 2010-2011 the original author or authors.
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
 * Unit tests for {@link Polygon}.
 * 
 * @author Oliver Gierke
 */
public class PolygonUnitTests {

	Point first = new Point(1, 1);
	Point second = new Point(2, 2);
	Point third = new Point(3, 3);

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullPoints() {
		new Polygon(null, null, null);
	}
	
	@Test
	public void createsSimplePolygon() {
		Polygon polygon = new Polygon(third, second, first);
		assertThat(polygon, is(notNullValue()));
	}
	
	@Test
	public void isEqualForSamePoints() {
		
		Polygon left = new Polygon(third, second, first);
		Polygon right = new Polygon(third, second, first);
		
		assertThat(left, is(right));
		assertThat(right, is(left));
	}
}
