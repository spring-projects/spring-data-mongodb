/*
 * Copyright 2011-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *			http://www.apache.org/licenses/LICENSE-2.0
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
 * Unit tests for {@link GeoResult}.
 * 
 * @author Oliver Gierke
 */
@SuppressWarnings("deprecation")
public class GeoResultUnitTests {

	GeoResult<String> first = new GeoResult<String>("Foo", new Distance(2.5));
	GeoResult<String> second = new GeoResult<String>("Foo", new Distance(2.5));
	GeoResult<String> third = new GeoResult<String>("Bar", new Distance(2.5));
	GeoResult<String> fourth = new GeoResult<String>("Foo", new Distance(5.2));

	@Test
	public void considersSameInstanceEqual() {

		assertThat(first.equals(first), is(true));
	}

	@Test
	public void considersSameValuesAsEqual() {
		assertThat(first.equals(second), is(true));
		assertThat(second.equals(first), is(true));
		assertThat(first.equals(third), is(false));
		assertThat(third.equals(first), is(false));
		assertThat(first.equals(fourth), is(false));
		assertThat(fourth.equals(first), is(false));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullContent() {
		new GeoResult(null, new Distance(2.5));
	}
}
