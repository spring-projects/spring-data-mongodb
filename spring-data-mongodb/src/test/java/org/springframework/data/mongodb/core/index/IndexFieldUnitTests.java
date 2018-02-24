/*
 * Copyright 2012-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.domain.Sort.Direction;

/**
 * Unit tests for {@link IndexField}.
 *
 * @author Oliver Gierke
 */
@SuppressWarnings("deprecation")
public class IndexFieldUnitTests {

	@Test
	public void createsPlainIndexFieldCorrectly() {

		IndexField field = IndexField.create("foo", Direction.ASC);

		assertThat(field.getKey(), is("foo"));
		assertThat(field.getDirection(), is(Direction.ASC));
		assertThat(field.isGeo(), is(false));
	}

	@Test
	public void createsGeoIndexFieldCorrectly() {

		IndexField field = IndexField.geo("foo");

		assertThat(field.getKey(), is("foo"));
		assertThat(field.getDirection(), is(nullValue()));
		assertThat(field.isGeo(), is(true));
	}

	@Test
	public void correctEqualsForPlainFields() {

		IndexField first = IndexField.create("foo", Direction.ASC);
		IndexField second = IndexField.create("foo", Direction.ASC);

		assertThat(first, is(second));
		assertThat(second, is(first));
	}

	@Test
	public void correctEqualsForGeoFields() {

		IndexField first = IndexField.geo("bar");
		IndexField second = IndexField.geo("bar");

		assertThat(first, is(second));
		assertThat(second, is(first));
	}
}
