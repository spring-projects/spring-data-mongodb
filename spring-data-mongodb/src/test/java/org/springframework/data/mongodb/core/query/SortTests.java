/*
 * Copyright 2010-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

/**
 * Unit tests for sorting.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class SortTests {

	@Test
	public void testWithSortAscending() {

		Query s = new Query().with(Sort.by(Direction.ASC, "name"));
		assertEquals(Document.parse("{ \"name\" : 1}"), s.getSortObject());
	}

	@Test
	public void testWithSortDescending() {

		Query s = new Query().with(Sort.by(Direction.DESC, "name"));
		assertEquals(Document.parse("{ \"name\" : -1}"), s.getSortObject());
	}

	@Test // DATADOC-177
	public void preservesOrderKeysOnMultipleSorts() {

		Query sort = new Query().with(Sort.by(Direction.DESC, "foo").and(Sort.by(Direction.DESC, "bar")));
		assertThat(sort.getSortObject(), is(Document.parse("{ \"foo\" : -1 , \"bar\" : -1}")));
	}
}
