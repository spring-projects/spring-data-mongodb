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
package org.springframework.data.mongodb.core.query;

import static org.springframework.data.mongodb.core.query.Order.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.query.Sort;

public class SortTests {

	@Test
	public void testWithSortAscending() {
		Sort s = new Sort().on("name", ASCENDING);
		assertEquals("{ \"name\" : 1}", s.getSortObject().toString());
	}

	@Test
	public void testWithSortDescending() {
		Sort s = new Sort().on("name", DESCENDING);
		assertEquals("{ \"name\" : -1}", s.getSortObject().toString());
	}
	
	/**
	 * @see DATADOC-177
	 */
	@Test
	public void preservesOrderKeysOnMultipleSorts() {
		Sort sort = new Sort("foo", DESCENDING).on("bar", DESCENDING);
		assertThat(sort.getSortObject().toString(), is("{ \"foo\" : -1 , \"bar\" : -1}"));
	}
}
