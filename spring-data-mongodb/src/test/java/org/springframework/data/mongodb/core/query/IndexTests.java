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

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.Index.Duplicates;
import org.springframework.data.mongodb.core.query.Order;

public class IndexTests {

	@Test
	public void testWithAscendingIndex() {
		Index i = new Index().on("name", Order.ASCENDING);
		assertEquals("{ \"name\" : 1}", i.getIndexKeys().toString());
	}

	@Test
	public void testWithDescendingIndex() {
		Index i = new Index().on("name", Order.DESCENDING);
		assertEquals("{ \"name\" : -1}", i.getIndexKeys().toString());
	}

	@Test
	public void testNamedMultiFieldUniqueIndex() {
		Index i = new Index().on("name", Order.ASCENDING).on("age", Order.DESCENDING);
		i.named("test").unique();
		assertEquals("{ \"name\" : 1 , \"age\" : -1}", i.getIndexKeys().toString());
		assertEquals("{ \"name\" : \"test\" , \"unique\" : true}", i.getIndexOptions().toString());
	}

	@Test
	public void testWithDropDuplicates() {
		Index i = new Index().on("name", Order.ASCENDING);
		i.unique(Duplicates.DROP);
		assertEquals("{ \"name\" : 1}", i.getIndexKeys().toString());
		assertEquals("{ \"unique\" : true , \"dropDups\" : true}", i.getIndexOptions().toString());
	}

	@Test
	public void testWithSparse() {
		Index i = new Index().on("name", Order.ASCENDING);
		i.sparse().unique();
		assertEquals("{ \"name\" : 1}", i.getIndexKeys().toString());
		assertEquals("{ \"unique\" : true , \"sparse\" : true}", i.getIndexOptions().toString());
	}

	@Test
	public void testGeospatialIndex() {
		GeospatialIndex i = new GeospatialIndex("location").withMin(0);
		assertEquals("{ \"location\" : \"2d\"}", i.getIndexKeys().toString());
		assertEquals("{ \"min\" : 0}", i.getIndexOptions().toString());
	}

	@Test
	public void ensuresPropertyOrder() {

		Index on = new Index("foo", Order.ASCENDING).on("bar", Order.ASCENDING);
		assertThat(on.getIndexKeys().toString(), is("{ \"foo\" : 1 , \"bar\" : 1}"));
	}
}
