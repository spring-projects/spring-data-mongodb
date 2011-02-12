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
package org.springframework.data.document.mongodb.query;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.document.mongodb.query.Index.Duplicates;

public class IndexTests {

	@Test
	public void testWithAscendingIndex() {
		Index i = new Index().on("name", Order.ASCENDING);
		Assert.assertEquals("{ \"name\" : 1}", i.getIndexObject().toString());
	}

	@Test
	public void testWithDescendingIndex() {
		Index i = new Index().on("name", Order.DESCENDING);
		Assert.assertEquals("{ \"name\" : -1}", i.getIndexObject().toString());
	}

	@Test
	public void testNamedMultiFieldUniqueIndex() {
		Index i = new Index().on("name", Order.ASCENDING).on("age", Order.DESCENDING);
		i.named("test").unique();
		Assert.assertEquals("{ \"age\" : -1 , \"name\" : 1}", i.getIndexObject().toString());
		Assert.assertEquals("{ \"name\" : \"test\" , \"unique\" : true}", i.getIndexOptions().toString());
	}

	@Test
	public void testWithDropDuplicates() {
		Index i = new Index().on("name", Order.ASCENDING);
		i.unique(Duplicates.DROP);
		Assert.assertEquals("{ \"name\" : 1}", i.getIndexObject().toString());
		Assert.assertEquals("{ \"unique\" : true , \"drop_dups\" : true}", i.getIndexOptions().toString());
	}

	@Test
	public void testGeospatialIndex() {
		GeospatialIndex i = new GeospatialIndex("location").withMin(0);
		Assert.assertEquals("{ \"location\" : \"2d\"}", i.getIndexObject().toString());
		Assert.assertEquals("{ \"min\" : 0}", i.getIndexOptions().toString());
	}

}
