/*
 * Copyright 2010-2013 the original author or authors.
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
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.Index.Duplicates;

/**
 * Unit tests for {@link Index}.
 * 
 * @author Oliver Gierke
 * @author Laurent Canet
 */
public class IndexUnitTests {

	@Test
	public void testWithAscendingIndex() {
		Index i = new Index().on("name", Direction.ASC);
		assertEquals(Document.parse("{ \"name\" : 1}"), i.getIndexKeys());
	}

	@Test
	public void testWithDescendingIndex() {
		Index i = new Index().on("name", Direction.DESC);
		assertEquals(Document.parse("{ \"name\" : -1}"), i.getIndexKeys());
	}

	@Test
	public void testNamedMultiFieldUniqueIndex() {
		Index i = new Index().on("name", Direction.ASC).on("age", Direction.DESC);
		i.named("test").unique();
		assertEquals(Document.parse("{ \"name\" : 1 , \"age\" : -1}"), i.getIndexKeys());
		assertEquals(Document.parse("{ \"name\" : \"test\" , \"unique\" : true}"), i.getIndexOptions());
	}

	@Test
	public void testWithSparse() {
		Index i = new Index().on("name", Direction.ASC);
		i.sparse().unique();
		assertEquals(Document.parse("{ \"name\" : 1}"), i.getIndexKeys());
		assertEquals(Document.parse("{ \"unique\" : true , \"sparse\" : true}"), i.getIndexOptions());
	}

	@Test
	public void testGeospatialIndex() {
		GeospatialIndex i = new GeospatialIndex("location").withMin(0);
		assertEquals(Document.parse("{ \"location\" : \"2d\"}"), i.getIndexKeys());
		assertEquals(Document.parse("{ \"min\" : 0}"), i.getIndexOptions());
	}

	/**
	 * @see DATAMONGO-778
	 */
	@Test
	public void testGeospatialIndex2DSphere() {

		GeospatialIndex i = new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2DSPHERE);
		assertEquals(Document.parse("{ \"location\" : \"2dsphere\"}"), i.getIndexKeys());
		assertEquals(Document.parse("{ }"), i.getIndexOptions());
	}

	/**
	 * @see DATAMONGO-778
	 */
	@Test
	public void testGeospatialIndexGeoHaystack() {

		GeospatialIndex i = new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_HAYSTACK)
				.withAdditionalField("name").withBucketSize(40);
		assertEquals(Document.parse("{ \"location\" : \"geoHaystack\" , \"name\" : 1}"), i.getIndexKeys());
		assertEquals(Document.parse("{ \"bucketSize\" : 40.0}"), i.getIndexOptions());
	}

	@Test
	public void ensuresPropertyOrder() {

		Index on = new Index("foo", Direction.ASC).on("bar", Direction.ASC);
		assertThat(on.getIndexKeys(), is(Document.parse("{ \"foo\" : 1 , \"bar\" : 1}")));
	}
}
