/*
 * Copyright 2010-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.query;

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.Index;

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
		assertThat(i.getIndexKeys()).isEqualTo(Document.parse("{ \"name\" : 1}"));
	}

	@Test
	public void testWithDescendingIndex() {
		Index i = new Index().on("name", Direction.DESC);
		assertThat(i.getIndexKeys()).isEqualTo(Document.parse("{ \"name\" : -1}"));
	}

	@Test
	public void testNamedMultiFieldUniqueIndex() {
		Index i = new Index().on("name", Direction.ASC).on("age", Direction.DESC);
		i.named("test").unique();
		assertThat(i.getIndexKeys()).isEqualTo(Document.parse("{ \"name\" : 1 , \"age\" : -1}"));
		assertThat(i.getIndexOptions()).isEqualTo(Document.parse("{ \"name\" : \"test\" , \"unique\" : true}"));
	}

	@Test
	public void testWithSparse() {
		Index i = new Index().on("name", Direction.ASC);
		i.sparse().unique();
		assertThat(i.getIndexKeys()).isEqualTo(Document.parse("{ \"name\" : 1}"));
		assertThat(i.getIndexOptions()).isEqualTo(Document.parse("{ \"unique\" : true , \"sparse\" : true}"));
	}

	@Test
	public void testGeospatialIndex() {
		GeospatialIndex i = new GeospatialIndex("location").withMin(0);
		assertThat(i.getIndexKeys()).isEqualTo(Document.parse("{ \"location\" : \"2d\"}"));
		assertThat(i.getIndexOptions()).isEqualTo(Document.parse("{ \"min\" : 0}"));
	}

	@Test // DATAMONGO-778
	public void testGeospatialIndex2DSphere() {

		GeospatialIndex i = new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2DSPHERE);
		assertThat(i.getIndexKeys()).isEqualTo(Document.parse("{ \"location\" : \"2dsphere\"}"));
		assertThat(i.getIndexOptions()).isEqualTo(Document.parse("{ }"));
	}

	@Test // DATAMONGO-778
	public void testGeospatialIndexGeoHaystack() {

		GeospatialIndex i = new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_HAYSTACK)
				.withAdditionalField("name").withBucketSize(40);
		assertThat(i.getIndexKeys()).isEqualTo(Document.parse("{ \"location\" : \"geoHaystack\" , \"name\" : 1}"));
		assertThat(i.getIndexOptions()).isEqualTo(Document.parse("{ \"bucketSize\" : 40.0}"));
	}

	@Test
	public void ensuresPropertyOrder() {

		Index on = new Index("foo", Direction.ASC).on("bar", Direction.ASC);
		assertThat(on.getIndexKeys()).isEqualTo(Document.parse("{ \"foo\" : 1 , \"bar\" : 1}"));
	}
}
