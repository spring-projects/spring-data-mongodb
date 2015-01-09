/*
 * Copyright 2010-2015 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.IndexOperations;
import org.springframework.data.mongodb.core.Venue;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;

/**
 * @author Christoph Strobl
 */
public class GeoSpatial2DSphereTests extends AbstractGeoSpatialTests {

	/**
	 * @see DATAMONGO-360
	 */
	@Test
	public void indexInfoIsCorrect() {

		IndexOperations operations = template.indexOps(Venue.class);
		List<IndexInfo> indexInfo = operations.getIndexInfo();

		assertThat(indexInfo.size(), is(2));

		List<IndexField> fields = indexInfo.get(0).getIndexFields();
		assertThat(fields.size(), is(1));
		assertThat(fields, hasItem(IndexField.create("_id", Direction.ASC)));

		fields = indexInfo.get(1).getIndexFields();
		assertThat(fields.size(), is(1));
		assertThat(fields, hasItem(IndexField.geo("location")));
	}

	@Override
	protected void createIndex() {
		template.indexOps(Venue.class).ensureIndex(new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2DSPHERE));
	}

	@Override
	protected void dropIndex() {
		template.indexOps(Venue.class).dropIndex("location_2dsphere");
	}

}
