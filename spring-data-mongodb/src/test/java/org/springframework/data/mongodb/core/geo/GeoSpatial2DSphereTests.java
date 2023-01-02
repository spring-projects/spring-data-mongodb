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

package org.springframework.data.mongodb.core.geo;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.List;

import org.junit.Test;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.Venue;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;

/**
 * @author Christoph Strobl
 */
public class GeoSpatial2DSphereTests extends AbstractGeoSpatialTests {

	@Test // DATAMONGO-360
	public void indexInfoIsCorrect() {

		IndexOperations operations = template.indexOps(Venue.class);
		List<IndexInfo> indexInfo = operations.getIndexInfo();

		assertThat(indexInfo.size()).isEqualTo(2);

		List<IndexField> fields = indexInfo.get(0).getIndexFields();
		assertThat(fields.size()).isEqualTo(1);
		assertThat(fields).contains(IndexField.create("_id", Direction.ASC));

		fields = indexInfo.get(1).getIndexFields();
		assertThat(fields.size()).isEqualTo(1);
		assertThat(fields).contains(IndexField.geo("location"));
	}

	@Test // DATAMONGO-1110
	public void geoNearWithMinDistance() {

		NearQuery geoNear = NearQuery.near(-73, 40, Metrics.KILOMETERS).limit(10).minDistance(1);

		GeoResults<Venue> result = template.geoNear(geoNear, Venue.class);

		assertThat(result.getContent().size()).isNotEqualTo(0);
		assertThat(result.getAverageDistance().getMetric()).isEqualTo((Metric) Metrics.KILOMETERS);
	}

	@Test // DATAMONGO-1110
	public void nearSphereWithMinDistance() {

		Point point = new Point(-73.99171, 40.738868);
		Query query = query(where("location").nearSphere(point).minDistance(0.01));

		List<Venue> venues = template.find(query, Venue.class);
		assertThat(venues.size()).isEqualTo(1);
	}

	@Test
	public void countNearSphereWithMinDistance() {

		Point point = new Point(-73.99171, 40.738868);
		Query query = query(where("location").nearSphere(point).minDistance(0.01));

		assertThat(template.count(query, Venue.class)).isEqualTo(1);
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
