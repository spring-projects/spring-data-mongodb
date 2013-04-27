/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.springframework.data.mongodb.core.aggregation.GeoNearOperation.geoNear;
import static org.springframework.data.mongodb.core.query.Criteria.where;

import org.junit.Test;

import com.mongodb.DBObject;

/**
 * Tests of {@link GeoNearOperation}.
 * 
 * @see DATAMONGO-586
 * @author Sebastian Herold
 */
public class GeoNearOperationTest {

	@Test
	public void withoutOptionalFields() throws Exception {
		GeoNearOperation geoNearOperation = geoNear(1.0, 2.0, "distance");

		assertField(geoNearOperation, "near", new double[] { 1.0, 2.0 });
		assertField(geoNearOperation, "distanceField", "distance");
	}

	@Test
	public void withOptionalFields() throws Exception {
		GeoNearOperation geoNearOperation = geoNear(1.0, 2.0, "distance").distanceMultiplier(4.2)
				.includeLocs("include.locs").limit(42).maxDistance(45.5).num(43).query(where("foo").is("bar")).spherical(true)
				.uniqueDocs(true);

		assertField(geoNearOperation, "near", new double[] { 1.0, 2.0 });
		assertField(geoNearOperation, "distanceField", "distance");
		assertField(geoNearOperation, "distanceMultiplier", 4.2);
		assertField(geoNearOperation, "includeLocs", "include.locs");
		assertField(geoNearOperation, "limit", 42L);
		assertField(geoNearOperation, "maxDistance", 45.5);
		assertField(geoNearOperation, "num", 43L);
		assertField(geoNearOperation, "query", where("foo").is("bar").getCriteriaObject());
		assertField(geoNearOperation, "spherical", true);
		assertField(geoNearOperation, "uniqueDocs", true);
	}

	private void assertField(GeoNearOperation geoNearOperation, String key, Object value) {
		DBObject geoNearDbObject = (DBObject) geoNearOperation.getDBObject().get("$geoNear");
		assertThat(geoNearDbObject, is(notNullValue()));
		assertThat(geoNearDbObject.get(key), is(value));
	}

	@Test(expected = IllegalArgumentException.class)
	public void distanceFieldRequired() throws Exception {
		geoNear(0, 0, null);
	}
}
