/*
 * Copyright 2013-2014 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.query.NearQuery;

import com.mongodb.DBObject;

/**
 * Unit tests for {@link GeoNearOperation}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class GeoNearOperationUnitTests {

	/**
	 * @see DATAMONGO-1127
	 */
	@Test
	public void rendersNearQueryAsAggregationOperation() {

		NearQuery query = NearQuery.near(10.0, 10.0).withDistanceField("distance");
		GeoNearOperation operation = new GeoNearOperation(query);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		DBObject nearClause = DBObjectTestUtils.getAsDBObject(dbObject, "$geoNear");
		assertThat(nearClause, is(query.toDBObject()));
	}
}
