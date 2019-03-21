/*
 * Copyright 2018 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.geo.Point;

/**
 * Unit tests for {@link SessionQueryContext}.
 *
 * @author Christoph Strobl
 * @see <a href="https://jira.mongodb.org/browse/DRIVERS-518">MongoDB: DRIVERS-518</a>
 */
public class SessionQueryContextUnitTests {

	@Test // DATAMONGO-2059
	public void rendersNearQueryAsGeoWithin() {

		Document queryObject = query(where("location").near(new Point(-73D, 40D)))
				.getQueryObject(SessionQueryContext.sessionContext());

		assertThat(queryObject).containsEntry("location.$geoWithin.$center",
				Arrays.asList(Arrays.asList(-73D, 40D), Double.MAX_VALUE));
	}

	@Test // DATAMONGO-2059
	public void rendersNearQueryAsGeoWithin1() {

		Document queryObject = query(where("location").near(new Point(-73D, 40D)))
				.getQueryObject(SessionQueryContext.sessionContext());

		assertThat(queryObject).containsEntry("location.$geoWithin.$center",
				Arrays.asList(Arrays.asList(-73D, 40D), Double.MAX_VALUE));
	}

	@Test // DATAMONGO-2059
	public void rendersNearSphereQueryAsGeoWithin() {

		Document queryObject = query(where("location").nearSphere(new Point(-73D, 40D)))
				.getQueryObject(SessionQueryContext.sessionContext());

		assertThat(queryObject).containsEntry("location.$geoWithin.$centerSphere",
				Arrays.asList(Arrays.asList(-73D, 40D), Double.MAX_VALUE));
	}

	@Test // DATAMONGO-2059
	public void rendersNearQueryAsGeoWithinWithMaxDistance() {

		Document queryObject = query(where("location").near(new Point(-73D, 40D)).maxDistance(10D))
				.getQueryObject(SessionQueryContext.sessionContext());

		assertThat(queryObject).containsEntry("location.$geoWithin.$center", Arrays.asList(Arrays.asList(-73D, 40D), 10D));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAMONGO-2059
	public void rendersNearQueryAsGeoWithinWithMinDistance() {

		query(where("location").near(new Point(-73D, 40D)).minDistance(10D))
				.getQueryObject(SessionQueryContext.sessionContext());
	}
}
