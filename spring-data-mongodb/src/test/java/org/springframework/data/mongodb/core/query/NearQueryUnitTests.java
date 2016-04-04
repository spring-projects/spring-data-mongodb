/*
 * Copyright 2011-2014 the original author or authors.
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

import org.junit.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.DBObjectTestUtils;

/**
 * Unit tests for {@link NearQuery}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class NearQueryUnitTests {

	private static final Distance ONE_FIFTY_KILOMETERS = new Distance(150, Metrics.KILOMETERS);

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullPoint() {
		NearQuery.near(null);
	}

	@Test
	public void settingUpNearWithMetricRecalculatesDistance() {

		NearQuery query = NearQuery.near(2.5, 2.5, Metrics.KILOMETERS).maxDistance(150);

		assertThat(query.getMaxDistance(), is(ONE_FIFTY_KILOMETERS));
		assertThat(query.getMetric(), is((Metric) Metrics.KILOMETERS));
		assertThat(query.isSpherical(), is(true));
	}

	@Test
	public void settingMetricRecalculatesMaxDistance() {

		NearQuery query = NearQuery.near(2.5, 2.5, Metrics.KILOMETERS).maxDistance(150);

		query.inMiles();

		assertThat(query.getMetric(), is((Metric) Metrics.MILES));
	}

	@Test
	public void configuresResultMetricCorrectly() {

		NearQuery query = NearQuery.near(2.5, 2.1);
		assertThat(query.getMetric(), is((Metric) Metrics.NEUTRAL));

		query = query.maxDistance(ONE_FIFTY_KILOMETERS);
		assertThat(query.getMetric(), is((Metric) Metrics.KILOMETERS));
		assertThat(query.getMaxDistance(), is(ONE_FIFTY_KILOMETERS));
		assertThat(query.isSpherical(), is(true));

		query = query.in(Metrics.MILES);
		assertThat(query.getMetric(), is((Metric) Metrics.MILES));
		assertThat(query.getMaxDistance(), is(ONE_FIFTY_KILOMETERS));
		assertThat(query.isSpherical(), is(true));

		query = query.maxDistance(new Distance(200, Metrics.KILOMETERS));
		assertThat(query.getMetric(), is((Metric) Metrics.MILES));
	}

	/**
	 * @see DATAMONGO-445
	 */
	@Test
	public void shouldTakeSkipAndLimitSettingsFromGivenPageable() {

		Pageable pageable = new PageRequest(3, 5);
		NearQuery query = NearQuery.near(new Point(1, 1)).with(pageable);

		assertThat(query.getSkip(), is(pageable.getPageNumber() * pageable.getPageSize()));
		assertThat((Integer) query.toDocument().get("num"), is((pageable.getPageNumber() + 1) * pageable.getPageSize()));
	}

	/**
	 * @see DATAMONGO-445
	 */
	@Test
	public void shouldTakeSkipAndLimitSettingsFromGivenQuery() {

		int limit = 10;
		int skip = 5;
		NearQuery query = NearQuery.near(new Point(1, 1))
				.query(Query.query(Criteria.where("foo").is("bar")).limit(limit).skip(skip));

		assertThat(query.getSkip(), is(skip));
		assertThat((Integer) query.toDocument().get("num"), is(limit));
	}

	/**
	 * @see DATAMONGO-445
	 */
	@Test
	public void shouldTakeSkipAndLimitSettingsFromPageableEvenIfItWasSpecifiedOnQuery() {

		int limit = 10;
		int skip = 5;
		Pageable pageable = new PageRequest(3, 5);
		NearQuery query = NearQuery.near(new Point(1, 1))
				.query(Query.query(Criteria.where("foo").is("bar")).limit(limit).skip(skip)).with(pageable);

		assertThat(query.getSkip(), is(pageable.getPageNumber() * pageable.getPageSize()));
		assertThat((Integer) query.toDocument().get("num"), is((pageable.getPageNumber() + 1) * pageable.getPageSize()));
	}

	/**
	 * @see DATAMONGO-829
	 */
	@Test
	public void nearQueryShouldInoreZeroLimitFromQuery() {

		NearQuery query = NearQuery.near(new Point(1, 2)).query(Query.query(Criteria.where("foo").is("bar")));
		assertThat(query.toDocument().get("num"), nullValue());
	}

	/**
	 * @see DATAMONOGO-829
	 */
	@Test(expected = IllegalArgumentException.class)
	public void nearQueryShouldThrowExceptionWhenGivenANullQuery() {
		NearQuery.near(new Point(1, 2)).query(null);
	}

	/**
	 * @see DATAMONGO-829
	 */
	@Test
	public void numShouldNotBeAlteredByQueryWithoutPageable() {

		int num = 100;
		NearQuery query = NearQuery.near(new Point(1, 2));
		query.num(num);
		query.query(Query.query(Criteria.where("foo").is("bar")));

		assertThat(DBObjectTestUtils.getTypedValue(query.toDocument(), "num", Integer.class), is(num));
	}
}
