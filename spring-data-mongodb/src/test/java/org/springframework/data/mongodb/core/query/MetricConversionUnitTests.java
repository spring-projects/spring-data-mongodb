/*
 * Copyright 2016-2019 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;

/**
 * Unit tests for {@link MetricConversion}.
 *
 * @author Mark Paluch
 */
public class MetricConversionUnitTests {

	@Test // DATAMONGO-1348
	public void shouldConvertMilesToMeters() {

		Distance distance = new Distance(1, Metrics.MILES);
		double distanceInMeters = MetricConversion.getDistanceInMeters(distance);

		assertThat(distanceInMeters, is(closeTo(1609.3438343d, 0.000000001)));
	}

	@Test // DATAMONGO-1348
	public void shouldConvertKilometersToMeters() {

		Distance distance = new Distance(1, Metrics.KILOMETERS);
		double distanceInMeters = MetricConversion.getDistanceInMeters(distance);

		assertThat(distanceInMeters, is(closeTo(1000, 0.000000001)));
	}

	@Test // DATAMONGO-1348
	public void shouldCalculateMetersToKilometersMultiplier() {

		double multiplier = MetricConversion.getMetersToMetricMultiplier(Metrics.KILOMETERS);

		assertThat(multiplier, is(closeTo(0.001, 0.000000001)));
	}

	@Test // DATAMONGO-1348
	public void shouldCalculateMetersToMilesMultiplier() {

		double multiplier = MetricConversion.getMetersToMetricMultiplier(Metrics.MILES);

		assertThat(multiplier, is(closeTo(0.00062137, 0.000000001)));
	}

}
