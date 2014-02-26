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
package org.springframework.data.mongodb.core.geo;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.geo.Metrics.*;

import org.junit.Test;
import org.springframework.data.geo.Metric;

/**
 * Unit tests for {@link Distance}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class DistanceUnitTests {

	@Test
	public void defaultsMetricToNeutralOne() {

		assertThat(new Distance(2.5).getMetric(), is((Metric) org.springframework.data.geo.Metrics.NEUTRAL));
		assertThat(new Distance(2.5, null).getMetric(), is((Metric) org.springframework.data.geo.Metrics.NEUTRAL));
	}

	@Test
	public void addsDistancesWithoutExplicitMetric() {

		Distance left = new Distance(2.5, KILOMETERS);
		Distance right = new Distance(2.5, KILOMETERS);

		assertThat(left.add(right), is(new org.springframework.data.geo.Distance(5.0, KILOMETERS)));
	}

	@Test
	public void addsDistancesWithExplicitMetric() {

		Distance left = new Distance(2.5, KILOMETERS);
		Distance right = new Distance(2.5, KILOMETERS);

		assertThat(left.add(right, MILES), is(new org.springframework.data.geo.Distance(3.106856281073925, MILES)));
	}
}
