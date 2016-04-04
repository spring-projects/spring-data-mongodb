/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.bson.Document;
import org.junit.Test;

/**
 * Unit tests for {@link GeoCommandStatistics}.
 * 
 * @author Oliver Gierke
 * @soundtrack Fruitcake - Jeff Coffin (The Inside of the Outside)
 */
public class GeoCommandStatisticsUnitTests {

	/**
	 * @see DATAMONGO-1361
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullCommandResult() {
		GeoCommandStatistics.from(null);
	}

	/**
	 * @see DATAMONGO-1361
	 */
	@Test
	public void fallsBackToNanIfNoAverageDistanceIsAvailable() {

		GeoCommandStatistics statistics = GeoCommandStatistics.from(new Document("stats", null));
		assertThat(statistics.getAverageDistance(), is(Double.NaN));

		statistics = GeoCommandStatistics.from(new Document("stats", new Document()));
		assertThat(statistics.getAverageDistance(), is(Double.NaN));
	}

	/**
	 * @see DATAMONGO-1361
	 */
	@Test
	public void returnsAverageDistanceIfPresent() {

		GeoCommandStatistics statistics = GeoCommandStatistics
				.from(new Document("stats", new Document("avgDistance", 1.5)));

		assertThat(statistics.getAverageDistance(), is(1.5));
	}
}
