/*
 * Copyright 2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

/**
 * A simple collection of grouped test entities used throughout the test suite.
 *
 * @author Christoph Strobl
 */
public class TestEntities {

	private static final GeoEntities GEO = new GeoEntities();

	public static GeoEntities geolocation() {
		return GEO;
	}

	public static class GeoEntities {

		/**
		 * <pre>
		 * X: -73.99408
		 * Y: 40.75057
		 * </pre>
		 *
		 * @return new {@link Venue}
		 */
		public Venue pennStation() {
			return new Venue("Penn Station", -73.99408, 40.75057);
		}

		/**
		 * <pre>
		 * X: -73.99171
		 * Y: 40.738868
		 * </pre>
		 *
		 * @return new {@link Venue}
		 */

		public Venue tenGenOffice() {
			return new Venue("10gen Office", -73.99171, 40.738868);
		}

		/**
		 * <pre>
		 * X: -73.988135
		 * Y: 40.741404
		 * </pre>
		 *
		 * @return new {@link Venue}
		 */
		public Venue flatironBuilding() {
			return new Venue("Flatiron Building", -73.988135, 40.741404);
		}

		/**
		 * <pre>
		 * X: -74.2713
		 * Y: 40.73137
		 * </pre>
		 *
		 * @return new {@link Venue}
		 */
		public Venue maplewoodNJ() {
			return new Venue("Maplewood, NJ", -74.2713, 40.73137);
		}

		public List<Venue> newYork() {

			List<Venue> venues = new ArrayList<>();

			venues.add(pennStation());
			venues.add(tenGenOffice());
			venues.add(flatironBuilding());
			venues.add(new Venue("Players Club", -73.997812, 40.739128));
			venues.add(new Venue("City Bakery ", -73.992491, 40.738673));
			venues.add(new Venue("Splash Bar", -73.992491, 40.738673));
			venues.add(new Venue("Momofuku Milk Bar", -73.985839, 40.731698));
			venues.add(new Venue("Shake Shack", -73.98820, 40.74164));
			venues.add(new Venue("Penn Station", -73.99408, 40.75057));
			venues.add(new Venue("Empire State Building", -73.98602, 40.74894));
			venues.add(new Venue("Ulaanbaatar, Mongolia", 106.9154, 47.9245));
			venues.add(maplewoodNJ());

			return venues;
		}
	}
}
