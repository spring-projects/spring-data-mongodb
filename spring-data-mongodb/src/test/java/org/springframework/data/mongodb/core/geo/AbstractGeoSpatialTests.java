/*
 * Copyright 2015-2017 the original author or authors.
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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.List;

import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.Venue;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

/**
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public abstract class AbstractGeoSpatialTests {

	@Configuration
	static class TestConfig extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		public MongoClient mongoClient() {
			return new MongoClient();
		}
	}

	@Autowired MongoTemplate template;

	@Before
	public void setUp() {

		template.setWriteConcern(WriteConcern.FSYNC_SAFE);

		createIndex();
		addVenues();
	}

	@After
	public void tearDown() {

		dropIndex();
		removeVenues();
	}

	/**
	 * Create the index required to run the tests.
	 */
	protected abstract void createIndex();

	/**
	 * Remove index
	 */
	protected abstract void dropIndex();

	protected void removeVenues() {
		template.dropCollection(Venue.class);
	}

	protected void addVenues() {

		template.insert(new Venue("Penn Station", -73.99408, 40.75057));
		template.insert(new Venue("10gen Office", -73.99171, 40.738868));
		template.insert(new Venue("Flatiron Building", -73.988135, 40.741404));
		template.insert(new Venue("Players Club", -73.997812, 40.739128));
		template.insert(new Venue("City Bakery ", -73.992491, 40.738673));
		template.insert(new Venue("Splash Bar", -73.992491, 40.738673));
		template.insert(new Venue("Momofuku Milk Bar", -73.985839, 40.731698));
		template.insert(new Venue("Shake Shack", -73.98820, 40.74164));
		template.insert(new Venue("Penn Station", -73.99408, 40.75057));
		template.insert(new Venue("Empire State Building", -73.98602, 40.74894));
		template.insert(new Venue("Ulaanbaatar, Mongolia", 106.9154, 47.9245));
		template.insert(new Venue("Maplewood, NJ", -74.2713, 40.73137));
	}

	@Test
	public void geoNear() {

		NearQuery geoNear = NearQuery.near(-73, 40, Metrics.KILOMETERS).num(10).maxDistance(150);

		GeoResults<Venue> result = template.geoNear(geoNear, Venue.class);

		assertThat(result.getContent().size(), is(not(0)));
		assertThat(result.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	@Test
	public void withinCenter() {

		Circle circle = new Circle(-73.99171, 40.738868, 0.01);
		List<Venue> venues = template.find(query(where("location").within(circle)), Venue.class);
		assertThat(venues.size(), is(7));
	}

	@Test
	public void withinCenterSphere() {

		Circle circle = new Circle(-73.99171, 40.738868, 0.003712240453784);
		List<Venue> venues = template.find(query(where("location").withinSphere(circle)), Venue.class);
		assertThat(venues.size(), is(11));
	}

	@Test
	public void withinBox() {

		Box box = new Box(new Point(-73.99756, 40.73083), new Point(-73.988135, 40.741404));
		List<Venue> venues = template.find(query(where("location").within(box)), Venue.class);
		assertThat(venues.size(), is(4));
	}

	@Test
	public void withinPolygon() {

		Point first = new Point(-73.99756, 40.73083);
		Point second = new Point(-73.99756, 40.741404);
		Point third = new Point(-73.988135, 40.741404);
		Point fourth = new Point(-73.988135, 40.73083);

		Polygon polygon = new Polygon(first, second, third, fourth);

		List<Venue> venues = template.find(query(where("location").within(polygon)), Venue.class);
		assertThat(venues.size(), is(4));
	}

	@Test
	public void nearSphere() {
		Point point = new Point(-73.99171, 40.738868);
		Query query = query(where("location").nearSphere(point).maxDistance(0.003712240453784));
		List<Venue> venues = template.find(query, Venue.class);
		assertThat(venues.size(), is(11));
	}

	@Test // DATAMONGO-1360
	public void mapsQueryContainedInNearQuery() {

		Query query = query(where("openingDate").lt(LocalDate.now()));
		template.geoNear(NearQuery.near(1.5, 1.7).spherical(true).query(query), Venue.class);
	}
}
