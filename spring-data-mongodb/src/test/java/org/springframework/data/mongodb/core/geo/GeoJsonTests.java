/*
 * Copyright 2015 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class GeoJsonTests {

	@Configuration
	static class TestConfig extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		public Mongo mongo() throws Exception {
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
	 * @see DATAMONGO-1135
	 */
	@Test
	public void geoNear() {

		NearQuery geoNear = NearQuery.near(-73, 40, Metrics.KILOMETERS).num(10).maxDistance(150);

		GeoResults<Venue2DSphere> result = template.geoNear(geoNear, Venue2DSphere.class);

		assertThat(result.getContent().size(), is(not(0)));
		assertThat(result.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void withinCenter() {

		Circle circle = new Circle(-73.99171, 40.738868, 0.01);
		List<Venue2DSphere> venues = template.find(query(where("location").within(circle)), Venue2DSphere.class);
		assertThat(venues.size(), is(7));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void withinCenterSphere() {

		Circle circle = new Circle(-73.99171, 40.738868, 0.003712240453784);
		List<Venue2DSphere> venues = template.find(query(where("location").withinSphere(circle)), Venue2DSphere.class);
		assertThat(venues.size(), is(11));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void withinBox() {

		Box box = new Box(new Point(-73.99756, 40.73083), new Point(-73.988135, 40.741404));
		List<Venue2DSphere> venues = template.find(query(where("location").within(box)), Venue2DSphere.class);
		assertThat(venues.size(), is(4));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void withinPolygon() {

		Point first = new Point(-73.99756, 40.73083);
		Point second = new Point(-73.99756, 40.741404);
		Point third = new Point(-73.988135, 40.741404);
		Point fourth = new Point(-73.988135, 40.73083);

		Polygon polygon = new Polygon(first, second, third, fourth);

		List<Venue2DSphere> venues = template.find(query(where("location").within(polygon)), Venue2DSphere.class);
		assertThat(venues.size(), is(4));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void nearPoint() {

		Point point = new Point(-73.99171, 40.738868);

		List<Venue2DSphere> venues = template.find(query(where("location").near(point).maxDistance(0.01)),
				Venue2DSphere.class);
		assertThat(venues.size(), is(1));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void nearSphere() {

		Point point = new Point(-73.99171, 40.738868);
		Query query = query(where("location").nearSphere(point).maxDistance(0.003712240453784));
		List<Venue2DSphere> venues = template.find(query, Venue2DSphere.class);
		assertThat(venues.size(), is(1));
	}

	private void addVenues() {

		template.insert(new Venue2DSphere("Penn Station", -73.99408, 40.75057));
		template.insert(new Venue2DSphere("10gen Office", -73.99171, 40.738868));
		template.insert(new Venue2DSphere("Flatiron Building", -73.988135, 40.741404));
		template.insert(new Venue2DSphere("Players Club", -73.997812, 40.739128));
		template.insert(new Venue2DSphere("City Bakery ", -73.992491, 40.738673));
		template.insert(new Venue2DSphere("Splash Bar", -73.992491, 40.738673));
		template.insert(new Venue2DSphere("Momofuku Milk Bar", -73.985839, 40.731698));
		template.insert(new Venue2DSphere("Shake Shack", -73.98820, 40.74164));
		template.insert(new Venue2DSphere("Penn Station", -73.99408, 40.75057));
		template.insert(new Venue2DSphere("Empire State Building", -73.98602, 40.74894));
		template.insert(new Venue2DSphere("Ulaanbaatar, Mongolia", 106.9154, 47.9245));
		template.insert(new Venue2DSphere("Maplewood, NJ", -74.2713, 40.73137));
	}

	protected void createIndex() {
		dropIndex();
		template.indexOps(Venue2DSphere.class).ensureIndex(
				new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2DSPHERE));
	}

	protected void dropIndex() {
		try {
			template.indexOps(Venue2DSphere.class).dropIndex("location");
		} catch (Exception e) {

		}
	}

	protected void removeVenues() {
		template.dropCollection(Venue2DSphere.class);
	}

	@Document(collection = "venue2dsphere")
	static class Venue2DSphere {

		@Id private String id;
		private String name;
		private @GeoSpatialIndexed(type = GeoSpatialIndexType.GEO_2DSPHERE) double[] location;

		@PersistenceConstructor
		public Venue2DSphere(String name, double[] location) {
			this.name = name;
			this.location = location;
		}

		public Venue2DSphere(String name, double x, double y) {
			this.name = name;
			this.location = new double[] { x, y };
		}

		public String getName() {
			return name;
		}

		public double[] getLocation() {
			return location;
		}

		@Override
		public String toString() {
			return "Venue2DSphere [id=" + id + ", name=" + name + ", location=" + Arrays.toString(location) + "]";
		}
	}

}
