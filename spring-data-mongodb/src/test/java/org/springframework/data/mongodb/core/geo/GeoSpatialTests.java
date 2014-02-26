/*
 * Copyright 2010-2014 the original author or authors.
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

import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.config.AbstractIntegrationTests;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.IndexOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.Venue;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

/**
 * Modified from https://github.com/deftlabs/mongo-java-geospatial-example
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class GeoSpatialTests extends AbstractIntegrationTests {

	private static final Log LOGGER = LogFactory.getLog(GeoSpatialTests.class);

	@Autowired MongoTemplate template;

	ExpressionParser parser = new SpelExpressionParser();

	@Before
	public void setUp() throws Exception {

		template.setWriteConcern(WriteConcern.FSYNC_SAFE);
		template.indexOps(Venue.class).ensureIndex(new GeospatialIndex("location"));

		indexCreated();
		addVenues();
	}

	private void addVenues() {

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
		// template.insert(new Venue("Washington Square Park", -73.99756, 40.73083));
		template.insert(new Venue("Ulaanbaatar, Mongolia", 106.9154, 47.9245));
		template.insert(new Venue("Maplewood, NJ", -74.2713, 40.73137));
	}

	@Test
	public void geoNear() {

		NearQuery geoNear = NearQuery.near(-73, 40, Metrics.KILOMETERS).num(10).maxDistance(150);

		GeoResults<Venue> result = template.geoNear(geoNear, Venue.class);

		assertThat(result.getContent().size(), is(not(0)));
		assertThat((Metric) result.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
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
	public void nearPoint() {
		Point point = new Point(-73.99171, 40.738868);
		List<Venue> venues = template.find(query(where("location").near(point).maxDistance(0.01)), Venue.class);
		assertThat(venues.size(), is(7));
	}

	@Test
	public void nearSphere() {
		Point point = new Point(-73.99171, 40.738868);
		Query query = query(where("location").nearSphere(point).maxDistance(0.003712240453784));
		List<Venue> venues = template.find(query, Venue.class);
		assertThat(venues.size(), is(11));
	}

	@Test
	public void searchAllData() {

		Venue foundVenue = template.findOne(query(where("name").is("Penn Station")), Venue.class);
		assertThat(foundVenue, is(notNullValue()));

		List<Venue> venues = template.findAll(Venue.class);
		assertThat(venues.size(), is(12));

		Collection<?> names = (Collection<?>) parser.parseExpression("![name]").getValue(venues);
		assertThat(names.size(), is(12));

	}

	public void indexCreated() {

		List<DBObject> indexInfo = getIndexInfo(Venue.class);
		LOGGER.debug(indexInfo);

		assertThat(indexInfo.size(), is(2));
		assertThat(indexInfo.get(1).get("name").toString(), is("location_2d"));
		assertThat(indexInfo.get(1).get("ns").toString(), is("database.newyork"));
	}

	/**
	 * @see DATAMONGO-360
	 */
	@Test
	public void indexInfoIsCorrect() {

		IndexOperations operations = template.indexOps(Venue.class);
		List<IndexInfo> indexInfo = operations.getIndexInfo();

		assertThat(indexInfo.size(), is(2));

		List<IndexField> fields = indexInfo.get(0).getIndexFields();
		assertThat(fields.size(), is(1));
		assertThat(fields, hasItem(IndexField.create("_id", Direction.ASC)));

		fields = indexInfo.get(1).getIndexFields();
		assertThat(fields.size(), is(1));
		assertThat(fields, hasItem(IndexField.geo("location")));
	}

	// TODO move to MongoAdmin
	public List<DBObject> getIndexInfo(Class<?> clazz) {
		return template.execute(clazz, new CollectionCallback<List<DBObject>>() {

			public List<DBObject> doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				return collection.getIndexInfo();
			}
		});
	}
}
