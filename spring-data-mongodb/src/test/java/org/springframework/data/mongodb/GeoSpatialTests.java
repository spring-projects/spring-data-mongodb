/*
 * Copyright 2010-2011 the original author or authors.
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

package org.springframework.data.mongodb;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.CollectionCallback;
import org.springframework.data.mongodb.MongoTemplate;
import org.springframework.data.mongodb.geo.Box;
import org.springframework.data.mongodb.geo.Circle;
import org.springframework.data.mongodb.geo.Point;
import org.springframework.data.mongodb.monitor.ServerInfo;
import org.springframework.data.mongodb.query.Criteria;
import org.springframework.data.mongodb.query.GeospatialIndex;
import org.springframework.data.mongodb.query.Query;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

/**
 * Modified from https://github.com/deftlabs/mongo-java-geospatial-example
 * 
 * @author Mark Pollack
 * 
 */
public class GeoSpatialTests {

	private static final Log LOGGER = LogFactory.getLog(GeoSpatialTests.class);
	private final String[] collectionsToDrop = new String[] { "newyork", "Person" };

	ApplicationContext applicationContext;
	MongoTemplate template;
	ServerInfo serverInfo;

	ExpressionParser parser;

	@Before
	public void setUp() throws Exception {
		cleanDb();
		applicationContext = new AnnotationConfigApplicationContext(GeoSpatialAppConfig.class);
		template = applicationContext.getBean(MongoTemplate.class);
		template.setWriteConcern(WriteConcern.FSYNC_SAFE);
		template.ensureIndex(new GeospatialIndex("location"), Venue.class);
		indexCreated();
		addVenues();
		parser = new SpelExpressionParser();
	}

	@After
	public void cleanUp() throws Exception {
		cleanDb();
	}

	private void cleanDb() throws UnknownHostException {
		Mongo mongo = new Mongo();
		serverInfo = new ServerInfo(mongo);
		DB db = mongo.getDB("database");
		for (String coll : collectionsToDrop) {
			db.getCollection(coll).drop();
		}
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

	/*
	public void geoNear() {
	  GeoNearResult<Venue> geoNearResult = template.geoNear(new Query(Criteria.where("type").is("Office")), Venue.class, 
	                   GeoNearCriteria.near(2,3).num(10).maxDistance(10).distanceMultiplier(10).spherical(true));
	}*/

	@Test
	public void withinCenter() {

		Circle circle = new Circle(-73.99171, 40.738868, 0.01);
		List<Venue> venues = template.find(new Query(Criteria.where("location").withinCenter(circle)), Venue.class);
		assertThat(venues.size(), equalTo(7));
	}

	@Test
	public void withinCenterSphere() {
		Circle circle = new Circle(-73.99171, 40.738868, 0.003712240453784);
		List<Venue> venues = template.find(new Query(Criteria.where("location").withinCenterSphere(circle)), Venue.class);
		assertThat(venues.size(), equalTo(11));
	}

	@Test
	public void withinBox() {
		Box box = new Box(new Point(-73.99756, 40.73083), new Point(-73.988135, 40.741404));
		// Box box = newBox.lowerLeft(x,y).upperRight(x,y);
		List<Venue> venues = template.find(new Query(Criteria.where("location").withinBox(box)), Venue.class);
		assertThat(venues.size(), equalTo(4));
	}

	@Test
	public void nearPoint() {
		Point point = new Point(-73.99171, 40.738868);
		List<Venue> venues = template
				.find(new Query(Criteria.where("location").near(point).maxDistance(0.01)), Venue.class);
		assertThat(venues.size(), equalTo(7));
	}

	@Test
	public void nearSphere() {
		Point point = new Point(-73.99171, 40.738868);
		List<Venue> venues = template.find(
				new Query(Criteria.where("location").nearSphere(point).maxDistance(0.003712240453784)), Venue.class);
		assertThat(venues.size(), equalTo(11));
	}

	@Test
	public void searchAllData() {
		assertThat(template, notNullValue());
		Venue foundVenue = template.findOne(new Query(Criteria.where("name").is("Penn Station")), Venue.class);
		assertThat(foundVenue, notNullValue());
		List<Venue> venues = template.findAll(Venue.class);
		assertThat(venues.size(), equalTo(12));
		Collection<?> names = (Collection<?>) parser.parseExpression("![name]").getValue(venues);
		assertThat(names.size(), equalTo(12));
		org.springframework.util.Assert.notEmpty(names);

	}

	public void indexCreated() {
		List<DBObject> indexInfo = getIndexInfo(Venue.class);
		LOGGER.debug(indexInfo);
		assertThat(indexInfo.size(), equalTo(2));
		assertThat(indexInfo.get(1).get("name").toString(), equalTo("location_2d"));
		assertThat(indexInfo.get(1).get("ns").toString(), equalTo("database.newyork"));
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
