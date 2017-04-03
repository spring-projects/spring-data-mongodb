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

import java.util.Arrays;
import java.util.List;

import com.mongodb.client.MongoCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.BasicDbListBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
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
		removeCollections();
	}

	@Test // DATAMONGO-1135
	public void geoNear() {

		NearQuery geoNear = NearQuery.near(new GeoJsonPoint(-73, 40), Metrics.KILOMETERS).num(10).maxDistance(150);

		GeoResults<Venue2DSphere> result = template.geoNear(geoNear, Venue2DSphere.class);

		assertThat(result.getContent().size(), is(not(0)));
		assertThat(result.getAverageDistance().getMetric(), is((Metric) Metrics.KILOMETERS));
	}

	@Test // DATAMONGO-1135
	public void withinPolygon() {

		Point first = new Point(-73.99756, 40.73083);
		Point second = new Point(-73.99756, 40.741404);
		Point third = new Point(-73.988135, 40.741404);
		Point fourth = new Point(-73.988135, 40.73083);

		GeoJsonPolygon polygon = new GeoJsonPolygon(first, second, third, fourth, first);

		List<Venue2DSphere> venues = template.find(query(where("location").within(polygon)), Venue2DSphere.class);
		assertThat(venues.size(), is(4));
	}

	@Test // DATAMONGO-1135
	public void nearPoint() {

		GeoJsonPoint point = new GeoJsonPoint(-73.99171, 40.738868);

		Query query = query(where("location").near(point).maxDistance(0.01));
		List<Venue2DSphere> venues = template.find(query, Venue2DSphere.class);
		assertThat(venues.size(), is(1));
	}

	@Test // DATAMONGO-1135
	public void nearSphere() {

		GeoJsonPoint point = new GeoJsonPoint(-73.99171, 40.738868);

		Query query = query(where("location").nearSphere(point).maxDistance(0.003712240453784));
		List<Venue2DSphere> venues = template.find(query, Venue2DSphere.class);

		assertThat(venues.size(), is(1));
	}

	@Test // DATAMONGO-1137
	public void shouleSaveAndRetrieveDocumentWithGeoJsonPointTypeCorrectly() {

		DocumentWithPropertyUsingGeoJsonType obj = new DocumentWithPropertyUsingGeoJsonType();
		obj.id = "geoJsonPoint";
		obj.geoJsonPoint = new GeoJsonPoint(100, 50);

		template.save(obj);

		DocumentWithPropertyUsingGeoJsonType result = template.findOne(query(where("id").is(obj.id)),
				DocumentWithPropertyUsingGeoJsonType.class);

		assertThat(result.geoJsonPoint, equalTo(obj.geoJsonPoint));
	}

	@Test // DATAMONGO-1137
	public void shouleSaveAndRetrieveDocumentWithGeoJsonPolygonTypeCorrectly() {

		DocumentWithPropertyUsingGeoJsonType obj = new DocumentWithPropertyUsingGeoJsonType();
		obj.id = "geoJsonPolygon";
		obj.geoJsonPolygon = new GeoJsonPolygon(new Point(0, 0), new Point(0, 1), new Point(1, 1), new Point(1, 0),
				new Point(0, 0));

		template.save(obj);

		DocumentWithPropertyUsingGeoJsonType result = template.findOne(query(where("id").is(obj.id)),
				DocumentWithPropertyUsingGeoJsonType.class);

		assertThat(result.geoJsonPolygon, equalTo(obj.geoJsonPolygon));
	}

	@Test // DATAMONGO-1137
	public void shouleSaveAndRetrieveDocumentWithGeoJsonLineStringTypeCorrectly() {

		DocumentWithPropertyUsingGeoJsonType obj = new DocumentWithPropertyUsingGeoJsonType();
		obj.id = "geoJsonLineString";
		obj.geoJsonLineString = new GeoJsonLineString(new Point(0, 0), new Point(0, 1), new Point(1, 1));

		template.save(obj);

		DocumentWithPropertyUsingGeoJsonType result = template.findOne(query(where("id").is(obj.id)),
				DocumentWithPropertyUsingGeoJsonType.class);

		assertThat(result.geoJsonLineString, equalTo(obj.geoJsonLineString));
	}

	@Test // DATAMONGO-1137
	public void shouleSaveAndRetrieveDocumentWithGeoJsonMultiLineStringTypeCorrectly() {

		DocumentWithPropertyUsingGeoJsonType obj = new DocumentWithPropertyUsingGeoJsonType();
		obj.id = "geoJsonMultiLineString";
		obj.geoJsonMultiLineString = new GeoJsonMultiLineString(Arrays.asList(new GeoJsonLineString(new Point(0, 0),
				new Point(0, 1), new Point(1, 1)), new GeoJsonLineString(new Point(199, 0), new Point(2, 3))));

		template.save(obj);

		DocumentWithPropertyUsingGeoJsonType result = template.findOne(query(where("id").is(obj.id)),
				DocumentWithPropertyUsingGeoJsonType.class);

		assertThat(result.geoJsonMultiLineString, equalTo(obj.geoJsonMultiLineString));
	}

	@Test // DATAMONGO-1137
	public void shouleSaveAndRetrieveDocumentWithGeoJsonMultiPointTypeCorrectly() {

		DocumentWithPropertyUsingGeoJsonType obj = new DocumentWithPropertyUsingGeoJsonType();
		obj.id = "geoJsonMultiPoint";
		obj.geoJsonMultiPoint = new GeoJsonMultiPoint(Arrays.asList(new Point(0, 0), new Point(0, 1), new Point(1, 1)));

		template.save(obj);

		DocumentWithPropertyUsingGeoJsonType result = template.findOne(query(where("id").is(obj.id)),
				DocumentWithPropertyUsingGeoJsonType.class);

		assertThat(result.geoJsonMultiPoint, equalTo(obj.geoJsonMultiPoint));
	}

	@Test // DATAMONGO-1137
	public void shouleSaveAndRetrieveDocumentWithGeoJsonMultiPolygonTypeCorrectly() {

		DocumentWithPropertyUsingGeoJsonType obj = new DocumentWithPropertyUsingGeoJsonType();
		obj.id = "geoJsonMultiPolygon";
		obj.geoJsonMultiPolygon = new GeoJsonMultiPolygon(Arrays.asList(new GeoJsonPolygon(new Point(0, 0),
				new Point(0, 1), new Point(1, 1), new Point(0, 0))));

		template.save(obj);

		DocumentWithPropertyUsingGeoJsonType result = template.findOne(query(where("id").is(obj.id)),
				DocumentWithPropertyUsingGeoJsonType.class);

		assertThat(result.geoJsonMultiPolygon, equalTo(obj.geoJsonMultiPolygon));
	}

	@Test // DATAMONGO-1137
	public void shouleSaveAndRetrieveDocumentWithGeoJsonGeometryCollectionTypeCorrectly() {

		DocumentWithPropertyUsingGeoJsonType obj = new DocumentWithPropertyUsingGeoJsonType();
		obj.id = "geoJsonGeometryCollection";
		obj.geoJsonGeometryCollection = new GeoJsonGeometryCollection(Arrays.<GeoJson<?>> asList(
				new GeoJsonPoint(100, 200), new GeoJsonPolygon(new Point(0, 0), new Point(0, 1), new Point(1, 1), new Point(1,
						0), new Point(0, 0))));

		template.save(obj);

		DocumentWithPropertyUsingGeoJsonType result = template.findOne(query(where("id").is(obj.id)),
				DocumentWithPropertyUsingGeoJsonType.class);

		assertThat(result.geoJsonGeometryCollection, equalTo(obj.geoJsonGeometryCollection));
	}

	@Test // DATAMONGO-1110
	public void nearWithMinDistance() {

		Point point = new GeoJsonPoint(-73.99171, 40.738868);
		List<Venue2DSphere> venues = template.find(query(where("location").near(point).minDistance(0.01)),
				Venue2DSphere.class);

		assertThat(venues.size(), is(11));
	}

	@Test // DATAMONGO-1110
	public void nearSphereWithMinDistance() {

		Point point = new GeoJsonPoint(-73.99171, 40.738868);
		List<Venue2DSphere> venues = template.find(query(where("location").nearSphere(point).minDistance(0.01)),
				Venue2DSphere.class);

		assertThat(venues.size(), is(11));
	}

	@Test // DATAMONGO-1135
	public void nearWithMinAndMaxDistance() {

		GeoJsonPoint point = new GeoJsonPoint(-73.99171, 40.738868);

		Query query = query(where("location").near(point).minDistance(0.01).maxDistance(100));
		List<Venue2DSphere> venues = template.find(query, Venue2DSphere.class);
		assertThat(venues.size(), is(2));
	}

	@Test // DATAMONGO-1453
	public void shouldConvertPointRepresentationCorrectlyWhenSourceCoordinatesUsesInteger() {

		this.template.execute(template.getCollectionName(DocumentWithPropertyUsingGeoJsonType.class),
				new CollectionCallback<Object>() {

					@Override
					public Object doInCollection(MongoCollection<org.bson.Document> collection) throws MongoException, DataAccessException {

						org.bson.Document pointRepresentation = new org.bson.Document();
						pointRepresentation.put("type", "Point");
						pointRepresentation.put("coordinates", new BasicDbListBuilder().add(0).add(0).get());

						org.bson.Document document = new org.bson.Document();
						document.append("_id", "datamongo-1453");
						document.append("geoJsonPoint", pointRepresentation);

						collection.insertOne(document);

						return document;
					}
				});

		assertThat(template.findOne(query(where("id").is("datamongo-1453")),
				DocumentWithPropertyUsingGeoJsonType.class).geoJsonPoint, is(equalTo(new GeoJsonPoint(0D, 0D))));
	}

	@Test // DATAMONGO-1453
	public void shouldConvertLineStringRepresentationCorrectlyWhenSourceCoordinatesUsesInteger() {

		this.template.execute(template.getCollectionName(DocumentWithPropertyUsingGeoJsonType.class),
				new CollectionCallback<Object>() {

					@Override
					public Object doInCollection(MongoCollection<org.bson.Document> collection) throws MongoException, DataAccessException {

						org.bson.Document lineStringRepresentation = new org.bson.Document();
						lineStringRepresentation.put("type", "LineString");
						lineStringRepresentation.put("coordinates",
								new BasicDbListBuilder().add(new BasicDbListBuilder().add(0).add(0).get())
										.add(new BasicDbListBuilder().add(1).add(1).get()).get());

						org.bson.Document document = new org.bson.Document();
						document.append("_id", "datamongo-1453");
						document.append("geoJsonLineString", lineStringRepresentation);

						collection.insertOne(document);

						return document;
					}
				});

		assertThat(
				template.findOne(query(where("id").is("datamongo-1453")),
						DocumentWithPropertyUsingGeoJsonType.class).geoJsonLineString,
				is(equalTo(new GeoJsonLineString(new Point(0D, 0D), new Point(1, 1)))));
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

	protected void removeCollections() {
		template.dropCollection(Venue2DSphere.class);
		template.dropCollection(DocumentWithPropertyUsingGeoJsonType.class);
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "venue2dsphere")
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

	static class DocumentWithPropertyUsingGeoJsonType {

		String id;
		GeoJsonPoint geoJsonPoint;
		GeoJsonPolygon geoJsonPolygon;
		GeoJsonLineString geoJsonLineString;
		GeoJsonMultiLineString geoJsonMultiLineString;
		GeoJsonMultiPoint geoJsonMultiPoint;
		GeoJsonMultiPolygon geoJsonMultiPolygon;
		GeoJsonGeometryCollection geoJsonGeometryCollection;
	}

}
