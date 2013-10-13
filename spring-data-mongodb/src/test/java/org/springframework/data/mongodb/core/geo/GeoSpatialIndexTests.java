package org.springframework.data.mongodb.core.geo;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.WriteResultChecking;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.index.GeospatialIndexType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

/**
 * Tests spatial index creation
 * 
 * @author Laurent Canet
 */
@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class })
@ContextConfiguration(classes = { GeoSpatialAppConfig.class })
public class GeoSpatialIndexTests {

	@Autowired private MongoTemplate template;

	@Before
	public void setUp() throws Exception {
		template.setWriteConcern(WriteConcern.FSYNC_SAFE);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);
	}

	static class GeoSpatialEntity2D {
		public String id;
		@GeoSpatialIndexed(type = GeospatialIndexType.GEO_2D) public Point location;

		public GeoSpatialEntity2D(double x, double y) {
			this.location = new Point(x, y);
		}
	}

	static class GeoSpatialEntityHaystack {
		public String id;
		public String name;
		@GeoSpatialIndexed(type = GeospatialIndexType.GEO_HAYSTACK, additionalField = "name") public Point location;

		public GeoSpatialEntityHaystack(double x, double y, String name) {
			this.location = new Point(x, y);
			this.name = name;
		}
	}

	static class GeoJsonPoint {
		String type = "Point";
		double coordinates[];
	}

	static class GeoSpatialEntity2DSphere {
		public String id;
		@GeoSpatialIndexed(type = GeospatialIndexType.GEO_2DSPHERE) public GeoJsonPoint location;

		public GeoSpatialEntity2DSphere(double x, double y) {
			this.location = new GeoJsonPoint();
			this.location.coordinates = new double[] { x, y };
		}
	}

	@Test
	public void test2dIndex() {
		try {
			template.save(new GeoSpatialEntity2D(45.2, 4.6));
			assertThat(hasIndexOfType(GeoSpatialEntity2D.class, "2d"), is(true));
		} finally {
			template.dropCollection(GeoSpatialEntity2D.class);
		}
	}

	@Test
	public void test2dSphereIndex() {
		try {
			template.save(new GeoSpatialEntity2DSphere(45.2, 4.6));
			assertThat(hasIndexOfType(GeoSpatialEntity2DSphere.class, "2dsphere"), is(true));
		} finally {
			template.dropCollection(GeoSpatialEntity2DSphere.class);
		}
	}

	@Test
	public void testHaystackIndex() {
		try {
			template.save(new GeoSpatialEntityHaystack(45.2, 4.6, "Paris"));
			assertThat(hasIndexOfType(GeoSpatialEntityHaystack.class, "geoHaystack"), is(true));
		} finally {
			template.dropCollection(GeoSpatialEntityHaystack.class);
		}
	}

	/**
	 * Returns whether an index with the given name exists for the given entity type.
	 * 
	 * @param indexName
	 * @param entityType
	 * @return
	 */
	private boolean hasIndexOfType(Class<?> entityType, final String type) {

		return template.execute(entityType, new CollectionCallback<Boolean>() {
			@SuppressWarnings("unchecked")
			public Boolean doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				for (DBObject indexInfo : collection.getIndexInfo()) {
					DBObject keys = (DBObject) indexInfo.get("key");
					Map<String, Object> keysMap = keys.toMap();
					for (String key : keysMap.keySet()) {
						Object indexType = keys.get(key);
						if (type.equals(indexType)) {
							return true;
						}
					}
				}
				return false;
			}
		});
	}

}
