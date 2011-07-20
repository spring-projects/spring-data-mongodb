package org.springframework.data.mongodb.core.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.geo.Metrics;

/**
 *
 * @author Oliver Gierke
 */
public class NearQueryUnitTests {

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullPoint() {
		NearQuery.near(null);
	}
	
	@Test
	public void settingUpNearWithMetricRecalculatesDistance() {
		
		NearQuery query = NearQuery.near(2.5, 2.5, Metrics.KILOMETERS).maxDistance(150);
		
		assertThat((Double) query.toDBObject().get("maxDistance"), is(0.02351783914331097));
		assertThat((Boolean) query.toDBObject().get("spherical"), is(true));
		assertThat((Double) query.toDBObject().get("distanceMultiplier"), is(Metrics.KILOMETERS.getMultiplier()));
	}
	
	@Test
	public void settingMetricRecalculatesMaxDistance() {
		
		NearQuery query = NearQuery.near(2.5, 2.5, Metrics.KILOMETERS).maxDistance(150);
		
		assertThat((Double) query.toDBObject().get("maxDistance"), is(0.02351783914331097));
		assertThat((Double) query.toDBObject().get("distanceMultiplier"), is(Metrics.KILOMETERS.getMultiplier()));
		
		query.inMiles();
		assertThat((Double) query.toDBObject().get("distanceMultiplier"), is(Metrics.MILES.getMultiplier()));
		
		NearQuery.near(2.5, 2.5).maxDistance(150).inKilometers();
		assertThat((Double) query.toDBObject().get("maxDistance"), is(0.02351783914331097));
	}
}
