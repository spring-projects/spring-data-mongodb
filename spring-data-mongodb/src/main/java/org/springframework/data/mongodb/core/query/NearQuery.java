/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *			http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.query;

import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.Metric;
import org.springframework.data.mongodb.core.geo.Metrics;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Builder class to build near-queries.
 * 
 * @author Oliver Gierke
 */
public class NearQuery {

	private final DBObject criteria;
	private Query query;
	private Double maxDistance;
	private Metric metric;

	/**
	 * Creates a new {@link NearQuery}.
	 * 
	 * @param point
	 */
	private NearQuery(Point point, Metric metric) {

		Assert.notNull(point);

		this.criteria = new BasicDBObject();
		this.criteria.put("near", point.asArray());

		this.metric = metric;
		if (metric != null) {
			spherical(true);
			distanceMultiplier(metric);
		}
	}

	/**
	 * Creates a new {@link NearQuery} starting near the given coordinates.
	 * 
	 * @param i
	 * @param j
	 * @return
	 */
	public static NearQuery near(double x, double y) {
		return near(x, y, null);
	}

	/**
	 * Creates a new {@link NearQuery} starting at the given coordinates using the given {@link Metric} to adapt given
	 * values to further configuration. E.g. setting a {@link #maxDistance(double)} will be interpreted as a value of the
	 * initially set {@link Metric}.
	 * 
	 * @param x
	 * @param y
	 * @param metric
	 * @return
	 */
	public static NearQuery near(double x, double y, Metric metric) {
		return near(new Point(x, y), metric);
	}

	/**
	 * Creates a new {@link NearQuery} starting at the given {@link Point}.
	 * 
	 * @param point must not be {@literal null}.
	 * @return
	 */
	public static NearQuery near(Point point) {
		return near(point, null);
	}

	/**
	 * Creates a {@link NearQuery} starting near the given {@link Point} using the given {@link Metric} to adapt given
	 * values to further configuration. E.g. setting a {@link #maxDistance(double)} will be interpreted as a value of the
	 * initially set {@link Metric}.
	 * 
	 * @param point must not be {@literal null}.
	 * @param metric
	 * @return
	 */
	public static NearQuery near(Point point, Metric metric) {
		Assert.notNull(point);
		return new NearQuery(point, metric);
	}
	
	/**
	 * Returns the {@link Metric} underlying the actual query.
	 * 
	 * @return
	 */
	public Metric getMetric() {
		return metric;
	}

	/**
	 * Configures the number of results to return.
	 * 
	 * @param num
	 * @return
	 */
	public NearQuery num(int num) {
		this.criteria.put("num", num);
		return this;
	}

	/**
	 * Sets the max distance results shall have from the configured origin. Will normalize the given value using a
	 * potentially already configured {@link Metric}.
	 * 
	 * @param maxDistance
	 * @return
	 */
	public NearQuery maxDistance(double maxDistance) {
		this.maxDistance = getNormalizedDistance(maxDistance, this.metric);
		return this;
	}

	/**
	 * Sets the maximum distance supplied in a given metric. Will normalize the distance but not reconfigure the query's
	 * {@link Metric}.
	 * 
	 * @param maxDistance
	 * @param metric must not be {@literal null}.
	 * @return
	 */
	public NearQuery maxDistance(double maxDistance, Metric metric) {
		Assert.notNull(metric);
		this.spherical(true);
		return maxDistance(getNormalizedDistance(maxDistance, metric));
	}
	
	/**
	 * Sets the maximum distance to the given {@link Distance}.
	 * 
	 * @param distance
	 * @return
	 */
	public NearQuery maxDistance(Distance distance) {
		Assert.notNull(distance);
		return maxDistance(distance.getValue(), distance.getMetric());
	}

	/**
	 * Configures a distance multiplier the resulting distances get applied.
	 * 
	 * @param distanceMultiplier
	 * @return
	 */
	public NearQuery distanceMultiplier(double distanceMultiplier) {
		this.criteria.put("distanceMultiplier", distanceMultiplier);
		return this;
	}

	/**
	 * Configures the distance multiplier to the multiplier of the given {@link Metric}. Does <em>not</em> recalculate the
	 * {@link #maxDistance(double)}.
	 * 
	 * @param metric must not be {@literal null}.
	 * @return
	 */
	public NearQuery distanceMultiplier(Metric metric) {
		Assert.notNull(metric);
		return distanceMultiplier(metric.getMultiplier());
	}

	/**
	 * Configures whether to return spherical values for the actual distance.
	 * 
	 * @param spherical
	 * @return
	 */
	public NearQuery spherical(boolean spherical) {
		this.criteria.put("spherical", spherical);
		return this;
	}

	/**
	 * Will cause the results' distances being returned in kilometers. Sets {@link #distanceMultiplier(double)} and
	 * {@link #spherical(boolean)} accordingly.
	 * 
	 * @return
	 */
	public NearQuery inKilometers() {
		return adaptMetric(Metrics.KILOMETERS);
	}

	/**
	 * Will cause the results' distances being returned in miles. Sets {@link #distanceMultiplier(double)} and
	 * {@link #spherical(boolean)} accordingly.
	 * 
	 * @return
	 */
	public NearQuery inMiles() {
		return adaptMetric(Metrics.MILES);
	}

	/**
	 * Configures the given {@link Metric} to be used as base on for this query and recalculate the maximum distance if no
	 * metric was set before.
	 * 
	 * @param metric
	 */
	private NearQuery adaptMetric(Metric metric) {

		if (this.metric == null && maxDistance != null) {
			maxDistance(this.maxDistance, metric);
		}

		spherical(true);
		return distanceMultiplier(metric);
	}

	/**
	 * Adds an actual query to the {@link NearQuery} to restrict the objects considered for the actual near operation.
	 * 
	 * @param query
	 * @return
	 */
	public NearQuery query(Query query) {
		this.query = query;
		return this;
	}

	/**
	 * Returns the {@link DBObject} built by the {@link NearQuery}.
	 * 
	 * @return
	 */
	public DBObject toDBObject() {

		BasicDBObject dbObject = new BasicDBObject(criteria.toMap());
		if (query != null) {
			dbObject.put("query", query.getQueryObject());
		}
		if (maxDistance != null) {
			dbObject.put("maxDistance", maxDistance);
		}

		return dbObject;
	}
	
	private double getNormalizedDistance(double distance, Metric metric) {
		return metric == null ? distance : distance / metric.getMultiplier();
	}
}
