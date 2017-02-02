/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.Arrays;

import org.bson.Document;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.CustomMetric;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Builder class to build near-queries.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public final class NearQuery {

	private final Point point;
	private Query query;
	private Distance maxDistance;
	private Distance minDistance;
	private Metric metric;
	private boolean spherical;
	private Long num;
	private Long skip;

	/**
	 * Creates a new {@link NearQuery}.
	 * 
	 * @param point must not be {@literal null}.
	 */
	private NearQuery(Point point, Metric metric) {

		Assert.notNull(point, "Point must not be null!");

		this.point = point;
		this.spherical = false;

		if (metric != null) {
			in(metric);
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
		return new NearQuery(point, metric);
	}

	/**
	 * Returns the {@link Metric} underlying the actual query. If no metric was set explicitly {@link Metrics#NEUTRAL}
	 * will be returned.
	 * 
	 * @return will never be {@literal null}.
	 */
	public Metric getMetric() {
		return metric == null ? Metrics.NEUTRAL : metric;
	}

	/**
	 * Configures the maximum number of results to return.
	 * 
	 * @param num
	 * @return
	 */
	public NearQuery num(long num) {
		this.num = num;
		return this;
	}

	/**
	 * Configures the number of results to skip.
	 * 
	 * @param skip
	 * @return
	 */
	public NearQuery skip(long skip) {
		this.skip = skip;
		return this;
	}

	/**
	 * Configures the {@link Pageable} to use.
	 * 
	 * @param pageable must not be {@literal null}
	 * @return
	 */
	public NearQuery with(Pageable pageable) {

		Assert.notNull(pageable, "Pageable must not be 'null'.");
		if(!ObjectUtils.nullSafeEquals(Pageable.NONE, pageable)) {
			this.num = pageable.getOffset() + pageable.getPageSize();
			this.skip = pageable.getOffset();
		}
		return this;
	}

	/**
	 * Sets the max distance results shall have from the configured origin. If a {@link Metric} was set before the given
	 * value will be interpreted as being a value in that metric. E.g.
	 * 
	 * <pre>
	 * NearQuery query = near(10.0, 20.0, Metrics.KILOMETERS).maxDistance(150);
	 * </pre>
	 * 
	 * Will set the maximum distance to 150 kilometers.
	 * 
	 * @param maxDistance
	 * @return
	 */
	public NearQuery maxDistance(double maxDistance) {
		return maxDistance(new Distance(maxDistance, getMetric()));
	}

	/**
	 * Sets the maximum distance supplied in a given metric. Will normalize the distance but not reconfigure the query's
	 * result {@link Metric} if one was configured before.
	 * 
	 * @param maxDistance
	 * @param metric must not be {@literal null}.
	 * @return
	 */
	public NearQuery maxDistance(double maxDistance, Metric metric) {

		Assert.notNull(metric, "Metric must not be null!");

		return maxDistance(new Distance(maxDistance, metric));
	}

	/**
	 * Sets the maximum distance to the given {@link Distance}. Will set the returned {@link Metric} to be the one of the
	 * given {@link Distance} if no {@link Metric} was set before.
	 * 
	 * @param distance must not be {@literal null}.
	 * @return
	 */
	public NearQuery maxDistance(Distance distance) {

		Assert.notNull(distance, "Distance must not be null!");

		if (distance.getMetric() != Metrics.NEUTRAL) {
			this.spherical(true);
		}

		if (this.metric == null) {
			in(distance.getMetric());
		}

		this.maxDistance = distance;
		return this;
	}

	/**
	 * Sets the minimum distance results shall have from the configured origin. If a {@link Metric} was set before the
	 * given value will be interpreted as being a value in that metric. E.g.
	 * 
	 * <pre>
	 * NearQuery query = near(10.0, 20.0, Metrics.KILOMETERS).minDistance(150);
	 * </pre>
	 * 
	 * Will set the minimum distance to 150 kilometers.
	 * 
	 * @param minDistance
	 * @return
	 * @since 1.7
	 */
	public NearQuery minDistance(double minDistance) {
		return minDistance(new Distance(minDistance, getMetric()));
	}

	/**
	 * Sets the minimum distance supplied in a given metric. Will normalize the distance but not reconfigure the query's
	 * result {@link Metric} if one was configured before.
	 * 
	 * @param minDistance
	 * @param metric must not be {@literal null}.
	 * @return
	 * @since 1.7
	 */
	public NearQuery minDistance(double minDistance, Metric metric) {

		Assert.notNull(metric, "Metric must not be null!");

		return minDistance(new Distance(minDistance, metric));
	}

	/**
	 * Sets the minimum distance to the given {@link Distance}. Will set the returned {@link Metric} to be the one of the
	 * given {@link Distance} if no {@link Metric} was set before.
	 * 
	 * @param distance must not be {@literal null}.
	 * @return
	 * @since 1.7
	 */
	public NearQuery minDistance(Distance distance) {

		Assert.notNull(distance, "Distance must not be null!");

		if (distance.getMetric() != Metrics.NEUTRAL) {
			this.spherical(true);
		}

		if (this.metric == null) {
			in(distance.getMetric());
		}

		this.minDistance = distance;
		return this;
	}

	/**
	 * Returns the maximum {@link Distance}.
	 * 
	 * @return
	 */
	public Distance getMaxDistance() {
		return this.maxDistance;
	}

	/**
	 * Returns the maximum {@link Distance}.
	 * 
	 * @return
	 * @since 1.7
	 */
	public Distance getMinDistance() {
		return this.minDistance;
	}

	/**
	 * Configures a {@link CustomMetric} with the given multiplier.
	 * 
	 * @param distanceMultiplier
	 * @return
	 */
	public NearQuery distanceMultiplier(double distanceMultiplier) {

		this.metric = new CustomMetric(distanceMultiplier);
		return this;
	}

	/**
	 * Configures whether to return spherical values for the actual distance.
	 * 
	 * @param spherical
	 * @return
	 */
	public NearQuery spherical(boolean spherical) {
		this.spherical = spherical;
		return this;
	}

	/**
	 * Returns whether spharical values will be returned.
	 * 
	 * @return
	 */
	public boolean isSpherical() {
		return this.spherical;
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
	 * Will cause the results' distances being returned in the given metric. Sets {@link #distanceMultiplier(double)}
	 * accordingly as well as {@link #spherical(boolean)} if the given {@link Metric} is not {@link Metrics#NEUTRAL}.
	 * 
	 * @param metric the metric the results shall be returned in. Uses {@link Metrics#NEUTRAL} if {@literal null} is
	 *          passed.
	 * @return
	 */
	public NearQuery in(Metric metric) {
		return adaptMetric(metric == null ? Metrics.NEUTRAL : metric);
	}

	/**
	 * Configures the given {@link Metric} to be used as base on for this query and recalculate the maximum distance if no
	 * metric was set before.
	 * 
	 * @param metric
	 */
	private NearQuery adaptMetric(Metric metric) {

		if (metric != Metrics.NEUTRAL) {
			spherical(true);
		}

		this.metric = metric;
		return this;
	}

	/**
	 * Adds an actual query to the {@link NearQuery} to restrict the objects considered for the actual near operation.
	 * 
	 * @param query must not be {@literal null}.
	 * @return
	 */
	public NearQuery query(Query query) {

		Assert.notNull(query, "Cannot apply 'null' query on NearQuery.");
		this.query = query;
		this.skip = query.getSkip();

		if (query.getLimit() != 0) {
			this.num = (long) query.getLimit();
		}
		return this;
	}

	/**
	 * @return the number of elements to skip.
	 */
	public Long getSkip() {
		return skip;
	}

	/**
	 * Returns the {@link Document} built by the {@link NearQuery}.
	 * 
	 * @return
	 */
	public Document toDocument() {

		Document document = new Document();

		if (query != null) {
			document.put("query", query.getQueryObject());
		}

		if (maxDistance != null) {
			document.put("maxDistance", maxDistance.getNormalizedValue());
		}

		if (minDistance != null) {
			document.put("minDistance", minDistance.getNormalizedValue());
		}

		if (metric != null) {
			document.put("distanceMultiplier", metric.getMultiplier());
		}

		if (num != null) {
			document.put("num", num);
		}

		document.put("near", Arrays.asList(point.getX(), point.getY()));

		document.put("spherical", spherical);

		return document;
	}
}
