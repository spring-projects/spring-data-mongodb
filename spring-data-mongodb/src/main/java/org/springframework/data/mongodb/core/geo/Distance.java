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
package org.springframework.data.mongodb.core.geo;

import org.springframework.util.ObjectUtils;

/**
 * Value object to represent distances in a given metric.
 * 
 * @author Oliver Gierke
 */
public class Distance {

	private final double value;
	private final Metric metric;

	/**
	 * Creates a new {@link Distance}.
	 * 
	 * @param value
	 */
	public Distance(double value) {
		this(value, Metrics.NEUTRAL);
	}

	/**
	 * Creates a new {@link Distance} with the given {@link Metric}.
	 * 
	 * @param value
	 * @param metric
	 */
	public Distance(double value, Metric metric) {
		this.value = value;
		this.metric = metric == null ? Metrics.NEUTRAL : metric;
	}

	/**
	 * @return the value
	 */
	public double getValue() {
		return value;
	}

	/**
	 * Returns the normalized value regarding the underlying {@link Metric}.
	 * 
	 * @return
	 */
	public double getNormalizedValue() {
		return value / metric.getMultiplier();
	}

	/**
	 * @return the metric
	 */
	public Metric getMetric() {
		return metric;
	}

	/**
	 * Adds the given distance to the current one. The resulting {@link Distance} will be in the same metric as the
	 * current one.
	 * 
	 * @param other
	 * @return
	 */
	public Distance add(Distance other) {
		double newNormalizedValue = getNormalizedValue() + other.getNormalizedValue();
		return new Distance(newNormalizedValue * metric.getMultiplier(), metric);
	}

	/**
	 * Adds the given {@link Distance} to the current one and forces the result to be in a given {@link Metric}.
	 * 
	 * @param other
	 * @param metric
	 * @return
	 */
	public Distance add(Distance other, Metric metric) {
		double newLeft = getNormalizedValue() * metric.getMultiplier();
		double newRight = other.getNormalizedValue() * metric.getMultiplier();
		return new Distance(newLeft + newRight, metric);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		Distance that = (Distance) obj;

		return this.value == that.value && ObjectUtils.nullSafeEquals(this.metric, that.metric);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = 17;
		result += 31 * Double.doubleToLongBits(value);
		result += 31 * ObjectUtils.nullSafeHashCode(metric);
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		StringBuilder builder = new StringBuilder();
		builder.append(value);

		if (metric != Metrics.NEUTRAL) {
			builder.append(" ").append(metric.toString());
		}

		return builder.toString();
	}
}
