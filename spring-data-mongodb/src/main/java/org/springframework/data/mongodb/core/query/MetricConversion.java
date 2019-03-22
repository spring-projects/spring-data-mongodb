/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.mongodb.core.query;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;

/**
 * {@link Metric} and {@link Distance} conversions using the metric system.
 *
 * @author Mark Paluch
 * @since 2.2
 */
class MetricConversion {

	private static final BigDecimal METERS_MULTIPLIER = new BigDecimal(Metrics.KILOMETERS.getMultiplier())
			.multiply(new BigDecimal(1000));

	// to achieve a calculation that is accurate to 0.3 meters
	private static final int PRECISION = 8;

	/**
	 * Return meters to {@code metric} multiplier.
	 *
	 * @param metric
	 * @return
	 */
	protected static double getMetersToMetricMultiplier(Metric metric) {

		ConversionMultiplier conversionMultiplier = ConversionMultiplier.builder().from(METERS_MULTIPLIER).to(metric)
				.build();
		return conversionMultiplier.multiplier().doubleValue();
	}

	/**
	 * Return {@code distance} in meters.
	 *
	 * @param distance
	 * @return
	 */
	protected static double getDistanceInMeters(Distance distance) {
		return new BigDecimal(distance.getValue()).multiply(getMetricToMetersMultiplier(distance.getMetric()))
				.doubleValue();
	}

	/**
	 * Return {@code metric} to meters multiplier.
	 *
	 * @param metric
	 * @return
	 */
	private static BigDecimal getMetricToMetersMultiplier(Metric metric) {

		ConversionMultiplier conversionMultiplier = ConversionMultiplier.builder().from(metric).to(METERS_MULTIPLIER)
				.build();
		return conversionMultiplier.multiplier();
	}

	/**
	 * Provides a multiplier to convert between various metrics. Metrics must share the same base scale and provide a
	 * multiplier to convert between the base scale and its own metric.
	 *
	 * @author Mark Paluch
	 */
	private static class ConversionMultiplier {

		private final BigDecimal source;
		private final BigDecimal target;

		ConversionMultiplier(Number source, Number target) {

			if (source instanceof BigDecimal) {
				this.source = (BigDecimal) source;
			} else {
				this.source = new BigDecimal(source.doubleValue());
			}

			if (target instanceof BigDecimal) {
				this.target = (BigDecimal) target;
			} else {
				this.target = new BigDecimal(target.doubleValue());
			}
		}

		/**
		 * Returns the multiplier to convert a number from the {@code source} metric to the {@code target} metric.
		 *
		 * @return
		 */
		BigDecimal multiplier() {
			return target.divide(source, PRECISION, RoundingMode.HALF_UP);
		}

		/**
		 * Creates a new {@link ConversionMultiplierBuilder}.
		 *
		 * @return
		 */
		static ConversionMultiplierBuilder builder() {
			return new ConversionMultiplierBuilder();
		}

	}

	/**
	 * Builder for {@link ConversionMultiplier}.
	 *
	 * @author Mark Paluch
	 */
	private static class ConversionMultiplierBuilder {

		private Number from;
		private Number to;

		ConversionMultiplierBuilder() {}

		ConversionMultiplierBuilder from(Number from) {
			this.from = from;
			return this;
		}

		ConversionMultiplierBuilder from(Metric from) {
			this.from = from.getMultiplier();
			return this;
		}

		ConversionMultiplierBuilder to(Number to) {
			this.to = to;
			return this;
		}

		ConversionMultiplierBuilder to(Metric to) {
			this.to = to.getMultiplier();
			return this;
		}

		ConversionMultiplier build() {
			return new ConversionMultiplier(this.from, this.to);
		}
	}
}
