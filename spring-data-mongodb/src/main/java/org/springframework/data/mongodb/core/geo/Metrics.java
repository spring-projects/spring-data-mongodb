package org.springframework.data.mongodb.core.geo;

import org.springframework.data.mongodb.core.query.NearQuery;

/**
 * Commonly used {@link Metrics} for {@link NearQuery}s.
 * 
 * @author Oliver Gierke
 */
public enum Metrics implements Metric {

	KILOMETERS(6378.137), MILES(3963.191), NEUTRAL(1);

	private final double multiplier;

	private Metrics(double multiplier) {
		this.multiplier = multiplier;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.Metric#getMultiplier()
	 */
	public double getMultiplier() {
		return multiplier;
	}
}