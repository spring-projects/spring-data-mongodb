package org.springframework.data.mongodb.core.geo;

/**
 * Interface for {@link Metric}s that can be applied to a base scale.
 * 
 * @author Oliver Gierke
 */
public interface Metric {

	/**
	 * Returns the multiplier to calculate metrics values from a base scale.
	 * 
	 * @return
	 */
	double getMultiplier();
}