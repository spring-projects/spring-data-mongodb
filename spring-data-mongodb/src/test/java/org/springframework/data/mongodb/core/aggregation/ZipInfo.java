package org.springframework.data.mongodb.core.aggregation;

import java.util.Arrays;

import org.springframework.data.mongodb.core.mapping.Field;

/**
 * Data model from mongodb reference data set
 * 
 * @see <a href="https://docs.mongodb.org/manual/tutorial/aggregation-examples/">Aggregation Examples</a>
 * @see <a href="http://media.mongodb.org/zips.json>zips.json</a>
 */
class ZipInfo {

	String id;
	String city;
	String state;
	@Field("pop") int population;
	@Field("loc") double[] location;

	public String toString() {
		return "ZipInfo [id=" + id + ", city=" + city + ", state=" + state + ", population=" + population + ", location="
				+ Arrays.toString(location) + "]";
	}
}
