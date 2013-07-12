package org.springframework.data.mongodb.core.aggregation;

import java.util.Arrays;

import org.springframework.data.mongodb.core.mapping.Field;

/**
 * Data model from mongodb reference data set
 * 
 * @see http://docs.mongodb.org/manual/tutorial/aggregation-examples/
 * @see http://media.mongodb.org/zips.json
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
