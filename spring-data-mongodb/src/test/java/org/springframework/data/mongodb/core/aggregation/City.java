package org.springframework.data.mongodb.core.aggregation;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@lombok.Data
@AllArgsConstructor
@NoArgsConstructor
class City {

	String name;
	int population;

	public String toString() {
		return "City [name=" + name + ", population=" + population + "]";
	}
}
