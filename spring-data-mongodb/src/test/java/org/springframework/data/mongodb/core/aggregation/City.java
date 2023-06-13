/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.Objects;

/**
 * @author Thomas Darimont
 * @author Mark Paluch
 */
class City {

	String name;
	int population;

	public City() {}

	public City(String name, int population) {

		this.name = name;
		this.population = population;
	}

	public String toString() {
		return "City [name=" + name + ", population=" + population + "]";
	}

	public String getName() {
		return this.name;
	}

	public int getPopulation() {
		return this.population;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPopulation(int population) {
		this.population = population;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		City city = (City) o;
		return population == city.population && Objects.equals(name, city.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, population);
	}
}
