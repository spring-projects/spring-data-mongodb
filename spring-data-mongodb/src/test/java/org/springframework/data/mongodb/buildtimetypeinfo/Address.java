/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.mongodb.buildtimetypeinfo;

import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class Address {

	String city;
	String street;

	public Address(String city, String street) {
		this.city = city;
		this.street = street;
	}

	public String getCity() {
		return city;
	}

	public String getStreet() {
		return street;
	}

	@Override
	public String toString() {
		return "Address{" + "city='" + city + '\'' + ", street='" + street + '\'' + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Address address = (Address) o;

		if (!ObjectUtils.nullSafeEquals(city, address.city)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(street, address.street);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(city);
		result = 31 * result + ObjectUtils.nullSafeHashCode(street);
		return result;
	}
}
