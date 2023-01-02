/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

import com.querydsl.core.annotations.QueryEmbeddable;

/**
 * @author Oliver Gierke
 */
@QueryEmbeddable
public class Address {

	private String street;
	private String zipCode;
	private String city;

	protected Address() {

	}

	/**
	 * @param street
	 * @param zipcode
	 * @param city
	 */
	public Address(String street, String zipcode, String city) {
		this.street = street;
		this.zipCode = zipcode;
		this.city = city;
	}

	/**
	 * @return the street
	 */
	public String getStreet() {
		return street;
	}

	/**
	 * @param street the street to set
	 */
	public void setStreet(String street) {
		this.street = street;
	}

	/**
	 * @return the zipCode
	 */
	public String getZipCode() {
		return zipCode;
	}

	/**
	 * @param zipCode the zipCode to set
	 */
	public void setZipCode(String zipCode) {
		this.zipCode = zipCode;
	}

	/**
	 * @return the city
	 */
	public String getCity() {
		return city;
	}

	/**
	 * @param city the city to set
	 */
	public void setCity(String city) {
		this.city = city;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Address address = (Address) o;

		if (!ObjectUtils.nullSafeEquals(street, address.street)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(zipCode, address.zipCode)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(city, address.city);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(street);
		result = 31 * result + ObjectUtils.nullSafeHashCode(zipCode);
		result = 31 * result + ObjectUtils.nullSafeHashCode(city);
		return result;
	}
}
