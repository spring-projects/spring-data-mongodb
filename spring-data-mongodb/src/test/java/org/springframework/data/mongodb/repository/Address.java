/*
 * Copyright 2011-2015 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import lombok.Getter;
import lombok.Setter;

import org.springframework.data.mongodb.core.geo.GeoJson;

import com.querydsl.core.annotations.QueryEmbeddable;

/**
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@QueryEmbeddable
@Getter
@Setter
public class Address {

	private String street;
	private String zipCode;
	private String city;

	private GeoJson location;

	protected Address() {

	}

	/**
	 * @param string
	 * @param string2
	 * @param string3
	 */
	public Address(String street, String zipcode, String city) {
		this.street = street;
		this.zipCode = zipcode;
		this.city = city;
	}
}
