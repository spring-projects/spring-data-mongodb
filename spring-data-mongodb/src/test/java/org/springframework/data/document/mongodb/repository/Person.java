/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.document.mongodb.repository;

import java.util.Set;

import org.springframework.data.document.mongodb.geo.Point;
import org.springframework.data.document.mongodb.index.GeoSpatialIndexed;
import org.springframework.data.document.mongodb.mapping.Document;

/**
 * Sample domain class.
 * 
 * @author Oliver Gierke
 */
@Document
public class Person extends Contact {

	public enum Sex {
		MALE, FEMALE;
	}
	
	private String firstname;
	private String lastname;
	private Integer age;
	private Sex sex;

	@GeoSpatialIndexed
	private Point location;

	private Address address;
	private Set<Address> shippingAddresses;

	public Person() {

		this(null, null);
	}

	public Person(String firstname, String lastname) {

		this(firstname, lastname, null);
	}

	public Person(String firstname, String lastname, Integer age) {

		this(firstname, lastname, age, Sex.MALE);
	}
	
	public Person(String firstname, String lastname, Integer age, Sex sex) {
		
		super();
		this.firstname = firstname;
		this.lastname = lastname;
		this.age = age;
		this.sex = sex;
	}

	/**
	 * @return the firstname
	 */
	public String getFirstname() {

		return firstname;
	}

	/**
	 * @param firstname
	 *          the firstname to set
	 */
	public void setFirstname(String firstname) {

		this.firstname = firstname;
	}

	/**
	 * @return the lastname
	 */
	public String getLastname() {

		return lastname;
	}

	/**
	 * @param lastname
	 *          the lastname to set
	 */
	public void setLastname(String lastname) {

		this.lastname = lastname;
	}

	/**
	 * @return the age
	 */
	public Integer getAge() {

		return age;
	}

	/**
	 * @param age
	 *          the age to set
	 */
	public void setAge(Integer age) {

		this.age = age;
	}

	/**
	 * @return the location
	 */
	public Point getLocation() {
		return location;
	}

	/**
	 * @param location
	 *          the location to set
	 */
	public void setLocation(Point location) {
		this.location = location;
	}

	/**
	 * @return the address
	 */
	public Address getAddress() {
		return address;
	}

	/**
	 * @param address
	 *          the address to set
	 */
	public void setAddress(Address address) {
		this.address = address;
	}

	/**
	 * @return the addresses
	 */
	public Set<Address> getShippingAddresses() {
		return shippingAddresses;
	}

	/**
	 * @param addresses
	 *          the addresses to set
	 */
	public void setShippingAddresses(Set<Address> addresses) {
		this.shippingAddresses = addresses;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.repository.Contact#getName()
	 */
	public String getName() {
		return String.format("%s %s", firstname, lastname);
	}

	/*
	* (non-Javadoc)
	*
	* @see java.lang.Object#equals(java.lang.Object)
	*/
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		Person that = (Person) obj;

		return this.getId().equals(that.getId());
	}

	/*
	* (non-Javadoc)
	*
	* @see java.lang.Object#hashCode()
	*/
	@Override
	public int hashCode() {

		return getId().hashCode();
	}

	/* (non-Javadoc)
	* @see java.lang.Object#toString()
	*/
	@Override
	public String toString() {
		return String.format("%s %s", firstname, lastname);
	}
}
