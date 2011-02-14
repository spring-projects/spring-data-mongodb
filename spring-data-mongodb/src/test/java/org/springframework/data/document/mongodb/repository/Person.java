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

import org.bson.types.ObjectId;

import com.mysema.query.annotations.QueryEntity;


/**
 * Sample domain class.
 *
 * @author Oliver Gierke
 */
@QueryEntity
public class Person {

  private String id;
  private String firstname;
  private String lastname;
  private Integer age;

  private Address address;
  private Set<Address> shippingAddresses;


  public Person() {

    this(null, null);
  }


  public Person(String firstname, String lastname) {

    this(firstname, lastname, null);
  }


  public Person(String firstname, String lastname, Integer age) {

    this.id = ObjectId.get().toString();
    this.firstname = firstname;
    this.lastname = lastname;
    this.age = age;
  }


  /**
   * @param id the id to set
   */
  public void setId(String id) {

    this.id = id;
  }


  /**
   * @return the id
   */
  public String getId() {

    return id;
  }


  /**
   * @return the firstname
   */
  public String getFirstname() {

    return firstname;
  }


  /**
   * @param firstname the firstname to set
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
   * @param lastname the lastname to set
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
   * @param age the age to set
   */
  public void setAge(Integer age) {

    this.age = age;
  }


  /**
   * @return the address
   */
  public Address getAddress() {
    return address;
  }


  /**
   * @param address the address to set
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
   * @param addresses the addresses to set
   */
  public void setShippingAddresses(Set<Address> addresses) {
    this.shippingAddresses = addresses;
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

    return this.id.equals(that.id);
  }


  /*
  * (non-Javadoc)
  *
  * @see java.lang.Object#hashCode()
  */
  @Override
  public int hashCode() {

    return id.hashCode();
  }

  /* (non-Javadoc)
  * @see java.lang.Object#toString()
  */
  @Override
  public String toString() {
    return String.format("%s %s", firstname, lastname);
  }
}
