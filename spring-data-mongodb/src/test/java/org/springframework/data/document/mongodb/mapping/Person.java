/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.mapping;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Transient;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.index.CompoundIndex;
import org.springframework.data.document.mongodb.index.CompoundIndexes;
import org.springframework.data.document.mongodb.index.Indexed;

import java.util.List;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
@Document
@CompoundIndexes({
    @CompoundIndex(name = "age_idx", def = "{'lastName': 1, 'age': -1}")
})
public class Person<T extends Address> {

  @Id
  private String id;
  @Indexed(unique = true)
  private Integer ssn;
  private String firstName;
  @Indexed
  private String lastName;
  private Integer age;
  @Transient
  private Integer accountTotal;
  @DBRef
  private List<Account> accounts;
  private T address;
  @Autowired
  private MongoTemplate mongoTemplate;

  @PersistenceConstructor
  public Person(Integer ssn, String firstName, String lastName, Integer age, T address) {
    this.ssn = ssn;
    this.firstName = firstName;
    this.lastName = lastName;
    this.age = age;
    this.address = address;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getSsn() {
    return ssn;
  }

  public void setSsn(Integer ssn) {
    this.ssn = ssn;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public Integer getAge() {
    return age;
  }

  public void setAge(Integer age) {
    this.age = age;
  }

  public Integer getAccountTotal() {
    return accountTotal;
  }

  public void setAccountTotal(Integer accountTotal) {
    this.accountTotal = accountTotal;
  }

  public List<Account> getAccounts() {
    return accounts;
  }

  public void setAccounts(List<Account> accounts) {
    this.accounts = accounts;
  }

  public T getAddress() {
    return address;
  }

  public void setAddress(T address) {
    this.address = address;
  }
}
