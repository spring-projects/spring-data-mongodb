/*
 * Copyright 2002-2010 the original author or authors.
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

import java.util.Collection;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;


/**
 * Sample repository managing {@link Person} entities.
 *
 * @author Oliver Gierke
 */
public interface PersonRepository extends MongoRepository<Person, String>, QueryDslPredicateExecutor<Person> {

  /**
   * Returns all {@link Person}s with the given lastname.
   *
   * @param lastname
   * @return
   */
  List<Person> findByLastname(String lastname);

  /**
   * Returns the {@link Person}s with the given firstname. Uses {@link Query} annotation to define the query to be
   * executed.
   *
   * @param firstname
   * @return
   */
  @Query(value="{ 'firstname' : ?0 }", fields="{ 'firstname': 1, 'lastname': 1}")
  List<Person> findByThePersonsFirstname(String firstname);


  /**
   * Returns all {@link Person}s with a firstname matching the given one
   * (*-wildcard supported).
   *
   * @param firstname
   * @return
   */
  List<Person> findByFirstnameLike(String firstname);


  /**
   * Returns a page of {@link Person}s with a lastname mathing the given one
   * (*-wildcards supported).
   *
   * @param lastname
   * @param pageable
   * @return
   */
  Page<Person> findByLastnameLike(String lastname, Pageable pageable);

  /**
   * Returns all {@link Person}s with a firstname contained in the given varargs.
   *
   * @param firstnames
   * @return
   */
  List<Person> findByFirstnameIn(String... firstnames);

  /**
   * Returns all {@link Person}s with a firstname not contained in the given collection.
   *
   * @param firstnames
   * @return
   */
  List<Person> findByFirstnameNotIn(Collection<String> firstnames);

  /**
   * Returns all {@link Person}s with an age between the two given values.
   *
   * @param from
   * @param to
   * @return
   */
  List<Person> findByAgeBetween(int from, int to);


  /**
   * Returns the {@link Person} with the given {@link Address} as shipping address.
   *
   * @param address
   * @return
   */
  Person findByShippingAddresses(Address address);


  /**
   * Returns all {@link Person}s with the given {@link Address}.
   *
   * @param address
   * @return
   */
  List<Person> findByAddress(Address address);


  List<Person> findByAddressZipCode(String zipCode);

  List<Person> findByLastnameLikeAndAgeBetween(String lastname, int from, int to);

  List<Person> findByAgeOrLastnameLikeAndFirstnameLike(int age, String lastname, String firstname);
}
