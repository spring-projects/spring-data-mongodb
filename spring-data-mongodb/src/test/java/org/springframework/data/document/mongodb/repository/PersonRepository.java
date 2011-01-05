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

import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;


/**
 * Sample repository managing {@link Person} entities.
 * 
 * @author Oliver Gierke
 */
public interface PersonRepository extends MongoRepository<Person, String> {

    /**
     * Returns all {@link Person}s with the given lastname.
     * 
     * @param lastname
     * @return
     */
    List<Person> findByLastname(String lastname);


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
     * Returns all {@link Person}s with an age between the two given values.
     * 
     * @param from
     * @param to
     * @return
     */
    List<Person> findByAgeBetween(int from, int to);
}
