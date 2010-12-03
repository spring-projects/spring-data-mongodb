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

import org.bson.types.ObjectId;


/**
 * Sample domain class.
 * 
 * @author Oliver Gierke
 */
public class Person {

    private String id;
    private String firstname;
    private String lastname;


    public Person() {

        this(null, null);
    }


    public Person(String firstname, String lastname) {

        this.id = ObjectId.get().toString();
        this.firstname = firstname;
        this.lastname = lastname;
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
}
