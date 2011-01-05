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
package org.springframework.data.document.mongodb;

import org.bson.types.ObjectId;

public class Person {

	private final ObjectId id;
	
	private String firstName;
	
	private Person friend;
	
	public Person() {
		this.id = new ObjectId();
	}
	
	public Person(ObjectId id, String firstname) {
		this.id = id;
		this.firstName = firstname;
	}
	
	public Person(String firstname) {
		this();
		this.firstName = firstname;
	}

	public ObjectId getId() {
		return id;
	}
	
	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public Person getFriend() {
		return friend;
	}

	public void setFriend(Person friend) {
		this.friend = friend;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (!(getClass().equals(obj.getClass()))) {
			return false;
		}

		Person that = (Person) obj;

		return this.id == null ? false : this.id.equals(that.id);
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return id.hashCode();
	}
}
