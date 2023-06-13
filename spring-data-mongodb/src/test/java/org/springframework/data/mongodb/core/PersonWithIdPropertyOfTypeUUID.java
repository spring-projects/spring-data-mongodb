/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Objects;
import java.util.UUID;

public class PersonWithIdPropertyOfTypeUUID {

	private UUID id;
	private String firstName;
	private int age;

	public UUID getId() {
		return this.id;
	}

	public String getFirstName() {
		return this.firstName;
	}

	public int getAge() {
		return this.age;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PersonWithIdPropertyOfTypeUUID that = (PersonWithIdPropertyOfTypeUUID) o;
		return age == that.age && Objects.equals(id, that.id) && Objects.equals(firstName, that.firstName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, firstName, age);
	}

	public String toString() {
		return "PersonWithIdPropertyOfTypeUUID(id=" + this.getId() + ", firstName=" + this.getFirstName() + ", age="
				+ this.getAge() + ")";
	}
}
