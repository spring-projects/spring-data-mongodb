/*
 * Copyright 2019-2023 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
final class PersonAggregate {

	@Id private final String lastname;
	private final Set<String> names;

	public PersonAggregate(String lastname, String name) {
		this(lastname, Collections.singletonList(name));
	}

	@PersistenceConstructor
	public PersonAggregate(String lastname, Collection<String> names) {

		this.lastname = lastname;
		this.names = new HashSet<>(names);
	}

	public String getLastname() {
		return this.lastname;
	}

	public Set<String> getNames() {
		return this.names;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PersonAggregate that = (PersonAggregate) o;
		return Objects.equals(lastname, that.lastname) && Objects.equals(names, that.names);
	}

	@Override
	public int hashCode() {
		return Objects.hash(lastname, names);
	}

	public String toString() {
		return "PersonAggregate(lastname=" + this.getLastname() + ", names=" + this.getNames() + ")";
	}
}
