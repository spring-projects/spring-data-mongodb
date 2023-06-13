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

import java.util.Objects;

import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
@Document
public class VersionedPerson extends Contact {

	private String firstname;
	private @Nullable String lastname;

	private @Version Long version;

	public VersionedPerson() {}

	public VersionedPerson(String firstname) {
		this(firstname, null);
	}

	public VersionedPerson(String firstname, @Nullable String lastname) {

		this.firstname = firstname;
		this.lastname = lastname;
	}

	public String getFirstname() {
		return this.firstname;
	}

	@Nullable
	public String getLastname() {
		return this.lastname;
	}

	public Long getVersion() {
		return this.version;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public void setLastname(@Nullable String lastname) {
		this.lastname = lastname;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	@Override
	public boolean equals(Object o) {

		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		VersionedPerson that = (VersionedPerson) o;
		return Objects.equals(firstname, that.firstname) && Objects.equals(lastname, that.lastname)
				&& Objects.equals(version, that.version);
	}

	@Override
	public int hashCode() {
		return Objects.hash(firstname, lastname, version);
	}

	public String toString() {
		return "VersionedPerson(firstname=" + this.getFirstname() + ", lastname=" + this.getLastname() + ", version="
				+ this.getVersion() + ")";
	}
}
