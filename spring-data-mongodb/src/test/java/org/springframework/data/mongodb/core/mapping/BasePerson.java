/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import com.querydsl.core.annotations.QuerySupertype;

/**
 * {@link QuerySupertype} is necessary for Querydsl 2.2.0-beta4 to compile the query classes directly. Can be removed as
 * soon as <a href="https://bugs.launchpad.net/querydsl/+bug/776219">https://bugs.launchpad.net/querydsl/+bug/776219</a>
 * is fixed.
 *
 * @see <a href="https://bugs.launchpad.net/querydsl/+bug/776219">https://bugs.launchpad.net/querydsl/+bug/776219</a>
 * @author Jon Brisbin
 * @author Oliver Gierke
 */
@QuerySupertype
public abstract class BasePerson {

	protected Integer ssn;
	protected String firstName;
	protected String lastName;

	public BasePerson() {}

	public BasePerson(Integer ssn, String firstName, String lastName) {
		this.ssn = ssn;
		this.firstName = firstName;
		this.lastName = lastName;
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
}
