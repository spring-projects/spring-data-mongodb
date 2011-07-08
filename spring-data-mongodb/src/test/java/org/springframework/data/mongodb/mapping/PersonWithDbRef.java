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

package org.springframework.data.mongodb.mapping;

import org.springframework.data.mongodb.mapping.DBRef;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class PersonWithDbRef extends BasePerson {

	@DBRef
	private GeoLocation home;

	public PersonWithDbRef(Integer ssn, String firstName, String lastName, GeoLocation home) {
		super(ssn, firstName, lastName);
		this.home = home;
	}

	public GeoLocation getHome() {
		return home;
	}

	public void setHome(GeoLocation home) {
		this.home = home;
	}
}
