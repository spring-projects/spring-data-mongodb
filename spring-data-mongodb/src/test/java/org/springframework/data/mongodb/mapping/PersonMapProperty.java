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

import java.util.Map;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.mapping.Document;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
@Document
public class PersonMapProperty extends BasePerson {

	private ObjectId id;
	private Map<String, AccountPojo> accounts;

	public PersonMapProperty(Integer ssn, String firstName, String lastName) {
		super(ssn, firstName, lastName);
	}

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public Map<String, AccountPojo> getAccounts() {
		return accounts;
	}

	public void setAccounts(Map<String, AccountPojo> accounts) {
		this.accounts = accounts;
	}

}
