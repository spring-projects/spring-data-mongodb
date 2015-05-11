/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author Thomas Darimont
 *
 * @param <RC>
 */
@Document
public abstract class GenericCustomer<RC extends GenericCustomer<RC>> {

	@Id private Long id;

	@org.springframework.data.mongodb.core.mapping.DBRef private RC referrer;

	@Indexed private String firstName;

	public GenericCustomer(Long id, String firstName, RC referrer) {
		this.firstName = firstName;
		this.id = id;
		this.referrer = referrer;
	}

	public RC getReferrer() {
		return referrer;
	}

	@Override
	public String toString() {
		return "GenericCustomer{" + "id=" + id + ", referrer=" + referrer + ", firstName='" + firstName + '\'' + '}';
	}
}
