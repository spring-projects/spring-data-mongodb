/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.mongodb.buildtimetypeinfo;

import org.springframework.data.mapping.model.DomainTypeConstructor;
import org.springframework.data.mapping.model.DomainTypeInformation;
import org.springframework.data.mapping.model.Field;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class AddressTypeInformation extends DomainTypeInformation<Address> {

	private static final AddressTypeInformation INSTANCE = new AddressTypeInformation();

	private AddressTypeInformation() {

		super(Address.class);

		// CONSTRUCTOR
		setConstructor(computePreferredConstructor());

		// FIELDS
		addField(Field.<Address> string("city").getter(Address::getCity));
		addField(Field.<Address> string("street").getter(Address::getStreet));
	}

	public static AddressTypeInformation instance() {
		return INSTANCE;
	}

	private DomainTypeConstructor<Address> computePreferredConstructor() {
		return DomainTypeConstructor.<Address> builder().args("city", "street")
				.newInstanceFunction(args -> new Address((String) args[0], (String) args[1]));
	}
}
