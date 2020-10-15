/*
 * Copyright 2020. the original author or authors.
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

/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.util;

import java.lang.annotation.Annotation;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class AddressTypeInformation extends DomainTypeInformation<Address> {

	private static final AddressTypeInformation INSTANCE = new AddressTypeInformation();

	private AddressTypeInformation() {
		super(Address.class);
	}

	public static AddressTypeInformation instance() {
		return INSTANCE;
	}

	@Override
	protected void computeFields() {

		addField(Field.<Address> string("city").getter(Address::getCity));
		addField(Field.<Address> string("street").getter(Address::getStreet));
	}

	@Override
	protected EntityInstantiator computeEntityInstantiator() {
		return new EntityInstantiator() {

			@Override
			public <T, E extends PersistentEntity<? extends T, P>, P extends PersistentProperty<P>> T createInstance(E entity,
					ParameterValueProvider<P> provider) {

				String city = (String) provider
						.getParameterValue(new Parameter("city", new StringTypeInformation(), new Annotation[] {}, entity));
				String street = (String) provider
						.getParameterValue(new Parameter("street", new StringTypeInformation(), new Annotation[] {}, entity));

				T address = (T) new Address(city, street);
				System.out.println("Created new Address instance via constructor using values (" + city + ", " + street
						+ ") resulting in " + address);
				return address;
			}
		};
	}

	@Override
	protected PreferredConstructor computePreferredConstructor() {
		return StaticPreferredConstructor.of("city", "street");
	}
}
