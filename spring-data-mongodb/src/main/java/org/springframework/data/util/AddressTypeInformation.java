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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

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
public class AddressTypeInformation extends StaticTypeInformation<Address> {

	public AddressTypeInformation() {
		super(Address.class);
	}

	@Override
	protected Map<String, TypeInformation<?>> computePropertiesMap() {

		Map<String, TypeInformation<?>> properties = new LinkedHashMap<>();
		properties.put("city", new StringTypeInformation());
		properties.put("street", new StringTypeInformation());
		return properties;
	}

	@Override
	protected Map<String, Function<Address, Object>> computeGetter() {
		Map<String, Function<Address, Object>> getters = new LinkedHashMap<>();
		getters.put("city", Address::getCity);
		getters.put("street", Address::getStreet);

		return getters;
	}

	@Override
	protected Map<String, BiFunction<Address, Object, Address>> computeSetter() {
		Map<String, BiFunction<Address, Object, Address>> setter = new LinkedHashMap<>();
//		setter.put("city", (bean, id) -> {
//			bean.setCity((String) id);
//			return bean;
//		});
//		setter.put("street", (bean, id) -> {
//			bean.setStreet((String) id);
//			return bean;
//		});
		return setter;
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

				return (T) new Address(city, street);
			}
		};
	}

	@Override
	protected PreferredConstructor computePreferredConstructor() {
		return StaticPreferredConstructor.of("city", "street");
	}
}
