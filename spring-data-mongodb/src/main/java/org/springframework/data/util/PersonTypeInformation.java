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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class PersonTypeInformation extends StaticTypeInformation<Person> {

	public PersonTypeInformation() {
		super(Person.class);
	}

	@Override
	protected Map<String, TypeInformation<?>> computePropertiesMap() {

		LinkedHashMap<String, TypeInformation<?>> properties = new LinkedHashMap<>();
		properties.put("firstname", new StringTypeInformation());
		properties.put("lastname", new StringTypeInformation());
		properties.put("id", new StaticTypeInformation<>(Long.class));
		properties.put("age", new StaticTypeInformation<>(int.class));
		properties.put("address", new AddressTypeInformation());
		properties.put("nicknames", new ListTypeInformation(new StringTypeInformation()));

		return properties;
	}

	@Override
	protected Map<String, BiFunction<Person, Object, Person>> computeSetter() {

		Map<String, BiFunction<Person, Object, Person>> setter = new LinkedHashMap<>();
		setter.put("id", (bean, id) -> {
			bean.setId((Long) id);
			return bean;
		});
		setter.put("age", (bean, id) -> {
			bean.setAge((int) id);
			return bean;
		});
		// setter.put("firstname", (bean, id) -> {bean.setFirstname((String)id); return bean;});
		// setter.put("lastname", (bean, id) -> {bean.setLastname((String)id); return bean;});
		setter.put("address", (bean, id) -> {
			bean.setAddress((Address) id);
			return bean;
		});
		setter.put("nicknames", (bean, id) -> {
			bean.setNicknames((List<String>) id);
			return bean;
		});

		return setter;
	}

	@Override
	protected Map<String, Function<Person, Object>> computeGetter() {

		Map<String, Function<Person, Object>> getter = new LinkedHashMap<>();

		getter.put("firstname", Person::getFirstname);
		getter.put("lastname", Person::getLastname);
		getter.put("id", Person::getId);
		getter.put("address", Person::getAddress);
		getter.put("nicknames", Person::getNicknames);
		getter.put("age", Person::getAge);

		return getter;
	}

	@Override
	protected EntityInstantiator computeEntityInstantiator() {

		return new EntityInstantiator() {

			@Override
			public <T, E extends PersistentEntity<? extends T, P>, P extends PersistentProperty<P>> T createInstance(E entity,
					ParameterValueProvider<P> provider) {

				String firstname = (String) provider
						.getParameterValue(new Parameter("firstname", new StringTypeInformation(), new Annotation[] {}, entity));
				String lastname = (String) provider
						.getParameterValue(new Parameter("lastname", new StringTypeInformation(), new Annotation[] {}, entity));

				return (T) new Person(firstname, lastname);
			}
		};
	}

	@Override
	protected PreferredConstructor computePreferredConstructor() {
		return StaticPreferredConstructor.of("firstname", "lastname");
	}

	@Override
	protected Map<String, List<Annotation>> computePropertyAnnotations() {

		Map<String, List<Annotation>> annotationMap = new LinkedHashMap<>();
		annotationMap.put("firstname", Collections.singletonList(new Field() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return Field.class;
			}

			@Override
			public String value() {
				return "first-name";
			}

			@Override
			public String name() {
				return value();
			}

			@Override
			public int order() {
				return 0;
			}

			@Override
			public FieldType targetType() {
				return FieldType.IMPLICIT;
			}
		}));

		return annotationMap;
	}
}
