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
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.FieldType;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class PersonTypeInformation extends StaticTypeInformation<Person> {

	private static final PersonTypeInformation INSTANCE = new PersonTypeInformation();

	private PersonTypeInformation() {
		super(Person.class);
	}

	public static PersonTypeInformation instance() {
		return INSTANCE;
	}

	@Override
	protected void computeFields() {

		addField(
				Field.<Person> int64("id").getter(Person::getId).wither((bean, id) -> bean.withId(id)).annotation(new Id() {
					@Override
					public Class<? extends Annotation> annotationType() {
						return Id.class;
					}
				}));
		addField(Field.<Person> string("firstname").getter(Person::getFirstname).annotation(atFieldOnFirstname()));
		addField(Field.<Person> string("lastname").getter(Person::getLastname));
		addField(Field.<Person> int32("age").getter(Person::getAge).setter(Person::setAge));
		addField(Field.<Person, Address> type("address", AddressTypeInformation.instance()).getter(Person::getAddress)
				.setter(Person::setAddress));
		addField(Field.<Person, List<String>> type("nicknames", new ListTypeInformation<>(StringTypeInformation.instance()))
				.getter(Person::getNicknames).setter(Person::setNicknames));
	}

	@Override
	protected void computeAnnotations() {
		addAnnotation(new Document() {
			@Override
			public Class<? extends Annotation> annotationType() {
				return Document.class;
			}

			@Override
			public String value() {
				return collection();
			}

			@Override
			public String collection() {
				return "star-wars";
			}

			@Override
			public String language() {
				return "";
			}

			@Override
			public String collation() {
				return "";
			}
		});
	}

	@Override
	protected EntityInstantiator computeEntityInstantiator() {

		return new EntityInstantiator() {

			@Override
			public <T, E extends PersistentEntity<? extends T, P>, P extends PersistentProperty<P>> T createInstance(E entity,
					ParameterValueProvider<P> provider) {

				String firstname = (String) provider.getParameterValue(
						new Parameter("firstname", StringTypeInformation.instance(), new Annotation[] {}, entity));
				String lastname = (String) provider.getParameterValue(
						new Parameter("lastname", StringTypeInformation.instance(), new Annotation[] {}, entity));

				T person = (T) new Person(firstname, lastname);
				System.out.println("Created new Person instance via constructor using values (" + firstname + ", " + lastname
						+ ") resulting in " + person);
				return person;
			}
		};
	}

	@Override
	protected PreferredConstructor computePreferredConstructor() {
		return StaticPreferredConstructor.of("firstname", "lastname");
	}

	Annotation atFieldOnFirstname() {

		return new org.springframework.data.mongodb.core.mapping.Field() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return org.springframework.data.mongodb.core.mapping.Field.class;
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
		};
	}
}
