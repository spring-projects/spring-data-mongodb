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

import java.lang.annotation.Annotation;
import java.util.List;

import org.springframework.data.mapping.model.DomainTypeConstructor;
import org.springframework.data.mapping.model.DomainTypeInformation;
import org.springframework.data.mapping.model.Field;
import org.springframework.data.mapping.model.ListTypeInformation;
import org.springframework.data.mapping.model.StringTypeInformation;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.FieldType;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class PersonTypeInformation extends DomainTypeInformation<Person> {

	private static final PersonTypeInformation INSTANCE = new PersonTypeInformation();

	private PersonTypeInformation() {

		super(Person.class);

		// CONSTRUCTOR
		setConstructor(computePreferredConstructor());

		// ANNOTATIONS
		addAnnotation(computeAtDocumentAnnotation());

		// FIELDS
		addField(
				Field.<Person> int64("id").annotatedWithAtId().getter(Person::getId).wither((bean, id) -> bean.withId(id)));
		addField(Field.<Person> string("firstname").getter(Person::getFirstname).annotation(atFieldOnFirstname()));
		addField(Field.<Person> string("lastname").getter(Person::getLastname));
		addField(Field.<Person> int32("age").getter(Person::getAge).setter(Person::setAge));
		addField(Field.<Person, Address> type("address", AddressTypeInformation.instance()).getter(Person::getAddress)
				.setter(Person::setAddress));
		addField(Field.<Person, List<String>> type("nicknames", new ListTypeInformation<>(StringTypeInformation.instance()))
				.getter(Person::getNicknames).setter(Person::setNicknames));

	}

	public static PersonTypeInformation instance() {
		return INSTANCE;
	}

	private DomainTypeConstructor<Person> computePreferredConstructor() {
		return DomainTypeConstructor.<Person> builder().args("firstname", "lastname")
				.newInstanceFunction((args) -> new Person((String) args[0], (String) args[1]));
	}

	private Document computeAtDocumentAnnotation() {
		return new Document() {
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
		};
	}

	private Annotation atFieldOnFirstname() {

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
