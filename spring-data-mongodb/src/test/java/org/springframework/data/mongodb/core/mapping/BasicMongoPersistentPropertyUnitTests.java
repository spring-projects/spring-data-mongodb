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
package org.springframework.data.mongodb.core.mapping;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Unit test for {@link BasicMongoPersistentProperty}.
 * 
 * @author Oliver Gierke
 */
public class BasicMongoPersistentPropertyUnitTests {

	MongoPersistentEntity<Person> entity;

	@Before
	public void setup() {
		entity = new BasicMongoPersistentEntity<Person>(ClassTypeInformation.from(Person.class));
	}

	@Test
	public void usesAnnotatedFieldName() {

		Field field = ReflectionUtils.findField(Person.class, "firstname");
		assertThat(getPropertyFor(field).getFieldName(), is("foo"));
	}

	@Test
	public void returns_IdForIdProperty() {
		Field field = ReflectionUtils.findField(Person.class, "id");
		MongoPersistentProperty property = getPropertyFor(field);
		assertThat(property.isIdProperty(), is(true));
		assertThat(property.getFieldName(), is("_id"));
	}

	@Test
	public void returnsPropertyNameForUnannotatedProperties() {

		Field field = ReflectionUtils.findField(Person.class, "lastname");
		assertThat(getPropertyFor(field).getFieldName(), is("lastname"));
	}

	@Test
	public void preventsNegativeOrder() {
		getPropertyFor(ReflectionUtils.findField(Person.class, "ssn"));
	}

	private MongoPersistentProperty getPropertyFor(Field field) {
		return new BasicMongoPersistentProperty(field, null, entity, new SimpleTypeHolder());
	}

	class Person {

		@Id
		String id;

		@org.springframework.data.mongodb.core.mapping.Field("foo")
		String firstname;
		String lastname;

		@org.springframework.data.mongodb.core.mapping.Field(order = -20)
		String ssn;
	}
}
