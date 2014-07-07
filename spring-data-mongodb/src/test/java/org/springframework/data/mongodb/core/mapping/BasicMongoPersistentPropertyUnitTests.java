/*
 * Copyright 2011-2014 by the original author(s).
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.Locale;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Unit test for {@link BasicMongoPersistentProperty}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class BasicMongoPersistentPropertyUnitTests {

	MongoPersistentEntity<Person> entity;

	@Rule public ExpectedException exception = ExpectedException.none();

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

	/**
	 * @see DATAMONGO-553
	 */
	@Test
	public void usesPropertyAccessForThrowableCause() {

		MongoPersistentProperty property = getPropertyFor(ReflectionUtils.findField(Throwable.class, "cause"));
		assertThat(property.usePropertyAccess(), is(true));
	}

	/**
	 * @see DATAMONGO-607
	 */
	@Test
	public void usesCustomFieldNamingStrategyByDefault() throws Exception {

		Field field = ReflectionUtils.findField(Person.class, "lastname");

		MongoPersistentProperty property = new BasicMongoPersistentProperty(field, null, entity, new SimpleTypeHolder(),
				UppercaseFieldNamingStrategy.INSTANCE);
		assertThat(property.getFieldName(), is("LASTNAME"));

		field = ReflectionUtils.findField(Person.class, "firstname");

		property = new BasicMongoPersistentProperty(field, null, entity, new SimpleTypeHolder(),
				UppercaseFieldNamingStrategy.INSTANCE);
		assertThat(property.getFieldName(), is("foo"));
	}

	/**
	 * @see DATAMONGO-607
	 */
	@Test
	public void rejectsInvalidValueReturnedByFieldNamingStrategy() {

		Field field = ReflectionUtils.findField(Person.class, "lastname");
		MongoPersistentProperty property = new BasicMongoPersistentProperty(field, null, entity, new SimpleTypeHolder(),
				InvalidFieldNamingStrategy.INSTANCE);

		exception.expect(MappingException.class);
		exception.expectMessage(InvalidFieldNamingStrategy.class.getName());
		exception.expectMessage(property.toString());

		property.getFieldName();
	}

	/**
	 * @see DATAMONGO-937
	 */
	@Test
	public void shouldDetectAnnotatedLanguagePropertyCorrectly() {

		BasicMongoPersistentEntity<DocumentWithLanguageProperty> persistentEntity = new BasicMongoPersistentEntity<DocumentWithLanguageProperty>(
				ClassTypeInformation.from(DocumentWithLanguageProperty.class));

		MongoPersistentProperty property = getPropertyFor(persistentEntity, "lang");
		assertThat(property.isLanguageProperty(), is(true));
	}

	/**
	 * @see DATAMONGO-937
	 */
	@Test
	public void shouldDetectIplicitLanguagePropertyCorrectly() {

		BasicMongoPersistentEntity<DocumentWithImplicitLanguageProperty> persistentEntity = new BasicMongoPersistentEntity<DocumentWithImplicitLanguageProperty>(
				ClassTypeInformation.from(DocumentWithImplicitLanguageProperty.class));

		MongoPersistentProperty property = getPropertyFor(persistentEntity, "language");
		assertThat(property.isLanguageProperty(), is(true));
	}

	/**
	 * @see DATAMONGO-976
	 */
	@Test
	public void shouldDetectTextScorePropertyCorrectly() {

		BasicMongoPersistentEntity<DocumentWithTextScoreProperty> persistentEntity = new BasicMongoPersistentEntity<DocumentWithTextScoreProperty>(
				ClassTypeInformation.from(DocumentWithTextScoreProperty.class));

		MongoPersistentProperty property = getPropertyFor(persistentEntity, "score");
		assertThat(property.isTextScoreProperty(), is(true));
	}

	/**
	 * @see DATAMONGO-976
	 */
	@Test
	public void shouldDetectTextScoreAsCalculatedProperty() {

		BasicMongoPersistentEntity<DocumentWithTextScoreProperty> persistentEntity = new BasicMongoPersistentEntity<DocumentWithTextScoreProperty>(
				ClassTypeInformation.from(DocumentWithTextScoreProperty.class));

		MongoPersistentProperty property = getPropertyFor(persistentEntity, "score");
		assertThat(property.isCalculatedProperty(), is(true));
	}

	private MongoPersistentProperty getPropertyFor(Field field) {
		return getPropertyFor(entity, field);
	}

	private MongoPersistentProperty getPropertyFor(MongoPersistentEntity<?> persistentEntity, String fieldname) {
		return getPropertyFor(persistentEntity, ReflectionUtils.findField(persistentEntity.getType(), fieldname));
	}

	private MongoPersistentProperty getPropertyFor(MongoPersistentEntity<?> persistentEntity, Field field) {
		return new BasicMongoPersistentProperty(field, null, persistentEntity, new SimpleTypeHolder(),
				PropertyNameFieldNamingStrategy.INSTANCE);
	}

	class Person {

		@Id String id;

		@org.springframework.data.mongodb.core.mapping.Field("foo") String firstname;
		String lastname;

		@org.springframework.data.mongodb.core.mapping.Field(order = -20) String ssn;
	}

	enum UppercaseFieldNamingStrategy implements FieldNamingStrategy {

		INSTANCE;

		public String getFieldName(PersistentProperty<?> property) {
			return property.getName().toUpperCase(Locale.US);
		}
	}

	enum InvalidFieldNamingStrategy implements FieldNamingStrategy {

		INSTANCE;

		public String getFieldName(PersistentProperty<?> property) {
			return null;
		}
	}

	static class DocumentWithLanguageProperty {

		@Language String lang;
	}

	static class DocumentWithImplicitLanguageProperty {

		String language;
	}

	static class DocumentWithTextScoreProperty {
		@TextScore Float score;
	}
}
