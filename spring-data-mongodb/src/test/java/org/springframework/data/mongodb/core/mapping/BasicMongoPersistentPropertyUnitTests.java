/*
 * Copyright 2011-2017 by the original author(s).
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Locale;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Unit test for {@link BasicMongoPersistentProperty}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
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

	@Test // DATAMONGO-553
	public void usesPropertyAccessForThrowableCause() {

		BasicMongoPersistentEntity<Throwable> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(Throwable.class));
		MongoPersistentProperty property = getPropertyFor(entity, "cause");

		assertThat(property.usePropertyAccess(), is(true));
	}

	@Test // DATAMONGO-607
	public void usesCustomFieldNamingStrategyByDefault() throws Exception {

		Field field = ReflectionUtils.findField(Person.class, "lastname");

		MongoPersistentProperty property = new BasicMongoPersistentProperty(Property.of(field), entity,
				new SimpleTypeHolder(), UppercaseFieldNamingStrategy.INSTANCE);
		assertThat(property.getFieldName(), is("LASTNAME"));

		field = ReflectionUtils.findField(Person.class, "firstname");

		property = new BasicMongoPersistentProperty(Property.of(field), entity, new SimpleTypeHolder(),
				UppercaseFieldNamingStrategy.INSTANCE);
		assertThat(property.getFieldName(), is("foo"));
	}

	@Test // DATAMONGO-607
	public void rejectsInvalidValueReturnedByFieldNamingStrategy() {

		Field field = ReflectionUtils.findField(Person.class, "lastname");
		MongoPersistentProperty property = new BasicMongoPersistentProperty(Property.of(field), entity,
				new SimpleTypeHolder(), InvalidFieldNamingStrategy.INSTANCE);

		exception.expect(MappingException.class);
		exception.expectMessage(InvalidFieldNamingStrategy.class.getName());
		exception.expectMessage(property.toString());

		property.getFieldName();
	}

	@Test // DATAMONGO-937
	public void shouldDetectAnnotatedLanguagePropertyCorrectly() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithLanguageProperty.class, "lang");
		assertThat(property.isLanguageProperty(), is(true));
	}

	@Test // DATAMONGO-937
	public void shouldDetectIplicitLanguagePropertyCorrectly() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithImplicitLanguageProperty.class, "language");
		assertThat(property.isLanguageProperty(), is(true));
	}

	@Test // DATAMONGO-976
	public void shouldDetectTextScorePropertyCorrectly() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithTextScoreProperty.class, "score");
		assertThat(property.isTextScoreProperty(), is(true));
	}

	@Test // DATAMONGO-976
	public void shouldDetectTextScoreAsReadOnlyProperty() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithTextScoreProperty.class, "score");
		assertThat(property.isWritable(), is(false));
	}

	@Test // DATAMONGO-1050
	public void shouldNotConsiderExplicitlyNameFieldAsIdProperty() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithExplicitlyRenamedIdProperty.class, "id");
		assertThat(property.isIdProperty(), is(false));
	}

	@Test // DATAMONGO-1050
	public void shouldConsiderPropertyAsIdWhenExplicitlyAnnotatedWithIdEvenWhenExplicitlyNamePresent() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithExplicitlyRenamedIdPropertyHavingIdAnnotation.class,
				"id");
		assertThat(property.isIdProperty(), is(true));
	}

	@Test // DATAMONGO-1373
	public void shouldConsiderComposedAnnotationsForIdField() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithComposedAnnotations.class, "myId");
		assertThat(property.isIdProperty(), is(true));
		assertThat(property.getFieldName(), is("_id"));
	}

	@Test // DATAMONGO-1373
	public void shouldConsiderComposedAnnotationsForFields() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithComposedAnnotations.class, "myField");
		assertThat(property.getFieldName(), is("myField"));
	}

	private MongoPersistentProperty getPropertyFor(Field field) {
		return getPropertyFor(entity, field);
	}

	private <T> MongoPersistentProperty getPropertyFor(Class<T> type, String fieldname) {
		return getPropertyFor(new BasicMongoPersistentEntity<T>(ClassTypeInformation.from(type)), fieldname);
	}

	private MongoPersistentProperty getPropertyFor(MongoPersistentEntity<?> persistentEntity, String fieldname) {
		return getPropertyFor(persistentEntity, ReflectionUtils.findField(persistentEntity.getType(), fieldname));
	}

	private MongoPersistentProperty getPropertyFor(MongoPersistentEntity<?> persistentEntity, Field field) {
		return new BasicMongoPersistentProperty(Property.of(field), persistentEntity, new SimpleTypeHolder(),
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

	static class DocumentWithExplicitlyRenamedIdProperty {

		@org.springframework.data.mongodb.core.mapping.Field("id") String id;
	}

	static class DocumentWithExplicitlyRenamedIdPropertyHavingIdAnnotation {

		@Id @org.springframework.data.mongodb.core.mapping.Field("id") String id;
	}

	static class DocumentWithComposedAnnotations {

		@ComposedIdAnnotation @ComposedFieldAnnotation String myId;
		@ComposedFieldAnnotation(name = "myField") String myField;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	@org.springframework.data.mongodb.core.mapping.Field
	static @interface ComposedFieldAnnotation {

		@AliasFor(annotation = org.springframework.data.mongodb.core.mapping.Field.class, attribute = "value")
		String name() default "_id";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	@Id
	static @interface ComposedIdAnnotation {
	}
}
