/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.jmolecules.ddd.annotation.Identity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;
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
 * @author Divya Srivastava
 */
public class BasicMongoPersistentPropertyUnitTests {

	private MongoPersistentEntity<Person> entity;

	@BeforeEach
	void setup() {
		entity = new BasicMongoPersistentEntity<>(ClassTypeInformation.from(Person.class));
	}

	@Test
	void usesAnnotatedFieldName() {

		Field field = ReflectionUtils.findField(Person.class, "firstname");
		assertThat(getPropertyFor(field).getFieldName()).isEqualTo("foo");
	}

	@Test
	void returns_IdForIdProperty() {
		Field field = ReflectionUtils.findField(Person.class, "id");
		MongoPersistentProperty property = getPropertyFor(field);
		assertThat(property.isIdProperty()).isTrue();
		assertThat(property.getFieldName()).isEqualTo("_id");
	}

	@Test
	void returnsPropertyNameForUnannotatedProperties() {

		Field field = ReflectionUtils.findField(Person.class, "lastname");
		assertThat(getPropertyFor(field).getFieldName()).isEqualTo("lastname");
	}

	@Test
	void preventsNegativeOrder() {
		getPropertyFor(ReflectionUtils.findField(Person.class, "ssn"));
	}

	@Test // DATAMONGO-553
	void usesPropertyAccessForThrowableCause() {

		BasicMongoPersistentEntity<Throwable> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(Throwable.class));
		MongoPersistentProperty property = getPropertyFor(entity, "cause");

		assertThat(property.usePropertyAccess()).isTrue();
	}

	@Test // DATAMONGO-607
	void usesCustomFieldNamingStrategyByDefault() throws Exception {

		ClassTypeInformation<Person> type = ClassTypeInformation.from(Person.class);
		Field field = ReflectionUtils.findField(Person.class, "lastname");

		MongoPersistentProperty property = new BasicMongoPersistentProperty(Property.of(type, field), entity,
				SimpleTypeHolder.DEFAULT, UppercaseFieldNamingStrategy.INSTANCE);
		assertThat(property.getFieldName()).isEqualTo("LASTNAME");

		field = ReflectionUtils.findField(Person.class, "firstname");

		property = new BasicMongoPersistentProperty(Property.of(type, field), entity, SimpleTypeHolder.DEFAULT,
				UppercaseFieldNamingStrategy.INSTANCE);
		assertThat(property.getFieldName()).isEqualTo("foo");
	}

	@Test // DATAMONGO-607
	void rejectsInvalidValueReturnedByFieldNamingStrategy() {

		ClassTypeInformation<Person> type = ClassTypeInformation.from(Person.class);
		Field field = ReflectionUtils.findField(Person.class, "lastname");

		MongoPersistentProperty property = new BasicMongoPersistentProperty(Property.of(type, field), entity,
				SimpleTypeHolder.DEFAULT, InvalidFieldNamingStrategy.INSTANCE);

		assertThatExceptionOfType(MappingException.class).isThrownBy(property::getFieldName)
				.withMessageContaining(InvalidFieldNamingStrategy.class.getName()).withMessageContaining(property.toString());
	}

	@Test // DATAMONGO-937
	void shouldDetectAnnotatedLanguagePropertyCorrectly() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithLanguageProperty.class, "lang");
		assertThat(property.isLanguageProperty()).isTrue();
	}

	@Test // DATAMONGO-937
	void shouldDetectImplicitLanguagePropertyCorrectly() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithImplicitLanguageProperty.class, "language");
		assertThat(property.isLanguageProperty()).isTrue();
	}

	@Test // DATAMONGO-976
	void shouldDetectTextScorePropertyCorrectly() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithTextScoreProperty.class, "score");
		assertThat(property.isTextScoreProperty()).isTrue();
	}

	@Test // DATAMONGO-976
	void shouldDetectTextScoreAsReadOnlyProperty() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithTextScoreProperty.class, "score");
		assertThat(property.isWritable()).isFalse();
	}

	@Test // DATAMONGO-1050
	void shouldNotConsiderExplicitlyNameFieldAsIdProperty() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithExplicitlyRenamedIdProperty.class, "id");
		assertThat(property.isIdProperty()).isFalse();
	}

	@Test // DATAMONGO-1050
	void shouldConsiderPropertyAsIdWhenExplicitlyAnnotatedWithIdEvenWhenExplicitlyNamePresent() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithExplicitlyRenamedIdPropertyHavingIdAnnotation.class,
				"id");
		assertThat(property.isIdProperty()).isTrue();
	}

	@Test // DATAMONGO-1373
	void shouldConsiderComposedAnnotationsForIdField() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithComposedAnnotations.class, "myId");
		assertThat(property.isIdProperty()).isTrue();
		assertThat(property.getFieldName()).isEqualTo("_id");
	}

	@Test // DATAMONGO-1373
	void shouldConsiderComposedAnnotationsForFields() {

		MongoPersistentProperty property = getPropertyFor(DocumentWithComposedAnnotations.class, "myField");
		assertThat(property.getFieldName()).isEqualTo("myField");
	}

	@Test // DATAMONGO-1737
	void honorsFieldOrderWhenIteratingOverProperties() {

		MongoMappingContext context = new MongoMappingContext();
		MongoPersistentEntity<?> entity = context.getPersistentEntity(Sample.class);

		List<String> properties = new ArrayList<>();

		entity.doWithProperties((MongoPersistentProperty property) -> properties.add(property.getName()));

		assertThat(properties).containsExactly("first", "second", "third");
	}

	@Test // GH-3407
	void shouldDetectWritability() {

		assertThat(getPropertyFor(WithFieldWrite.class, "fieldWithDefaults").writeNullValues()).isFalse();
		assertThat(getPropertyFor(WithFieldWrite.class, "fieldWithField").writeNullValues()).isFalse();
		assertThat(getPropertyFor(WithFieldWrite.class, "writeNonNull").writeNullValues()).isFalse();
		assertThat(getPropertyFor(WithFieldWrite.class, "writeAlways").writeNullValues()).isTrue();
	}

	@Test // DATAMONGO-1798
	void fieldTypeShouldReturnActualTypeForNonIdProperties() {

		MongoPersistentProperty property = getPropertyFor(Person.class, "lastname");
		assertThat(property.getFieldType()).isEqualTo(String.class);
	}

	@Test // DATAMONGO-1798
	void fieldTypeShouldBeObjectIdForPropertiesAnnotatedWithCommonsId() {

		MongoPersistentProperty property = getPropertyFor(Person.class, "id");
		assertThat(property.getFieldType()).isEqualTo(ObjectId.class);
	}

	@Test // DATAMONGO-1798
	void fieldTypeShouldBeImplicitForPropertiesAnnotatedWithMongoId() {

		MongoPersistentProperty property = getPropertyFor(WithStringMongoId.class, "id");
		assertThat(property.getFieldType()).isEqualTo(String.class);
	}

	@Test // DATAMONGO-1798
	void fieldTypeShouldBeObjectIdForPropertiesAnnotatedWithMongoIdAndTargetTypeObjectId() {

		MongoPersistentProperty property = getPropertyFor(WithStringMongoIdMappedToObjectId.class, "id");
		assertThat(property.getFieldType()).isEqualTo(ObjectId.class);
	}

	@Test // DATAMONGO-2460
	void fieldTypeShouldBeDocumentForPropertiesAnnotatedIdWhenAComplexTypeAndFieldTypeImplicit() {

		MongoPersistentProperty property = getPropertyFor(WithComplexId.class, "id");
		assertThat(property.getFieldType()).isEqualTo(Document.class);
	}

	@Test // GH-3803
	void considersJMoleculesIdentityExplicitlyAnnotatedIdentifier() {

		MongoPersistentProperty property = getPropertyFor(WithJMoleculesIdentity.class, "identifier");

		assertThat(property.isIdProperty()).isTrue();
		assertThat(property.isExplicitIdProperty()).isTrue();
	}

	private MongoPersistentProperty getPropertyFor(Field field) {
		return getPropertyFor(entity, field);
	}

	private static <T> MongoPersistentProperty getPropertyFor(Class<T> type, String fieldname) {
		return getPropertyFor(new BasicMongoPersistentEntity<>(ClassTypeInformation.from(type)), fieldname);
	}

	private static MongoPersistentProperty getPropertyFor(MongoPersistentEntity<?> entity, String fieldname) {
		return getPropertyFor(entity, ReflectionUtils.findField(entity.getType(), fieldname));
	}

	private static MongoPersistentProperty getPropertyFor(MongoPersistentEntity<?> entity, Field field) {
		return new BasicMongoPersistentProperty(Property.of(entity.getTypeInformation(), field), entity,
				SimpleTypeHolder.DEFAULT, PropertyNameFieldNamingStrategy.INSTANCE);
	}

	class Person {

		@Id String id;

		@org.springframework.data.mongodb.core.mapping.Field("foo") String firstname;
		String lastname;

		@org.springframework.data.mongodb.core.mapping.Field(order = -20) String ssn;
	}

	class Sample {

		@org.springframework.data.mongodb.core.mapping.Field(order = 2) String second;
		@org.springframework.data.mongodb.core.mapping.Field(order = 3) String third;
		@org.springframework.data.mongodb.core.mapping.Field(order = 1) String first;
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

	static class WithFieldWrite {

		int fieldWithDefaults;
		@org.springframework.data.mongodb.core.mapping.Field int fieldWithField;
		@org.springframework.data.mongodb.core.mapping.Field(
				write = org.springframework.data.mongodb.core.mapping.Field.Write.NON_NULL) Integer writeNonNull;

		@org.springframework.data.mongodb.core.mapping.Field(
				write = org.springframework.data.mongodb.core.mapping.Field.Write.ALWAYS) Integer writeAlways;

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
	static @interface ComposedIdAnnotation {}

	static class WithStringMongoId {

		@MongoId String id;
	}

	static class WithStringMongoIdMappedToObjectId {

		@MongoId(FieldType.OBJECT_ID) String id;
	}

	static class ComplexId {

		String value;
	}

	static class WithComplexId {

		@Id @org.springframework.data.mongodb.core.mapping.Field ComplexId id;
	}

	static class WithJMoleculesIdentity {
		@Identity ObjectId identifier;
	}
}
