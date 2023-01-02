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

import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.context.ApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;

import com.mongodb.DBRef;

/**
 * Unit tests for {@link MongoMappingContext}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
public class MongoMappingContextUnitTests {

	@Mock ApplicationContext applicationContext;

	@Test
	void addsSelfReferencingPersistentEntityCorrectly() throws Exception {

		MongoMappingContext context = new MongoMappingContext();

		context.setInitialEntitySet(Collections.singleton(SampleClass.class));
		context.initialize();
	}

	@Test
	void doesNotReturnPersistentEntityForMongoSimpleType() {

		MongoMappingContext context = new MongoMappingContext();
		assertThat(context.getPersistentEntity(DBRef.class)).isNull();
	}

	@Test // DATAMONGO-638
	void doesNotCreatePersistentEntityForAbstractMap() {

		MongoMappingContext context = new MongoMappingContext();
		assertThat(context.getPersistentEntity(AbstractMap.class)).isNull();
	}

	@Test // DATAMONGO-607
	void populatesPersistentPropertyWithCustomFieldNamingStrategy() {

		MongoMappingContext context = new MongoMappingContext();
		context.setApplicationContext(applicationContext);
		context.setFieldNamingStrategy(new FieldNamingStrategy() {

			public String getFieldName(PersistentProperty<?> property) {
				return property.getName().toUpperCase(Locale.US);
			}
		});

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(Person.class);
		assertThat(entity.getRequiredPersistentProperty("firstname").getFieldName()).isEqualTo("FIRSTNAME");
	}

	@Test // DATAMONGO-607
	void rejectsClassWithAmbiguousFieldMappings() {

		MongoMappingContext context = new MongoMappingContext();
		context.setApplicationContext(applicationContext);

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> context.getPersistentEntity(InvalidPerson.class))
				.withMessageContaining("firstname").withMessageContaining("lastname").withMessageContaining("foo")
				.withMessageContaining("@Field");
	}

	@Test // DATAMONGO-694
	void doesNotConsiderOverrridenAccessorANewField() {

		MongoMappingContext context = new MongoMappingContext();
		context.setApplicationContext(applicationContext);
		context.getPersistentEntity(Child.class);
	}

	@Test // DATAMONGO-688
	void mappingContextShouldAcceptClassWithImplicitIdProperty() {

		MongoMappingContext context = new MongoMappingContext();
		MongoPersistentEntity<?> pe = context.getRequiredPersistentEntity(ClassWithImplicitId.class);

		assertThat(pe).isNotNull();
		assertThat(pe.isIdProperty(pe.getRequiredPersistentProperty("id"))).isTrue();
	}

	@Test // DATAMONGO-688
	void mappingContextShouldAcceptClassWithExplicitIdProperty() {

		MongoMappingContext context = new MongoMappingContext();
		MongoPersistentEntity<?> pe = context.getRequiredPersistentEntity(ClassWithExplicitId.class);

		assertThat(pe).isNotNull();
		assertThat(pe.isIdProperty(pe.getRequiredPersistentProperty("myId"))).isTrue();
	}

	@Test // DATAMONGO-688
	void mappingContextShouldAcceptClassWithExplicitAndImplicitIdPropertyByGivingPrecedenceToExplicitIdProperty() {

		MongoMappingContext context = new MongoMappingContext();
		MongoPersistentEntity<?> pe = context.getRequiredPersistentEntity(ClassWithExplicitIdAndImplicitId.class);
		assertThat(pe).isNotNull();
	}

	@Test // DATAMONGO-688
	void rejectsClassWithAmbiguousExplicitIdPropertyFieldMappings() {

		MongoMappingContext context = new MongoMappingContext();
		assertThatThrownBy(() -> context.getPersistentEntity(ClassWithMultipleExplicitIds.class))
				.isInstanceOf(MappingException.class);
	}

	@Test // DATAMONGO-688
	void rejectsClassWithAmbiguousImplicitIdPropertyFieldMappings() {

		MongoMappingContext context = new MongoMappingContext();
		assertThatThrownBy(() -> context.getPersistentEntity(ClassWithMultipleImplicitIds.class))
				.isInstanceOf(MappingException.class);
	}

	@Test // DATAMONGO-976
	void shouldRejectClassWithInvalidTextScoreProperty() {

		MongoMappingContext context = new MongoMappingContext();

		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> context.getPersistentEntity(ClassWithInvalidTextScoreProperty.class))
				.withMessageContaining("score").withMessageContaining("Float").withMessageContaining("Double");
	}

	@Test // DATAMONGO-2599
	void shouldNotCreateEntityForEnum() {

		MongoMappingContext context = new MongoMappingContext();

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(ClassWithChronoUnit.class);

		assertThat(entity.getPersistentProperty("unit").isEntity()).isFalse();
		assertThat(context.hasPersistentEntityFor(ChronoUnit.class)).isFalse();
		assertThat(context.getPersistentEntity(ChronoUnit.class)).isNull();
	}

	@Test // GH-3656
	void shouldNotCreateEntityForOptionalGetter() {

		MongoMappingContext context = new MongoMappingContext();
		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(InterfaceWithMethodReturningOptional.class);

		assertThat(context.getPersistentEntities()).map(it -> it.getType()).doesNotContain((Class)
				Optional.class).contains((Class)Person.class);
	}

	@Test // GH-3656
	void shouldNotCreateEntityForOptionalField() {

		MongoMappingContext context = new MongoMappingContext();
		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(ClassWithOptionalField.class);

		assertThat(context.getPersistentEntities()).map(it -> it.getType()).doesNotContain((Class)
				Optional.class).contains((Class)Person.class);
	}

	public class SampleClass {

		Map<String, SampleClass> children;
	}

	class Person {

		String firstname, lastname;
	}

	class InvalidPerson {

		@org.springframework.data.mongodb.core.mapping.Field("foo") String firstname, lastname;
	}

	class Parent {

		String name;

		public String getName() {
			return name;
		}
	}

	class Child extends Parent {

		@Override
		public String getName() {
			return super.getName();
		}
	}

	class ClassWithImplicitId {

		String field;
		String id;
	}

	class ClassWithExplicitId {

		@Id String myId;
		String field;
	}

	class ClassWithExplicitIdAndImplicitId {

		@Id String myId;
		String id;
	}

	class ClassWithMultipleExplicitIds {

		@Id String myId;
		@Id String id;
	}

	class ClassWithMultipleImplicitIds {

		String _id;
		String id;
	}

	class ClassWithInvalidTextScoreProperty {

		@TextScore Locale score;
	}

	class ClassWithChronoUnit {

		ChronoUnit unit;
	}

	interface InterfaceWithMethodReturningOptional {

		Optional<Person> getPerson();
	}

	class ClassWithOptionalField {
		Optional<Person> person;
	}
}
