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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.MappingException;

import com.mongodb.DBRef;

/**
 * Unit tests for {@link MongoMappingContext}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoMappingContextUnitTests {

	@Mock ApplicationContext applicationContext;

	@Rule public ExpectedException exception = ExpectedException.none();

	@Test
	public void addsSelfReferencingPersistentEntityCorrectly() throws Exception {

		MongoMappingContext context = new MongoMappingContext();

		context.setInitialEntitySet(Collections.singleton(SampleClass.class));
		context.initialize();
	}

	@Test
	public void doesNotReturnPersistentEntityForMongoSimpleType() {

		MongoMappingContext context = new MongoMappingContext();
		assertThat(context.getPersistentEntity(DBRef.class), is(Optional.empty()));
	}

	@Test // DATAMONGO-638
	public void doesNotCreatePersistentEntityForAbstractMap() {

		MongoMappingContext context = new MongoMappingContext();
		assertThat(context.getPersistentEntity(AbstractMap.class), is(Optional.empty()));
	}

	@Test // DATAMONGO-607
	public void populatesPersistentPropertyWithCustomFieldNamingStrategy() {

		MongoMappingContext context = new MongoMappingContext();
		context.setApplicationContext(applicationContext);
		context.setFieldNamingStrategy(new FieldNamingStrategy() {

			public String getFieldName(PersistentProperty<?> property) {
				return property.getName().toUpperCase(Locale.US);
			}
		});

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(Person.class);
		assertThat(entity.getRequiredPersistentProperty("firstname").getFieldName(), is("FIRSTNAME"));
	}

	@Test // DATAMONGO-607
	public void rejectsClassWithAmbiguousFieldMappings() {

		exception.expect(MappingException.class);
		exception.expectMessage("firstname");
		exception.expectMessage("lastname");
		exception.expectMessage("foo");
		exception.expectMessage("@Field");

		MongoMappingContext context = new MongoMappingContext();
		context.setApplicationContext(applicationContext);
		context.getPersistentEntity(InvalidPerson.class);
	}

	@Test // DATAMONGO-694
	public void doesNotConsiderOverrridenAccessorANewField() {

		MongoMappingContext context = new MongoMappingContext();
		context.setApplicationContext(applicationContext);
		context.getPersistentEntity(Child.class);
	}

	@Test // DATAMONGO-688
	public void mappingContextShouldAcceptClassWithImplicitIdProperty() {

		MongoMappingContext context = new MongoMappingContext();
		BasicMongoPersistentEntity<?> pe = context.getRequiredPersistentEntity(ClassWithImplicitId.class);

		assertThat(pe, is(not(nullValue())));
		assertThat(pe.isIdProperty(pe.getRequiredPersistentProperty("id")), is(true));
	}

	@Test // DATAMONGO-688
	public void mappingContextShouldAcceptClassWithExplicitIdProperty() {

		MongoMappingContext context = new MongoMappingContext();
		BasicMongoPersistentEntity<?> pe = context.getRequiredPersistentEntity(ClassWithExplicitId.class);

		assertThat(pe, is(not(nullValue())));
		assertThat(pe.isIdProperty(pe.getRequiredPersistentProperty("myId")), is(true));
	}

	@Test // DATAMONGO-688
	public void mappingContextShouldAcceptClassWithExplicitAndImplicitIdPropertyByGivingPrecedenceToExplicitIdProperty() {

		MongoMappingContext context = new MongoMappingContext();
		BasicMongoPersistentEntity<?> pe = context.getRequiredPersistentEntity(ClassWithExplicitIdAndImplicitId.class);
		assertThat(pe, is(not(nullValue())));
	}

	@Test(expected = MappingException.class) // DATAMONGO-688
	public void rejectsClassWithAmbiguousExplicitIdPropertyFieldMappings() {

		MongoMappingContext context = new MongoMappingContext();
		context.getPersistentEntity(ClassWithMultipleExplicitIds.class);
	}

	@Test(expected = MappingException.class) // DATAMONGO-688
	public void rejectsClassWithAmbiguousImplicitIdPropertyFieldMappings() {

		MongoMappingContext context = new MongoMappingContext();
		context.getPersistentEntity(ClassWithMultipleImplicitIds.class);
	}

	@Test // DATAMONGO-976
	public void shouldRejectClassWithInvalidTextScoreProperty() {

		exception.expect(MappingException.class);
		exception.expectMessage("score");
		exception.expectMessage("Float");
		exception.expectMessage("Double");

		MongoMappingContext context = new MongoMappingContext();
		context.getPersistentEntity(ClassWithInvalidTextScoreProperty.class);
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
}
