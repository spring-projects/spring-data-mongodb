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
import static org.mockito.Mockito.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicMongoPersistentEntity}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicMongoPersistentEntityUnitTests {

	@Mock ApplicationContext context;
	@Mock MongoPersistentProperty propertyMock;

	@Test
	public void subclassInheritsAtDocumentAnnotation() {

		BasicMongoPersistentEntity<Person> entity = new BasicMongoPersistentEntity<Person>(
				ClassTypeInformation.from(Person.class));
		assertThat(entity.getCollection(), is("contacts"));
	}

	@Test
	public void evaluatesSpELExpression() {

		MongoPersistentEntity<Company> entity = new BasicMongoPersistentEntity<Company>(
				ClassTypeInformation.from(Company.class));
		assertThat(entity.getCollection(), is("35"));
	}

	@Test // DATAMONGO-65, DATAMONGO-1108
	public void collectionAllowsReferencingSpringBean() {

		CollectionProvider provider = new CollectionProvider();
		provider.collectionName = "reference";

		when(context.getBean("myBean")).thenReturn(provider);
		when(context.containsBean("myBean")).thenReturn(true);

		BasicMongoPersistentEntity<DynamicallyMapped> entity = new BasicMongoPersistentEntity<DynamicallyMapped>(
				ClassTypeInformation.from(DynamicallyMapped.class));
		entity.setApplicationContext(context);

		assertThat(entity.getCollection(), is("reference"));

		provider.collectionName = "otherReference";
		assertThat(entity.getCollection(), is("otherReference"));
	}

	@Test // DATAMONGO-937
	public void shouldDetectLanguageCorrectly() {

		BasicMongoPersistentEntity<DocumentWithLanguage> entity = new BasicMongoPersistentEntity<DocumentWithLanguage>(
				ClassTypeInformation.from(DocumentWithLanguage.class));
		assertThat(entity.getLanguage(), is("spanish"));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test(expected = MappingException.class) // DATAMONGO-1053
	public void verifyShouldThrowExceptionForInvalidTypeOfExplicitLanguageProperty() {

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));

		when(propertyMock.isExplicitLanguageProperty()).thenReturn(true);
		when(propertyMock.getActualType()).thenReturn((Class) Number.class);

		entity.addPersistentProperty(propertyMock);
		entity.verify();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test // DATAMONGO-1053
	public void verifyShouldPassForStringAsExplicitLanguageProperty() {

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		when(propertyMock.isExplicitLanguageProperty()).thenReturn(true);
		when(propertyMock.getActualType()).thenReturn((Class) String.class);
		entity.addPersistentProperty(propertyMock);

		entity.verify();

		verify(propertyMock, times(1)).isExplicitLanguageProperty();
		verify(propertyMock, times(1)).getActualType();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test // DATAMONGO-1053
	public void verifyShouldIgnoreNonExplicitLanguageProperty() {

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		when(propertyMock.isExplicitLanguageProperty()).thenReturn(false);
		entity.addPersistentProperty(propertyMock);

		entity.verify();

		verify(propertyMock, times(1)).isExplicitLanguageProperty();
		verify(propertyMock, never()).getActualType();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test(expected = MappingException.class) // DATAMONGO-1157
	public void verifyShouldThrowErrorForLazyDBRefOnFinalClass() {

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);
		when(propertyMock.isDbReference()).thenReturn(true);
		when(propertyMock.getDBRef()).thenReturn(dbRefMock);
		when(dbRefMock.lazy()).thenReturn(true);
		when(propertyMock.getActualType()).thenReturn((Class) Class.class);
		entity.addPersistentProperty(propertyMock);

		entity.verify();
	}

	@Test(expected = MappingException.class) // DATAMONGO-1157
	public void verifyShouldThrowErrorForLazyDBRefArray() {

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);
		when(propertyMock.isDbReference()).thenReturn(true);
		when(propertyMock.getDBRef()).thenReturn(dbRefMock);
		when(dbRefMock.lazy()).thenReturn(true);
		when(propertyMock.isArray()).thenReturn(true);
		entity.addPersistentProperty(propertyMock);

		entity.verify();
	}

	@Test // DATAMONGO-1157
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void verifyShouldPassForLazyDBRefOnNonArrayNonFinalClass() {

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);
		when(propertyMock.isDbReference()).thenReturn(true);
		when(propertyMock.getDBRef()).thenReturn(dbRefMock);
		when(dbRefMock.lazy()).thenReturn(true);
		when(propertyMock.getActualType()).thenReturn((Class) Object.class);
		entity.addPersistentProperty(propertyMock);

		entity.verify();

		verify(propertyMock, times(1)).isDbReference();
	}

	@Test // DATAMONGO-1157
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void verifyShouldPassForNonLazyDBRefOnFinalClass() {

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);
		when(propertyMock.isDbReference()).thenReturn(true);
		when(propertyMock.getDBRef()).thenReturn(dbRefMock);
		when(dbRefMock.lazy()).thenReturn(false);
		entity.addPersistentProperty(propertyMock);

		entity.verify();

		verify(dbRefMock, times(1)).lazy();
	}

	@Test // DATAMONGO-1291
	public void metaInformationShouldBeReadCorrectlyFromInheritedDocumentAnnotation() {

		BasicMongoPersistentEntity<DocumentWithCustomAnnotation> entity = new BasicMongoPersistentEntity<DocumentWithCustomAnnotation>(
				ClassTypeInformation.from(DocumentWithCustomAnnotation.class));

		assertThat(entity.getCollection(), is("collection-1"));
	}

	@Test // DATAMONGO-1373
	public void metaInformationShouldBeReadCorrectlyFromComposedDocumentAnnotation() {

		BasicMongoPersistentEntity<DocumentWithComposedAnnotation> entity = new BasicMongoPersistentEntity<DocumentWithComposedAnnotation>(
				ClassTypeInformation.from(DocumentWithComposedAnnotation.class));

		assertThat(entity.getCollection(), is("custom-collection"));
	}

	@Document(collection = "contacts")
	class Contact {}

	class Person extends Contact {}

	@Document(collection = "#{35}")
	class Company {}

	@Document(collection = "#{myBean.collectionName}")
	class DynamicallyMapped {}

	class CollectionProvider {
		String collectionName;

		public String getCollectionName() {
			return collectionName;
		}
	}

	@Document(language = "spanish")
	static class DocumentWithLanguage {}

	static class AnyDocument {}

	@CustomDocumentAnnotation
	static class DocumentWithCustomAnnotation {}

	@ComposedDocumentAnnotation
	static class DocumentWithComposedAnnotation {}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@Document(collection = "collection-1")
	static @interface CustomDocumentAnnotation {
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@Document
	static @interface ComposedDocumentAnnotation {

		@AliasFor(annotation = Document.class, attribute = "collection")
		String name() default "custom-collection";
	}
}
