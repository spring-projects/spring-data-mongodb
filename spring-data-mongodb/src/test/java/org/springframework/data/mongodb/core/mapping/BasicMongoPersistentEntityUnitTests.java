/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.spel.ExtensionAwareEvaluationContextProvider;
import org.springframework.data.spel.spi.EvaluationContextExtension;
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
		assertThat(entity.getCollection()).isEqualTo("contacts");
	}

	@Test
	public void evaluatesSpELExpression() {

		MongoPersistentEntity<Company> entity = new BasicMongoPersistentEntity<Company>(
				ClassTypeInformation.from(Company.class));
		assertThat(entity.getCollection()).isEqualTo("35");
	}

	@Test // DATAMONGO-65, DATAMONGO-1108
	public void collectionAllowsReferencingSpringBean() {

		CollectionProvider provider = new CollectionProvider();
		provider.collectionName = "reference";

		when(context.getBean("myBean")).thenReturn(provider);

		BasicMongoPersistentEntity<DynamicallyMapped> entity = new BasicMongoPersistentEntity<DynamicallyMapped>(
				ClassTypeInformation.from(DynamicallyMapped.class));
		entity.setEvaluationContextProvider(new ExtensionAwareEvaluationContextProvider(context));

		assertThat(entity.getCollection()).isEqualTo("reference");

		provider.collectionName = "otherReference";
		assertThat(entity.getCollection()).isEqualTo("otherReference");
	}

	@Test // DATAMONGO-937
	public void shouldDetectLanguageCorrectly() {

		BasicMongoPersistentEntity<DocumentWithLanguage> entity = new BasicMongoPersistentEntity<DocumentWithLanguage>(
				ClassTypeInformation.from(DocumentWithLanguage.class));

		assertThat(entity.getLanguage()).isEqualTo("spanish");
	}

	@Test // DATAMONGO-1053
	public void verifyShouldThrowExceptionForInvalidTypeOfExplicitLanguageProperty() {

		doReturn(true).when(propertyMock).isExplicitLanguageProperty();
		doReturn(Number.class).when(propertyMock).getActualType();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> entity.verify());
	}

	@Test // DATAMONGO-1053
	public void verifyShouldPassForStringAsExplicitLanguageProperty() {

		doReturn(true).when(propertyMock).isExplicitLanguageProperty();
		doReturn(String.class).when(propertyMock).getActualType();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);

		entity.verify();

		verify(propertyMock, times(1)).isExplicitLanguageProperty();
		verify(propertyMock, times(1)).getActualType();
	}

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

	@Test // DATAMONGO-1157
	public void verifyShouldThrowErrorForLazyDBRefOnFinalClass() {

		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);

		doReturn(Class.class).when(propertyMock).getActualType();
		doReturn(true).when(propertyMock).isDbReference();
		doReturn(dbRefMock).when(propertyMock).getDBRef();
		doReturn(true).when(dbRefMock).lazy();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> entity.verify());
	}

	@Test // DATAMONGO-1157
	public void verifyShouldThrowErrorForLazyDBRefArray() {

		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);

		doReturn(true).when(propertyMock).isDbReference();
		doReturn(true).when(propertyMock).isArray();
		doReturn(dbRefMock).when(propertyMock).getDBRef();
		doReturn(true).when(dbRefMock).lazy();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> entity.verify());
	}

	@Test // DATAMONGO-1157
	public void verifyShouldPassForLazyDBRefOnNonArrayNonFinalClass() {

		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);

		doReturn(true).when(propertyMock).isDbReference();
		doReturn(Object.class).when(propertyMock).getActualType();
		doReturn(dbRefMock).when(propertyMock).getDBRef();
		doReturn(true).when(dbRefMock).lazy();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);
		entity.verify();

		verify(propertyMock, times(1)).isDbReference();
	}

	@Test // DATAMONGO-1157
	public void verifyShouldPassForNonLazyDBRefOnFinalClass() {

		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);

		doReturn(true).when(propertyMock).isDbReference();
		doReturn(dbRefMock).when(propertyMock).getDBRef();
		doReturn(false).when(dbRefMock).lazy();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<AnyDocument>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);
		entity.verify();

		verify(dbRefMock, times(1)).lazy();
	}

	@Test // DATAMONGO-1291
	public void metaInformationShouldBeReadCorrectlyFromInheritedDocumentAnnotation() {

		BasicMongoPersistentEntity<DocumentWithCustomAnnotation> entity = new BasicMongoPersistentEntity<DocumentWithCustomAnnotation>(
				ClassTypeInformation.from(DocumentWithCustomAnnotation.class));

		assertThat(entity.getCollection()).isEqualTo("collection-1");
	}

	@Test // DATAMONGO-1373
	public void metaInformationShouldBeReadCorrectlyFromComposedDocumentAnnotation() {

		BasicMongoPersistentEntity<DocumentWithComposedAnnotation> entity = new BasicMongoPersistentEntity<DocumentWithComposedAnnotation>(
				ClassTypeInformation.from(DocumentWithComposedAnnotation.class));

		assertThat(entity.getCollection()).isEqualTo("custom-collection");
	}

	@Test // DATAMONGO-1874
	public void usesEvaluationContextExtensionInDynamicDocumentName() {

		BasicMongoPersistentEntity<MappedWithExtension> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(MappedWithExtension.class));
		entity.setEvaluationContextProvider(
				new ExtensionAwareEvaluationContextProvider(Arrays.asList(new SampleExtension())));

		assertThat(entity.getCollection()).isEqualTo("collectionName");
	}

	@Document("contacts")
	class Contact {}

	class Person extends Contact {}

	@Document("#{35}")
	class Company {}

	@Document("#{@myBean.collectionName}")
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
	@Document("collection-1")
	static @interface CustomDocumentAnnotation {
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@Document
	static @interface ComposedDocumentAnnotation {

		@AliasFor(annotation = Document.class, attribute = "collection")
		String name() default "custom-collection";
	}

	// DATAMONGO-1874

	@Document("#{myProperty}")
	class MappedWithExtension {}

	static class SampleExtension implements EvaluationContextExtension {

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.spel.spi.EvaluationContextExtension#getExtensionId()
		 */
		@Override
		public String getExtensionId() {
			return "sampleExtension";
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.spel.spi.EvaluationContextExtension#getProperties()
		 */
		@Override
		public Map<String, Object> getProperties() {
			return Collections.singletonMap("myProperty", "collectionName");
		}
	}
}
