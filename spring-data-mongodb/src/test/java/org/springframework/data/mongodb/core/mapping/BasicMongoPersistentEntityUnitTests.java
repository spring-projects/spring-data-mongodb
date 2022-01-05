/*
 * Copyright 2011-2021 the original author or authors.
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
import static org.mockito.Mockito.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.spel.ExtensionAwareEvaluationContextProvider;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicMongoPersistentEntity}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
public class BasicMongoPersistentEntityUnitTests {

	@Mock ApplicationContext context;
	@Mock MongoPersistentProperty propertyMock;

	@Test
	void subclassInheritsAtDocumentAnnotation() {

		BasicMongoPersistentEntity<Person> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(Person.class));
		assertThat(entity.getCollection()).isEqualTo("contacts");
	}

	@Test
	void evaluatesSpELExpression() {

		MongoPersistentEntity<Company> entity = new BasicMongoPersistentEntity<>(ClassTypeInformation.from(Company.class));
		assertThat(entity.getCollection()).isEqualTo("35");
	}

	@Test // DATAMONGO-65, DATAMONGO-1108
	void collectionAllowsReferencingSpringBean() {

		CollectionProvider provider = new CollectionProvider();
		provider.collectionName = "reference";

		when(context.getBean("myBean")).thenReturn(provider);

		BasicMongoPersistentEntity<DynamicallyMapped> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(DynamicallyMapped.class));
		entity.setEvaluationContextProvider(new ExtensionAwareEvaluationContextProvider(context));

		assertThat(entity.getCollection()).isEqualTo("reference");

		provider.collectionName = "otherReference";
		assertThat(entity.getCollection()).isEqualTo("otherReference");
	}

	@Test // DATAMONGO-937
	void shouldDetectLanguageCorrectly() {

		BasicMongoPersistentEntity<DocumentWithLanguage> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(DocumentWithLanguage.class));

		assertThat(entity.getLanguage()).isEqualTo("spanish");
	}

	@Test // DATAMONGO-1053
	void verifyShouldThrowExceptionForInvalidTypeOfExplicitLanguageProperty() {

		doReturn(true).when(propertyMock).isExplicitLanguageProperty();
		doReturn(Number.class).when(propertyMock).getActualType();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);

		assertThatExceptionOfType(MappingException.class).isThrownBy(entity::verify);
	}

	@Test // DATAMONGO-1053
	void verifyShouldPassForStringAsExplicitLanguageProperty() {

		doReturn(true).when(propertyMock).isExplicitLanguageProperty();
		doReturn(String.class).when(propertyMock).getActualType();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);

		entity.verify();

		verify(propertyMock, times(1)).isExplicitLanguageProperty();
		verify(propertyMock, times(1)).getActualType();
	}

	@Test // DATAMONGO-1053
	void verifyShouldIgnoreNonExplicitLanguageProperty() {

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(AnyDocument.class));
		when(propertyMock.isExplicitLanguageProperty()).thenReturn(false);
		entity.addPersistentProperty(propertyMock);

		entity.verify();

		verify(propertyMock, times(1)).isExplicitLanguageProperty();
		verify(propertyMock, never()).getActualType();
	}

	@Test // DATAMONGO-1157
	void verifyShouldThrowErrorForLazyDBRefOnFinalClass() {

		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);

		doReturn(Class.class).when(propertyMock).getActualType();
		doReturn(true).when(propertyMock).isDbReference();
		doReturn(dbRefMock).when(propertyMock).getDBRef();
		doReturn(true).when(dbRefMock).lazy();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);

		assertThatExceptionOfType(MappingException.class).isThrownBy(entity::verify);
	}

	@Test // DATAMONGO-1157
	void verifyShouldThrowErrorForLazyDBRefArray() {

		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);

		doReturn(true).when(propertyMock).isDbReference();
		doReturn(true).when(propertyMock).isArray();
		doReturn(dbRefMock).when(propertyMock).getDBRef();
		doReturn(true).when(dbRefMock).lazy();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);

		assertThatExceptionOfType(MappingException.class).isThrownBy(entity::verify);
	}

	@Test // DATAMONGO-1157
	void verifyShouldPassForLazyDBRefOnNonArrayNonFinalClass() {

		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);

		doReturn(true).when(propertyMock).isDbReference();
		doReturn(Object.class).when(propertyMock).getActualType();
		doReturn(dbRefMock).when(propertyMock).getDBRef();
		doReturn(true).when(dbRefMock).lazy();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);
		entity.verify();

		verify(propertyMock, times(1)).isDbReference();
	}

	@Test // DATAMONGO-1157
	void verifyShouldPassForNonLazyDBRefOnFinalClass() {

		org.springframework.data.mongodb.core.mapping.DBRef dbRefMock = mock(
				org.springframework.data.mongodb.core.mapping.DBRef.class);

		doReturn(true).when(propertyMock).isDbReference();
		doReturn(dbRefMock).when(propertyMock).getDBRef();
		doReturn(false).when(dbRefMock).lazy();

		BasicMongoPersistentEntity<AnyDocument> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(AnyDocument.class));
		entity.addPersistentProperty(propertyMock);
		entity.verify();

		verify(dbRefMock, times(1)).lazy();
	}

	@Test // DATAMONGO-1291
	void metaInformationShouldBeReadCorrectlyFromInheritedDocumentAnnotation() {

		BasicMongoPersistentEntity<DocumentWithCustomAnnotation> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(DocumentWithCustomAnnotation.class));

		assertThat(entity.getCollection()).isEqualTo("collection-1");
	}

	@Test // DATAMONGO-1373
	void metaInformationShouldBeReadCorrectlyFromComposedDocumentAnnotation() {

		BasicMongoPersistentEntity<DocumentWithComposedAnnotation> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(DocumentWithComposedAnnotation.class));

		assertThat(entity.getCollection()).isEqualTo("custom-collection");
	}

	@Test // DATAMONGO-1874
	void usesEvaluationContextExtensionInDynamicDocumentName() {

		BasicMongoPersistentEntity<MappedWithExtension> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(MappedWithExtension.class));
		entity.setEvaluationContextProvider(
				new ExtensionAwareEvaluationContextProvider(Collections.singletonList(new SampleExtension())));

		assertThat(entity.getCollection()).isEqualTo("collectionName");
	}

	@Test // DATAMONGO-1854
	void readsSimpleCollation() {

		BasicMongoPersistentEntity<WithSimpleCollation> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(WithSimpleCollation.class));

		assertThat(entity.getCollation()).isEqualTo(org.springframework.data.mongodb.core.query.Collation.of("en_US"));
	}

	@Test // DATAMONGO-1854
	void readsDocumentCollation() {

		BasicMongoPersistentEntity<WithDocumentCollation> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(WithDocumentCollation.class));

		assertThat(entity.getCollation()).isEqualTo(org.springframework.data.mongodb.core.query.Collation.of("en_US"));
	}

	@Test // DATAMONGO-2565
	void usesCorrectExpressionsForCollectionAndCollation() {

		BasicMongoPersistentEntity<WithCollectionAndCollationFromSpEL> entity = new BasicMongoPersistentEntity<>(
				ClassTypeInformation.from(WithCollectionAndCollationFromSpEL.class));
		entity.setEvaluationContextProvider(
				new ExtensionAwareEvaluationContextProvider(Collections.singletonList(new SampleExtension())));

		assertThat(entity.getCollection()).isEqualTo("collectionName");
		assertThat(entity.getCollation()).isEqualTo(Collation.of("en_US"));
	}

	@Test // DATAMONGO-2341
	void detectsShardedEntityCorrectly() {

		assertThat(entityOf(WithDefaultShardKey.class).isSharded()).isTrue();
		assertThat(entityOf(Contact.class).isSharded()).isFalse();
	}

	@Test // DATAMONGO-2341
	void readsDefaultShardKey() {

		assertThat(entityOf(WithDefaultShardKey.class).getShardKey().getDocument())
				.isEqualTo(new org.bson.Document("_id", 1));
	}

	@Test // DATAMONGO-2341
	void readsSingleShardKey() {

		assertThat(entityOf(WithSingleShardKey.class).getShardKey().getDocument())
				.isEqualTo(new org.bson.Document("country", 1));
	}

	@Test // DATAMONGO-2341
	void readsMultiShardKey() {

		assertThat(entityOf(WithMultiShardKey.class).getShardKey().getDocument())
				.isEqualTo(new org.bson.Document("country", 1).append("userid", 1));
	}

	static <T> BasicMongoPersistentEntity<T> entityOf(Class<T> type) {
		return new BasicMongoPersistentEntity<>(ClassTypeInformation.from(type));
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

	private static class AnyDocument {}

	@CustomDocumentAnnotation
	private static class DocumentWithCustomAnnotation {}

	@ComposedDocumentAnnotation
	private static class DocumentWithComposedAnnotation {}

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

	@Document("${value.from.file}")
	class MappedWithValue {}

	@Document(collation = "#{myCollation}")
	class WithCollationFromSpEL {}

	@Document(collection = "#{myProperty}", collation = "#{myCollation}")
	class WithCollectionAndCollationFromSpEL {}

	@Document(collation = "en_US")
	class WithSimpleCollation {}

	@Document(collation = "{ 'locale' : 'en_US' }")
	class WithDocumentCollation {}

	@Sharded
	private class WithDefaultShardKey {}

	@Sharded("country")
	private class WithSingleShardKey {}

	@Sharded({ "country", "userid" })
	private class WithMultiShardKey {}

	static class SampleExtension implements EvaluationContextExtension {

		@Override
		public String getExtensionId() {
			return "sampleExtension";
		}

		@Override
		public Map<String, Object> getProperties() {

			Map<String, Object> properties = new LinkedHashMap<>();
			properties.put("myProperty", "collectionName");
			properties.put("myCollation", "en_US");
			return properties;
		}
	}
}
