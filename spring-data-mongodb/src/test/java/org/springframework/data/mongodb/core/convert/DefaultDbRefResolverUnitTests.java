/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Unit tests for {@link DefaultDbRefResolver}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultDbRefResolverUnitTests {

	@Mock MongoDatabaseFactory factoryMock;
	@Mock MongoDatabase dbMock;
	@Mock MongoCollection<Document> collectionMock;
	@Mock FindIterable<Document> cursorMock;
	@Mock MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	@Mock SpELContext spELContext;
	private DefaultDbRefResolver resolver;

	@BeforeEach
	void setUp() {

		when(factoryMock.getMongoDatabase()).thenReturn(dbMock);
		when(factoryMock.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		when(dbMock.getCollection(anyString(), any(Class.class))).thenReturn(collectionMock);
		when(collectionMock.find(any(Document.class))).thenReturn(cursorMock);

		resolver = new DefaultDbRefResolver(factoryMock);
	}

	@Test // DATAMONGO-1194
	@SuppressWarnings("unchecked")
	void bulkFetchShouldLoadDbRefsCorrectly() {

		DBRef ref1 = new DBRef("collection-1", new ObjectId());
		DBRef ref2 = new DBRef("collection-1", new ObjectId());

		resolver.bulkFetch(Arrays.asList(ref1, ref2));

		ArgumentCaptor<Document> captor = ArgumentCaptor.forClass(Document.class);

		verify(collectionMock, times(1)).find(captor.capture());

		Document _id = DocumentTestUtils.getAsDocument(captor.getValue(), "_id");
		Iterable<Object> $in = DocumentTestUtils.getTypedValue(_id, "$in", Iterable.class);

		assertThat($in).hasSize(2);
	}

	@Test // DATAMONGO-1194
	void bulkFetchShouldThrowExceptionWhenUsingDifferentCollectionsWithinSetOfReferences() {

		DBRef ref1 = new DBRef("collection-1", new ObjectId());
		DBRef ref2 = new DBRef("collection-2", new ObjectId());

		assertThatThrownBy(() -> resolver.bulkFetch(Arrays.asList(ref1, ref2)))
				.isInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Test // DATAMONGO-1194
	void bulkFetchShouldReturnEarlyForEmptyLists() {

		resolver.bulkFetch(Collections.emptyList());

		verify(collectionMock, never()).find(Mockito.any(Document.class));
	}

	@Test // DATAMONGO-1194
	void bulkFetchShouldRestoreOriginalOrder() {

		Document o1 = new Document("_id", new ObjectId());
		Document o2 = new Document("_id", new ObjectId());

		DBRef ref1 = new DBRef("collection-1", o1.get("_id"));
		DBRef ref2 = new DBRef("collection-1", o2.get("_id"));

		when(cursorMock.into(any())).then(invocation -> Arrays.asList(o2, o1));

		assertThat(resolver.bulkFetch(Arrays.asList(ref1, ref2))).containsExactly(o1, o2);
	}

	@Test // DATAMONGO-1765
	void bulkFetchContainsDuplicates() {

		Document document = new Document("_id", new ObjectId());

		DBRef ref1 = new DBRef("collection-1", document.get("_id"));
		DBRef ref2 = new DBRef("collection-1", document.get("_id"));

		when(cursorMock.into(any())).then(invocation -> Arrays.asList(document));

		assertThat(resolver.bulkFetch(Arrays.asList(ref1, ref2))).containsExactly(document, document);
	}

	@Test // GH-5065
	@DisplayName("GH-5065: Empty Map with @DocumentReference annotation should deserialize to an empty map.")
	void resolveEmptyMapIsNotNull() {
		DocumentReference documentReference = mock(DocumentReference.class);
		when(documentReference.lookup()).thenReturn("{ '_id' : ?#{#target} }");
		when(documentReference.sort()).thenReturn("");
		when(documentReference.lazy()).thenReturn(false);
		MongoPersistentProperty property = mock(MongoPersistentProperty.class);
		when(property.isCollectionLike()).thenReturn(false);
		when(property.isMap()).thenReturn(true);
		when(property.isDocumentReference()).thenReturn(true);
		when(property.getDocumentReference()).thenReturn(documentReference);
		DocumentReferenceSource source = mock(DocumentReferenceSource.class);
		when(source.getTargetSource()).thenReturn(Document.parse("{}"));
		ReferenceLookupDelegate lookupDelegate = new ReferenceLookupDelegate(mappingContext, spELContext);

		ReferenceResolver.MongoEntityReader entityReader = mock(ReferenceResolver.MongoEntityReader.class);

		Object target = resolver.resolveReference(property, source, lookupDelegate, entityReader);

		verify(property, atLeastOnce()).isMap();
		verify(property, atLeastOnce()).isDocumentReference();
		verify(property, atLeastOnce()).getDocumentReference();
		verify(property, atLeastOnce()).isCollectionLike();
		verify(documentReference, atLeastOnce()).lookup();
		verify(documentReference, atLeastOnce()).sort();
		verify(documentReference, atLeastOnce()).lazy();
		verify(source, atLeastOnce()).getTargetSource();
		verifyNoMoreInteractions(documentReference, property, source); // Make sure we only call the properties we mocked.

		assertThat(target)
				.isNotNull()
				.isInstanceOf(Map.class);
	}

	@Test // GH-5065
	@DisplayName("GH-5065: Lazy loaded empty Map with @DocumentReference annotation should deserialize to an empty map with a non-null values property.")
	void resolveLazyLoadedEmptyMapIsNotNull() {
		DocumentReference documentReference = mock(DocumentReference.class);
		when(documentReference.lookup()).thenReturn("{ '_id' : ?#{#target} }");
		when(documentReference.sort()).thenReturn("");
		when(documentReference.lazy()).thenReturn(true);
		MongoPersistentProperty property = mock(MongoPersistentProperty.class);
		when(property.isCollectionLike()).thenReturn(false);
		when(property.isMap()).thenReturn(true);
		when(property.isDocumentReference()).thenReturn(true);
		when(property.getDocumentReference()).thenReturn(documentReference);
        //noinspection rawtypes,unchecked
        when(property.getType()).thenReturn((Class) Map.class);
		DocumentReferenceSource source = mock(DocumentReferenceSource.class);
		when(source.getTargetSource()).thenReturn(Document.parse("{}"));
		ReferenceLookupDelegate lookupDelegate = new ReferenceLookupDelegate(mappingContext, spELContext);

		ReferenceResolver.MongoEntityReader entityReader = mock(ReferenceResolver.MongoEntityReader.class);

		Object target = resolver.resolveReference(property, source, lookupDelegate, entityReader);

		verify(property, atLeastOnce()).isMap();
		verify(property, atLeastOnce()).isDocumentReference();
		verify(property, atLeastOnce()).getDocumentReference();
		verify(property, atLeastOnce()).isCollectionLike();
		verify(property, atLeastOnce()).getType();
		verify(documentReference, atLeastOnce()).lazy();
		verify(source, atLeastOnce()).getTargetSource();
		verifyNoMoreInteractions(documentReference, property, source); // Make sure we only call the properties we mocked.

		assertThat(target)
				.isNotNull()
				.isInstanceOf(Map.class)
				.asInstanceOf(MAP).values().isNotNull();
	}
}
