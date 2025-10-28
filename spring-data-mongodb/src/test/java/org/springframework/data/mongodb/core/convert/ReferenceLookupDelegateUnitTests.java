/*
 * Copyright 2021-2025 the original author or authors.
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
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.MongoEntityReader;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.expression.EvaluationContext;

/**
 * Unit tests for {@link ReferenceLookupDelegate}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class ReferenceLookupDelegateUnitTests {

	@Mock MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	@Mock SpELContext spELContext;
	@Mock EvaluationContext evaluationContext;
	@Mock MongoEntityReader entityReader;

	private ReferenceLookupDelegate lookupDelegate;

	@BeforeEach
	void beforeEach() {
		lookupDelegate = new ReferenceLookupDelegate(mappingContext, spELContext);
	}

	@Test // GH-3842
	void shouldComputePlainStringTargetCollection() {

		DocumentReference documentReference = mock(DocumentReference.class);
		MongoPersistentEntity entity = mock(MongoPersistentEntity.class);
		MongoPersistentProperty property = mock(MongoPersistentProperty.class);

		doReturn(entity).when(mappingContext).getRequiredPersistentEntity((Class) any());

		when(property.isDocumentReference()).thenReturn(true);
		when(property.getDocumentReference()).thenReturn(documentReference);
		when(documentReference.collection()).thenReturn("collection1");

		lookupDelegate.readReference(property, Collections.singletonList("one"), (referenceQuery, referenceCollection) -> {

			assertThat(referenceCollection.getCollection()).isEqualTo("collection1");
			return Collections.emptyList();
		}, entityReader);
	}

	@Test // GH-4612
	void shouldResolveEmptyListOnEmptyTargetCollection() {

		MongoPersistentProperty property = mock(MongoPersistentProperty.class);
		ReferenceLookupDelegate.LookupFunction lookupFunction = mock(ReferenceLookupDelegate.LookupFunction.class);

		when(property.isCollectionLike()).thenReturn(true);
		lookupDelegate.readReference(property, Collections.emptyList(), lookupFunction, entityReader);
		verify(lookupFunction, never()).apply(any(), any());
	}

	@Test // GH-4612
	void shouldResolveEmptyMapOnEmptyTargetCollection() {

		MongoPersistentProperty property = mock(MongoPersistentProperty.class);
		ReferenceLookupDelegate.LookupFunction lookupFunction = mock(ReferenceLookupDelegate.LookupFunction.class);

		when(property.isMap()).thenReturn(true);
		lookupDelegate.readReference(property, Collections.emptyMap(), lookupFunction, entityReader);
		verify(lookupFunction, never()).apply(any(), any());
	}

	@Test // GH-5065
	void emptyMapWithDocumentReferenceAnnotationShouldDeserializeToAnEmptyMap() {
		DocumentReference documentReference = mock(DocumentReference.class);
		when(documentReference.lookup()).thenReturn("{ '_id' : ?#{#target} }");
		when(documentReference.sort()).thenReturn("");
		MongoPersistentProperty property = mock(MongoPersistentProperty.class);
		when(property.isMap()).thenReturn(true);
		when(property.isDocumentReference()).thenReturn(true);
		when(property.getDocumentReference()).thenReturn(documentReference);
		when(property.isCollectionLike()).thenReturn(false);
		DocumentReferenceSource source = mock(DocumentReferenceSource.class);
		when(source.getTargetSource()).thenReturn(Document.parse("{}"));

		// Placeholder mock.  It should never get called.
		ReferenceLookupDelegate.LookupFunction lookupFunction = mock(ReferenceLookupDelegate.LookupFunction.class);

		Object target = lookupDelegate.readReference(property, source, lookupFunction, entityReader);

		// Since we mocked a placeholder that should never be called, make sure it is never called.
		verify(lookupFunction, never()).apply(any(), any());

		// Verify that all the mocks we created are used.
		verify(property, atLeastOnce()).isMap();
		verify(property, atLeastOnce()).isDocumentReference();
		verify(property, atLeastOnce()).getDocumentReference();
		verify(property, atLeastOnce()).isCollectionLike();
		verify(documentReference, atLeastOnce()).lookup();
		verify(documentReference, atLeastOnce()).sort();

		// Make sure we only call the properties we mocked.
		verifyNoMoreInteractions(documentReference, property, source);

		assertThat(target)
				.isNotNull()
				.isInstanceOf(Map.class);
	}

}
