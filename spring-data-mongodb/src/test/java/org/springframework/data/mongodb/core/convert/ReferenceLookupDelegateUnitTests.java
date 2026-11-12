/*
 * Copyright 2021-present the original author or authors.
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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mongodb.core.convert.ReferenceLookupDelegate.ListDocumentReferenceQuery;
import org.springframework.data.mongodb.core.convert.ReferenceLookupDelegate.MapDocumentReferenceQuery;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.MongoEntityReader;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.expression.EvaluationContext;

/**
 * Unit tests for {@link ReferenceLookupDelegate}.
 *
 * @author Christoph Strobl
 * @author Priyanshu Gagiya
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

	@Test // GH-5272
	void shouldRestoreReferenceOrderUsingCompoundLookup() {

		Document first = new Document("tenant", "one").append("code", "shared");
		Document second = new Document("tenant", "two").append("code", "shared");
		List<Document> references = List.of(second, new Document("tenant", "missing"), first);
		ListDocumentReferenceQuery query = new ListDocumentReferenceQuery(new Document("$or", references), new Document());

		assertThat(query.restoreOrder(List.of(first, second))).containsExactly(second, first);
	}

	@Test // GH-5272
	void shouldRetainResultOrderForEqualReferenceIndexes() {

		Document matched = new Document("_id", 1);
		Document tie1 = new Document("tenant", "one").append("code", "a");
		Document tie2 = new Document("tenant", "one").append("code", "b");
		Document unmatched1 = new Document("_id", 98);
		Document unmatched2 = new Document("_id", 99);
		List<Document> references = List.of(new Document("_id", 1), new Document("tenant", "one"));
		ListDocumentReferenceQuery query = new ListDocumentReferenceQuery(new Document("$or", references), new Document());

		assertThat(query.restoreOrder(List.of(unmatched1, tie1, unmatched2, tie2, matched))).containsExactly(matched, tie1,
				tie2, unmatched1, unmatched2);
	}

	@Test // GH-5272
	void shouldRestoreReferenceOrderUsingIdLookup() {

		Document one = new Document("_id", 1).append("name", "one");
		Document two = new Document("_id", 2).append("name", "two");
		Document three = new Document("_id", 3).append("name", "three");
		Document withoutId = new Document("name", "none");
		List<Document> references = List.of(new Document("_id", 3), new Document("_id", 99), new Document("_id", 1),
				new Document("_id", 3), new Document("_id", 2), new Document("_id", null));
		ListDocumentReferenceQuery query = new ListDocumentReferenceQuery(new Document("$or", references), new Document());

		assertThat(query.restoreOrder(new LinkedList<>(List.of(withoutId, one, two, three)))).containsExactly(three, one,
				two, withoutId);
	}

	@Test // GH-5272
	void shouldRetainResultOrderForDuplicateIds() {

		Document one = new Document("_id", 1).append("name", "one");
		Document duplicate1 = new Document("_id", 2).append("name", "first");
		Document duplicate2 = new Document("_id", 2).append("name", "second");
		List<Document> references = List.of(new Document("_id", 2), new Document("_id", 1), new Document("_id", 2));
		ListDocumentReferenceQuery query = new ListDocumentReferenceQuery(new Document("$or", references), new Document());

		assertThat(query.restoreOrder(List.of(duplicate1, one, duplicate2))).containsExactly(duplicate1, duplicate2, one);
	}

	@Test // GH-5272
	void shouldRestoreReferenceOrderUsingArrayIdsByContent() {

		Document one = new Document("_id", new byte[] { 1 });
		Document two = new Document("_id", new byte[] { 2 });
		Document three = new Document("_id", new byte[] { 3 });
		List<Document> references = List.of(new Document("_id", new byte[] { 3 }), new Document("_id", new byte[] { 1 }),
				new Document("_id", new byte[] { 2 }));
		ListDocumentReferenceQuery query = new ListDocumentReferenceQuery(new Document("$or", references), new Document());

		assertThat(query.restoreOrder(List.of(one, two, three))).containsExactly(three, one, two);
		assertThat(query.restoreOrder(List.of(one, three))).containsExactly(three, one);
	}

	@Test // GH-5272
	void shouldRestoreReferenceOrderForTwoDocuments() {

		Document first = new Document("_id", 1);
		Document second = new Document("_id", 2);
		Document unmatched = new Document("_id", 3);
		List<Document> references = List.of(new Document("_id", 2), new Document("_id", 1));
		ListDocumentReferenceQuery query = new ListDocumentReferenceQuery(new Document("$or", references), new Document());

		assertThat(query.restoreOrder(List.of(first, second))).containsExactly(second, first);
		assertThat(query.restoreOrder(List.of(unmatched, first))).containsExactly(first, unmatched);
		assertThat(query.restoreOrder(List.of(first, first))).containsExactly(first, first);
	}

	@Test // GH-5272
	void shouldRestoreMapReferencesUsingIdLookup() {

		Document one = new Document("_id", 1);
		Document two = new Document("_id", 2).append("name", "first");
		Document duplicate = new Document("_id", 2).append("name", "second");
		Document array = new Document("_id", new byte[] { 3 });
		Map<Object, Document> filters = new LinkedHashMap<>();
		filters.put("b", new Document("_id", 2));
		filters.put("missing", new Document("_id", 99));
		filters.put("a", new Document("_id", 1));
		filters.put("again", new Document("_id", 2));
		filters.put("array", new Document("_id", new byte[] { 3 }));
		MapDocumentReferenceQuery query = new MapDocumentReferenceQuery(new Document("$or", filters.values()),
				new Document(), filters);

		Iterable<Document> result = query.restoreOrder(new LinkedList<>(List.of(one, array, two, duplicate)));

		assertThat(result).singleElement().satisfies(it -> assertThat(it).containsExactly(entry("b", two),
				entry("missing", null), entry("a", one), entry("again", two), entry("array", array)));
	}

	@Test // GH-5272
	void shouldRestoreMapReferencesUsingCompoundLookup() {

		Document first = new Document("tenant", "one").append("code", "shared");
		Document second = new Document("tenant", "two").append("code", "shared");
		Map<Object, Document> filters = new LinkedHashMap<>();
		filters.put("two", new Document("tenant", "two"));
		filters.put("missing", new Document("tenant", "missing"));
		filters.put("one", new Document("tenant", "one").append("code", "shared"));
		MapDocumentReferenceQuery query = new MapDocumentReferenceQuery(new Document("$or", filters.values()),
				new Document(), filters);

		assertThat(query.restoreOrder(List.of(first, second))).singleElement()
				.satisfies(it -> assertThat(it).containsExactly(entry("two", second), entry("missing", null),
						entry("one", first)));
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
}
