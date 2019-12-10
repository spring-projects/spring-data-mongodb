/*
 * Copyright 2016-2019 the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DocumentTestUtils;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Unit tests for {@link DefaultDbRefResolver}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultDbRefResolverUnitTests {

	@Mock MongoDbFactory factoryMock;
	@Mock MongoDatabase dbMock;
	@Mock MongoCollection<Document> collectionMock;
	@Mock FindIterable<Document> cursorMock;
	DefaultDbRefResolver resolver;

	@Before
	public void setUp() {

		when(factoryMock.getMongoDatabase()).thenReturn(dbMock);
		when(dbMock.getCollection(anyString(), any(Class.class))).thenReturn(collectionMock);
		when(collectionMock.find(any(Document.class))).thenReturn(cursorMock);

		resolver = new DefaultDbRefResolver(factoryMock);
	}

	@Test // DATAMONGO-1194
	@SuppressWarnings("unchecked")
	public void bulkFetchShouldLoadDbRefsCorrectly() {

		DBRef ref1 = new DBRef("collection-1", new ObjectId());
		DBRef ref2 = new DBRef("collection-1", new ObjectId());

		resolver.bulkFetch(Arrays.asList(ref1, ref2));

		ArgumentCaptor<Document> captor = ArgumentCaptor.forClass(Document.class);

		verify(collectionMock, times(1)).find(captor.capture());

		Document _id = DocumentTestUtils.getAsDocument(captor.getValue(), "_id");
		Iterable<Object> $in = DocumentTestUtils.getTypedValue(_id, "$in", Iterable.class);

		assertThat($in).hasSize(2);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAMONGO-1194
	public void bulkFetchShouldThrowExceptionWhenUsingDifferntCollectionsWithinSetOfReferences() {

		DBRef ref1 = new DBRef("collection-1", new ObjectId());
		DBRef ref2 = new DBRef("collection-2", new ObjectId());

		resolver.bulkFetch(Arrays.asList(ref1, ref2));
	}

	@Test // DATAMONGO-1194
	public void bulkFetchShouldReturnEarlyForEmptyLists() {

		resolver.bulkFetch(Collections.<DBRef> emptyList());

		verify(collectionMock, never()).find(Mockito.any(Document.class));
	}

	@Test // DATAMONGO-1194
	public void bulkFetchShouldRestoreOriginalOrder() {

		Document o1 = new Document("_id", new ObjectId());
		Document o2 = new Document("_id", new ObjectId());

		DBRef ref1 = new DBRef("collection-1", o1.get("_id"));
		DBRef ref2 = new DBRef("collection-1", o2.get("_id"));

		when(cursorMock.into(any())).then(invocation -> Arrays.asList(o2, o1));

		assertThat(resolver.bulkFetch(Arrays.asList(ref1, ref2))).containsExactly(o1, o2);
	}

	@Test // DATAMONGO-1765
	public void bulkFetchContainsDuplicates() {

		Document document = new Document("_id", new ObjectId());

		DBRef ref1 = new DBRef("collection-1", document.get("_id"));
		DBRef ref2 = new DBRef("collection-1", document.get("_id"));

		when(cursorMock.into(any())).then(invocation -> Arrays.asList(document));

		assertThat(resolver.bulkFetch(Arrays.asList(ref1, ref2))).containsExactly(document, document);
	}
}
