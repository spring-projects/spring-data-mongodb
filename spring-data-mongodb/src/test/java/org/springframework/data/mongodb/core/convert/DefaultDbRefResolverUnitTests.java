/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DBObjectTestUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

/**
 * Unit tests for {@link DefaultDbRefResolver}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultDbRefResolverUnitTests {

	@Mock MongoDbFactory factoryMock;
	@Mock DB dbMock;
	@Mock DBCollection collectionMock;
	@Mock DBCursor cursorMock;
	DefaultDbRefResolver resolver;

	@Before
	public void setUp() {

		when(factoryMock.getDb()).thenReturn(dbMock);
		when(dbMock.getCollection(anyString())).thenReturn(collectionMock);
		when(collectionMock.find(Mockito.any(DBObject.class))).thenReturn(cursorMock);
		when(cursorMock.toArray()).thenReturn(Collections.<DBObject>emptyList());

		resolver = new DefaultDbRefResolver(factoryMock);
	}

	@Test // DATAMONGO-1194
	@SuppressWarnings("unchecked")
	public void bulkFetchShouldLoadDbRefsCorrectly() {

		DBRef ref1 = new DBRef("collection-1", new ObjectId());
		DBRef ref2 = new DBRef("collection-1", new ObjectId());

		resolver.bulkFetch(Arrays.asList(ref1, ref2));

		ArgumentCaptor<DBObject> captor = ArgumentCaptor.forClass(DBObject.class);

		verify(collectionMock, times(1)).find(captor.capture());

		DBObject _id = DBObjectTestUtils.getAsDBObject(captor.getValue(), "_id");
		Iterable<Object> $in = DBObjectTestUtils.getTypedValue(_id, "$in", Iterable.class);

		assertThat($in, iterableWithSize(2));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAMONGO-1194
	public void bulkFetchShouldThrowExceptionWhenUsingDifferntCollectionsWithinSetOfReferences() {

		DBRef ref1 = new DBRef("collection-1", new ObjectId());
		DBRef ref2 = new DBRef("collection-2", new ObjectId());

		resolver.bulkFetch(Arrays.asList(ref1, ref2));
	}

	@Test // DATAMONGO-1194
	public void bulkFetchShouldReturnEarlyForEmptyLists() {

		resolver.bulkFetch(Collections.<DBRef>emptyList());

		verify(collectionMock, never()).find(Mockito.any(DBObject.class));
	}

	@Test // DATAMONGO-1194
	public void bulkFetchShouldRestoreOriginalOrder() {

		DBObject o1 = new BasicDBObject("_id", new ObjectId());
		DBObject o2 = new BasicDBObject("_id", new ObjectId());

		DBRef ref1 = new DBRef("collection-1", o1.get("_id"));
		DBRef ref2 = new DBRef("collection-1", o2.get("_id"));

		when(cursorMock.toArray()).thenReturn(Arrays.asList(o2, o1));

		assertThat(resolver.bulkFetch(Arrays.asList(ref1, ref2)), contains(o1, o2));
	}

	@Test // DATAMONGO-1765
	public void bulkFetchContainsDuplicates() {

		DBObject document = new BasicDBObject("_id", new ObjectId());

		DBRef ref1 = new DBRef("collection-1", document.get("_id"));
		DBRef ref2 = new DBRef("collection-1", document.get("_id"));

		when(cursorMock.toArray()).thenReturn(Arrays.asList(document));

		assertThat(resolver.bulkFetch(Arrays.asList(ref1, ref2)), contains(document, document));
	}
}
