/*
 * Copyright 2015-2017 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.util.MongoClientVersion.*;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Unit tests for {@link ReflectiveDBRefResolver}.
 * 
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class ReflectiveDBRefResolverUnitTests {

	@Mock MongoDbFactory dbFactoryMock;
	@Mock DBRef dbRefMock;
	@Mock MongoDatabase dbMock;
	@Mock MongoCollection<Document> collectionMock;
	@Mock FindIterable<Document> fi;

	@Before
	public void setUp() {

		when(dbRefMock.getCollectionName()).thenReturn("collection-1");
		when(dbRefMock.getId()).thenReturn("id-1");
		when(dbFactoryMock.getDb()).thenReturn(dbMock);
		when(dbMock.getCollection(eq("collection-1"), eq(Document.class))).thenReturn(collectionMock);
		when(collectionMock.find(any(Bson.class))).thenReturn(fi);
		when(fi.first()).thenReturn(new Document("_id", "id-1"));
	}

	@Test // DATAMONGO-1193
	public void fetchShouldNotLookUpDbWhenUsingDriverVersion2() {

		assumeThat(isMongo3Driver(), is(false));

		ReflectiveDBRefResolver.fetch(dbFactoryMock, dbRefMock);

		verify(dbFactoryMock, never()).getDb();
		verify(dbFactoryMock, never()).getDb(anyString());
	}

	@Test // DATAMONGO-1193
	public void fetchShouldUseDbToResolveDbRefWhenUsingDriverVersion3() {

		assumeThat(isMongo3Driver(), is(true));

		assertThat(ReflectiveDBRefResolver.fetch(dbFactoryMock, dbRefMock), notNullValue());
		verify(dbFactoryMock, times(1)).getDb();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1193
	public void fetchShouldThrowExceptionWhenDbFactoryIsNullUsingDriverVersion3() {

		assumeThat(isMongo3Driver(), is(true));

		ReflectiveDBRefResolver.fetch(null, dbRefMock);
	}
}
