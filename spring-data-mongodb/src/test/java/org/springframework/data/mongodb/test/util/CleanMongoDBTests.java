/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.test.util.CleanMongoDB.Types;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class CleanMongoDBTests {

	private CleanMongoDB cleaner;
	private @Mock Statement baseStatementMock;
	private @Mock Description descriptionMock;
	private @Mock MongoClient mongoClientMock;
	private @Mock DB db1mock;
	private @Mock DB db2mock;
	private @Mock DBCollection collection1mock;

	@Before
	public void setUp() {

		when(mongoClientMock.getDatabaseNames()).thenReturn(Arrays.asList("admin", "db1", "db2"));
		when(mongoClientMock.getDB(eq("db1"))).thenReturn(db1mock);
		when(mongoClientMock.getDB(eq("db2"))).thenReturn(db2mock);
		when(db1mock.collectionExists(anyString())).thenReturn(true);
		when(db2mock.collectionExists(anyString())).thenReturn(true);
		when(db1mock.getCollectionNames()).thenReturn(Collections.singleton("collection-1"));
		when(db2mock.getCollectionNames()).thenReturn(Collections.<String> emptySet());
		when(db1mock.getCollectionFromString(eq("collection-1"))).thenReturn(collection1mock);

		cleaner = new CleanMongoDB(mongoClientMock);
	}

	@Test
	public void preservesSystemCollectionsCorrectly() throws Throwable {

		cleaner.clean(Types.DATABASE);

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, times(1)).dropDatabase(eq("db1"));
		verify(mongoClientMock, times(1)).dropDatabase(eq("db2"));
		verify(mongoClientMock, never()).dropDatabase(eq("admin"));
	}

	@Test
	public void removesCollectionsCorrectly() throws Throwable {

		cleaner.clean(Types.COLLECTION);

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, never()).dropDatabase(eq("db1"));
		verify(mongoClientMock, never()).dropDatabase(eq("db2"));
		verify(mongoClientMock, never()).dropDatabase(eq("admin"));

		verify(collection1mock, times(1)).drop();
	}

	@Test
	public void removesIndexesCorrectly() throws Throwable {

		cleaner.clean(Types.INDEX);

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, never()).dropDatabase(eq("db1"));
		verify(mongoClientMock, never()).dropDatabase(eq("db2"));
		verify(mongoClientMock, never()).dropDatabase(eq("admin"));

		verify(collection1mock, times(1)).dropIndexes();
	}
}
