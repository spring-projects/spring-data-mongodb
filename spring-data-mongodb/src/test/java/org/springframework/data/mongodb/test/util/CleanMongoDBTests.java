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

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.test.util.CleanMongoDB.Struct;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class CleanMongoDBTests {

	private CleanMongoDB cleaner;

	// JUnit internals
	private @Mock Statement baseStatementMock;
	private @Mock Description descriptionMock;

	// MongoClient in use
	private @Mock MongoClient mongoClientMock;

	// Some Mock DBs
	private @Mock DB db1mock, db2mock;
	private @Mock DBCollection db1collection1mock, db1collection2mock, db2collection1mock;

	@SuppressWarnings("serial")
	@Before
	public void setUp() {

		// DB setup
		when(mongoClientMock.getDatabaseNames()).thenReturn(Arrays.asList("admin", "db1", "db2"));
		when(mongoClientMock.getDB(eq("db1"))).thenReturn(db1mock);
		when(mongoClientMock.getDB(eq("db2"))).thenReturn(db2mock);

		// collections have to exist
		when(db1mock.collectionExists(anyString())).thenReturn(true);
		when(db2mock.collectionExists(anyString())).thenReturn(true);

		// init collection names per database
		when(db1mock.getCollectionNames()).thenReturn(new HashSet<String>() {
			{
				add("db1collection1");
				add("db1collection2");
			}
		});
		when(db2mock.getCollectionNames()).thenReturn(Collections.singleton("db2collection1"));

		// return collections according to names
		when(db1mock.getCollectionFromString(eq("db1collection1"))).thenReturn(db1collection1mock);
		when(db1mock.getCollectionFromString(eq("db1collection2"))).thenReturn(db1collection2mock);
		when(db2mock.getCollectionFromString(eq("db2collection1"))).thenReturn(db2collection1mock);

		cleaner = new CleanMongoDB(mongoClientMock);
	}

	@Test
	public void preservesSystemDBsCorrectlyWhenCleaningDatabase() throws Throwable {

		cleaner.clean(Struct.DATABASE);

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, never()).dropDatabase(eq("admin"));
	}

	@Test
	public void preservesNamedDBsCorrectlyWhenCleaningDatabase() throws Throwable {

		cleaner.clean(Struct.DATABASE);
		cleaner.preserveDatabases("db1");

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, never()).dropDatabase(eq("db1"));
	}

	@Test
	public void dropsAllDBsCorrectlyWhenCleaingDatabaseAndNotExplictDBNamePresent() throws Throwable {

		cleaner.clean(Struct.DATABASE);

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, times(1)).dropDatabase(eq("db1"));
		verify(mongoClientMock, times(1)).dropDatabase(eq("db2"));
	}

	@Test
	public void dropsSpecifiedDBsCorrectlyWhenExplicitNameSet() throws Throwable {

		cleaner.clean(Struct.DATABASE);
		cleaner.useDatabases("db2");

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, times(1)).dropDatabase(eq("db2"));
		verify(mongoClientMock, never()).dropDatabase(eq("db1"));
	}

	@Test
	public void doesNotRemoveAnyDBwhenCleaningCollections() throws Throwable {

		cleaner.clean(Struct.COLLECTION);

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, never()).dropDatabase(eq("db1"));
		verify(mongoClientMock, never()).dropDatabase(eq("db2"));
		verify(mongoClientMock, never()).dropDatabase(eq("admin"));
	}

	@Test
	public void doesNotDropCollectionsFromPreservedDBs() throws Throwable {

		cleaner.clean(Struct.COLLECTION);
		cleaner.preserveDatabases("db1");

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(db1collection1mock, never()).drop();
		verify(db1collection2mock, never()).drop();
		verify(db2collection1mock, times(1)).drop();
	}

	@Test
	public void removesAllCollectionsFromAllDatabasesWhenNotLimitedToSpecificOnes() throws Throwable {

		cleaner.clean(Struct.COLLECTION);

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(db1collection1mock, times(1)).drop();
		verify(db1collection2mock, times(1)).drop();
		verify(db2collection1mock, times(1)).drop();
	}

	@Test
	public void removesOnlyNamedCollectionsWhenSpecified() throws Throwable {

		cleaner.clean(Struct.COLLECTION);
		cleaner.useCollections("db1collection2");

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(db1collection1mock, never()).drop();
		verify(db2collection1mock, never()).drop();
		verify(db1collection2mock, times(1)).drop();
	}

	@Test
	public void removesIndexesCorrectly() throws Throwable {

		cleaner.clean(Struct.INDEX);

		cleaner.apply(baseStatementMock, descriptionMock).evaluate();

		verify(mongoClientMock, never()).dropDatabase(eq("db1"));
		verify(mongoClientMock, never()).dropDatabase(eq("db2"));
		verify(mongoClientMock, never()).dropDatabase(eq("admin"));

		verify(db1collection1mock, times(1)).dropIndexes();
	}
}
