/*
 * Copyright 2012-2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.CannotGetMongoDbConnectionException;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * Unit tests for {@link MongoDbUtils}.
 * 
 * @author Oliver Gierke
 * @author Randy Watler
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoDbUtilsUnitTests {

	@Mock Mongo mongo;
	@Mock MongoClient mongoClientMock;
	@Mock DB dbMock;

	@Before
	public void setUp() throws Exception {

		when(mongo.getDB(anyString())).thenReturn(dbMock).thenReturn(mock(DB.class));
		when(mongoClientMock.getDB(anyString())).thenReturn(dbMock);

		TransactionSynchronizationManager.initSynchronization();
	}

	@After
	public void tearDown() {

		for (Object key : TransactionSynchronizationManager.getResourceMap().keySet()) {
			TransactionSynchronizationManager.unbindResource(key);
		}

		TransactionSynchronizationManager.clearSynchronization();
	}

	@Test
	public void returnsNewInstanceForDifferentDatabaseName() {

		DB first = MongoDbUtils.getDB(mongo, "first");
		DB second = MongoDbUtils.getDB(mongo, "second");
		assertThat(second, is(not(first)));
	}

	@Test
	public void returnsSameInstanceForSameDatabaseName() {

		DB first = MongoDbUtils.getDB(mongo, "first");
		assertThat(first, is(notNullValue()));
		assertThat(MongoDbUtils.getDB(mongo, "first"), is(sameInstance(first)));
	}

	@Test // DATAMONGO-737
	public void handlesTransactionSynchronizationLifecycle() {

		// ensure transaction synchronization manager has no registered
		// transaction synchronizations or bound resources at start of test
		assertThat(TransactionSynchronizationManager.getSynchronizations().isEmpty(), is(true));
		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty(), is(true));

		// access database for one mongo instance, (registers transaction
		// synchronization and binds transaction resource)
		MongoDbUtils.getDB(mongo, "first");

		// ensure transaction synchronization manager has registered
		// transaction synchronizations and bound resources
		assertThat(TransactionSynchronizationManager.getSynchronizations().isEmpty(), is(false));
		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty(), is(false));

		// simulate transaction completion, (unbinds transaction resource)
		try {
			simulateTransactionCompletion();
		} catch (Exception e) {
			fail("Unexpected exception thrown during transaction completion: " + e);
		}

		// ensure transaction synchronization manager has no bound resources
		// at end of test
		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty(), is(true));
	}

	@Test // DATAMONGO-737
	public void handlesTransactionSynchronizationsLifecycle() {

		// ensure transaction synchronization manager has no registered
		// transaction synchronizations or bound resources at start of test
		assertThat(TransactionSynchronizationManager.getSynchronizations().isEmpty(), is(true));
		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty(), is(true));

		// access multiple databases for one mongo instance, (registers
		// transaction synchronizations and binds transaction resources)
		MongoDbUtils.getDB(mongo, "first");
		MongoDbUtils.getDB(mongo, "second");

		// ensure transaction synchronization manager has registered
		// transaction synchronizations and bound resources
		assertThat(TransactionSynchronizationManager.getSynchronizations().isEmpty(), is(false));
		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty(), is(false));

		// simulate transaction completion, (unbinds transaction resources)
		try {
			simulateTransactionCompletion();
		} catch (Exception e) {
			fail("Unexpected exception thrown during transaction completion: " + e);
		}

		// ensure transaction synchronization manager has no bound
		// transaction resources at end of test
		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty(), is(true));
	}

	@Test // DATAMONGO-1218
	@SuppressWarnings("deprecation")
	public void getDBDAuthenticateViaAuthDbWhenCalledWithMongoInstance() {

		assumeThat(MongoClientVersion.isMongo3Driver(), is(false));

		when(dbMock.getName()).thenReturn("db");

		try {
			MongoDbUtils.getDB(mongo, "db", new UserCredentials("shallan", "davar"), "authdb");
		} catch (CannotGetMongoDbConnectionException e) {
			// need to catch that one since we cannot answer the reflective call sufficiently
		}

		verify(mongo, times(1)).getDB("authdb");
	}

	@Test // DATAMONGO-1218
	@SuppressWarnings("deprecation")
	public void getDBDShouldSkipAuthenticationViaAuthDbWhenCalledWithMongoClientInstance() {

		MongoDbUtils.getDB(mongoClientMock, "db", new UserCredentials("dalinar", "kholin"), "authdb");

		verify(mongoClientMock, never()).getDB("authdb");
	}

	/**
	 * Simulate transaction rollback/commit completion protocol on managed transaction synchronizations which will unbind
	 * managed transaction resources. Does not swallow exceptions for testing purposes.
	 * 
	 * @see TransactionSynchronizationUtils#triggerBeforeCompletion()
	 * @see TransactionSynchronizationUtils#triggerAfterCompletion(int)
	 */
	private void simulateTransactionCompletion() {

		// triggerBeforeCompletion() implementation without swallowed exceptions
		List<TransactionSynchronization> synchronizations = TransactionSynchronizationManager.getSynchronizations();
		for (TransactionSynchronization synchronization : synchronizations) {
			synchronization.beforeCompletion();
		}

		// triggerAfterCompletion() implementation without swallowed exceptions
		List<TransactionSynchronization> remainingSynchronizations = TransactionSynchronizationManager
				.getSynchronizations();
		if (remainingSynchronizations != null) {
			for (TransactionSynchronization remainingSynchronization : remainingSynchronizations) {
				remainingSynchronization.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
			}
		}
	}
}
