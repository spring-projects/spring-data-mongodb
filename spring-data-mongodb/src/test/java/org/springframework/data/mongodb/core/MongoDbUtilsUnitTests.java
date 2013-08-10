/*
 * Copyright 2012 the original author or authors.
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

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MongoDbUtils}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoDbUtilsUnitTests {

	@Mock
	Mongo mongo;

	@Before
	public void setUp() throws Exception {

		when(mongo.getDB(anyString())).then(new Answer<DB>() {
			public DB answer(InvocationOnMock invocation) throws Throwable {
				return mock(DB.class);
			}
		});

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

	@Test
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
			fail("Unexpected exception thrown during transaction completion: "+e);
		}

		// ensure transaction synchronization manager has no bound resources
		// at end of test
		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty(), is(true));
	}

	@Test
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
			fail("Unexpected exception thrown during transaction completion: "+e);
		}

		// ensure transaction synchronization manager has no bound
		// transaction resources at end of test
		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty(), is(true));
	}

	/**
	 * Simulate transaction rollback/commit completion protocol on managed
	 * transaction synchronizations which will unbind managed transaction
	 * resources. See:
	 * TransactionSynchronizationUtils.triggerBeforeCompletion() and
	 * TransactionSynchronizationUtils.triggerAfterCompletion(int completionStatus).
	 * Does not swallow exceptions for testing purposes.
	 */
	private void simulateTransactionCompletion() {

		// triggerBeforeCompletion() implementation without swallowed exceptions
		List<TransactionSynchronization> synchronizations = TransactionSynchronizationManager.getSynchronizations();
		for (TransactionSynchronization synchronization : synchronizations) {
			synchronization.beforeCompletion();
		}

		// triggerAfterCompletion() implementation without swallowed exceptions
		List<TransactionSynchronization> remainingSynchronizations = TransactionSynchronizationManager.getSynchronizations();
		if (remainingSynchronizations != null) {
			for (TransactionSynchronization remainingSynchronization : remainingSynchronizations) {
				remainingSynchronization.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
			}
		}
	}
}
