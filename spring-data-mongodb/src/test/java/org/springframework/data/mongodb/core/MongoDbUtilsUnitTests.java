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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.mongodb.DB;
import com.mongodb.Mongo;

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
}
