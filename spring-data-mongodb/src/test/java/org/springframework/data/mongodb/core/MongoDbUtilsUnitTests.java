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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Unit tests for {@link MongoDbUtils}.
 * 
 * @author Oliver Gierke
 */
public class MongoDbUtilsUnitTests {

	Mongo mongo;

	@Before
	public void setUp() throws Exception {
		this.mongo = new Mongo();
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
		assertThat(first, is(notNullValue()));
		assertThat(MongoDbUtils.getDB(mongo, "first"), is(first));

		DB second = MongoDbUtils.getDB(mongo, "second");
		assertThat(second, is(not(first)));
		assertThat(MongoDbUtils.getDB(mongo, "second"), is(second));
	}
}
