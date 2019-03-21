/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Unit tests for {@link SimpleReactiveMongoDatabaseFactory}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleReactiveMongoDatabaseFactoryUnitTests {

	@Mock MongoClient mongoClient;
	@Mock MongoDatabase database;

	@Test // DATAMONGO-1903
	public void rejectsIllegalDatabaseNames() {

		rejectsDatabaseName("foo.bar");
		rejectsDatabaseName("foo$bar");
		rejectsDatabaseName("foo\\bar");
		rejectsDatabaseName("foo//bar");
		rejectsDatabaseName("foo bar");
		rejectsDatabaseName("foo\"bar");
	}

	private void rejectsDatabaseName(String databaseName) {
		assertThatThrownBy(() -> new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
