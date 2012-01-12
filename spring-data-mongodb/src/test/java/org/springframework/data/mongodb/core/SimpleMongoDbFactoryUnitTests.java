/*
 * Copyright 2011 the original author or authors.
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

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.net.UnknownHostException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.Mongo;
import com.mongodb.MongoURI;

/**
 * Unit tests for {@link SimpleMongoDbFactory}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleMongoDbFactoryUnitTests {

	@Mock
	Mongo mongo;

	/**
	 * @see DATADOC-254
	 */
	@Test
	public void rejectsIllegalDatabaseNames() {
		rejectsDatabaseName("foo.bar");
		rejectsDatabaseName("foo!bar");
	}

	/**
	 * @see DATADOC-254
	 */
	@Test
	public void allowsDatabaseNames() {
		new SimpleMongoDbFactory(mongo, "foo-bar");
		new SimpleMongoDbFactory(mongo, "foo_bar");
		new SimpleMongoDbFactory(mongo, "foo01231bar");
	}

	/**
	 * @see DATADOC-295
	 * @throws UnknownHostException
	 */
	@Test
	public void mongoUriConstructor() throws UnknownHostException {

		MongoURI mongoURI = new MongoURI("mongodb://myUsername:myPassword@localhost/myDatabase.myCollection");
		MongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoURI);

		assertThat(ReflectionTestUtils.getField(mongoDbFactory, "username").toString(), is("myUsername"));
		assertThat(ReflectionTestUtils.getField(mongoDbFactory, "password").toString(), is("myPassword"));
		assertThat(ReflectionTestUtils.getField(mongoDbFactory, "databaseName").toString(), is("myDatabase"));
		assertThat(ReflectionTestUtils.getField(mongoDbFactory, "databaseName").toString(), is("myDatabase"));
	}

	private void rejectsDatabaseName(String databaseName) {

		try {
			new SimpleMongoDbFactory(mongo, databaseName);
			fail("Expected database name " + databaseName + " to be rejected!");
		} catch (IllegalArgumentException ex) {

		}
	}
}
