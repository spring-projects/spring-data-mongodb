/*
 * Copyright 2011-2017 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import java.net.UnknownHostException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.MongoDbFactory;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;

/**
 * Unit tests for {@link SimpleMongoDbFactory}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleMongoDbFactoryUnitTests {

	public @Rule ExpectedException expectedException = ExpectedException.none();
	@Mock Mongo mongo;

	@Test // DATADOC-254
	public void rejectsIllegalDatabaseNames() {
		rejectsDatabaseName("foo.bar");
		rejectsDatabaseName("foo!bar");
	}

	@Test // DATADOC-254
	@SuppressWarnings("deprecation")
	public void allowsDatabaseNames() {
		new SimpleMongoDbFactory(mongo, "foo-bar");
		new SimpleMongoDbFactory(mongo, "foo_bar");
		new SimpleMongoDbFactory(mongo, "foo01231bar");
	}

	@Test // DATADOC-295
	@SuppressWarnings("deprecation")
	public void mongoUriConstructor() throws UnknownHostException {

		MongoURI mongoURI = new MongoURI("mongodb://myUsername:myPassword@localhost/myDatabase.myCollection");
		MongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoURI);

		assertThat(getField(mongoDbFactory, "credentials"), is((Object) new UserCredentials("myUsername", "myPassword")));
		assertThat(getField(mongoDbFactory, "databaseName").toString(), is("myDatabase"));
	}

	@Test // DATAMONGO-789
	@SuppressWarnings("deprecation")
	public void defaultsAuthenticationDatabaseToDatabase() {

		SimpleMongoDbFactory factory = new SimpleMongoDbFactory(mongo, "foo");
		assertThat(getField(factory, "authenticationDatabaseName"), is((Object) "foo"));
	}

	@Test // DATAMONGO-1158
	public void constructsMongoClientAccordingToMongoUri() throws UnknownHostException {

		MongoClientURI uri = new MongoClientURI("mongodb://myUserName:myPassWord@127.0.0.1:27017/myDataBase.myCollection");
		SimpleMongoDbFactory factory = new SimpleMongoDbFactory(uri);

		assertThat(getField(factory, "databaseName").toString(), is("myDataBase"));
	}

	@Test // DATAMONGO-1158
	public void shouldDefaultAuthenticationDbNameToDbNameWhenUsingMongoClient() throws UnknownHostException {

		MongoClient clientMock = mock(MongoClient.class);
		SimpleMongoDbFactory factory = new SimpleMongoDbFactory(clientMock, "FooBar");

		assertThat(getField(factory, "authenticationDatabaseName").toString(), is("FooBar"));
	}

	@Test // DATAMONGO-1260
	public void rejectsMongoClientWithUserCredentials() {

		expectedException.expect(InvalidDataAccessApiUsageException.class);
		expectedException.expectMessage("use 'MongoCredential' for 'MongoClient'");

		new SimpleMongoDbFactory(mock(MongoClient.class), "cairhienin", new UserCredentials("moiraine", "sedai"));
	}

	@Test // DATAMONGO-1260
	public void rejectsMongoClientWithUserCredentialsAndAuthDb() {

		expectedException.expect(InvalidDataAccessApiUsageException.class);
		expectedException.expectMessage("use 'MongoCredential' for 'MongoClient'");

		new SimpleMongoDbFactory(mock(MongoClient.class), "malkieri", new UserCredentials("lan", "mandragoran"), "authdb");
	}

	@Test // DATAMONGO-1260
	public void shouldNotRejectMongoClientWithNoCredentials() {
		new SimpleMongoDbFactory(mock(MongoClient.class), "andoran", UserCredentials.NO_CREDENTIALS);
	}

	@Test // DATAMONGO-1260
	public void shouldNotRejectMongoClientWithEmptyUserCredentials() {
		new SimpleMongoDbFactory(mock(MongoClient.class), "shangtai", new UserCredentials("", ""));
	}

	@SuppressWarnings("deprecation")
	private void rejectsDatabaseName(String databaseName) {

		try {
			new SimpleMongoDbFactory(mongo, databaseName);
			fail("Expected database name " + databaseName + " to be rejected!");
		} catch (IllegalArgumentException ex) {

		}
	}
}
