/*
 * Copyright 2011-2019 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ConnectionString;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Unit tests for {@link SimpleMongoClientDbFactory}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleMongoClientDbFactoryUnitTests {

	@Mock MongoClient mongo;
	@Mock ClientSession clientSession;
	@Mock MongoDatabase database;

	@Test // DATADOC-254, DATAMONGO-1903
	public void rejectsIllegalDatabaseNames() {

		rejectsDatabaseName("foo.bar");
		rejectsDatabaseName("foo$bar");
		rejectsDatabaseName("foo\\bar");
		rejectsDatabaseName("foo//bar");
		rejectsDatabaseName("foo bar");
		rejectsDatabaseName("foo\"bar");
	}

	@Test // DATADOC-254
	@SuppressWarnings("deprecation")
	public void allowsDatabaseNames() {
		new SimpleMongoClientDbFactory(mongo, "foo-bar");
		new SimpleMongoClientDbFactory(mongo, "foo_bar");
		new SimpleMongoClientDbFactory(mongo, "foo01231bar");
	}

	@Test // DATADOC-295
	@SuppressWarnings("deprecation")
	public void mongoUriConstructor() {

		ConnectionString mongoURI = new ConnectionString(
				"mongodb://myUsername:myPassword@localhost/myDatabase.myCollection");
		MongoDbFactory mongoDbFactory = new SimpleMongoClientDbFactory(mongoURI);

		assertThat(getField(mongoDbFactory, "databaseName").toString()).isEqualTo("myDatabase");
	}

	@Test // DATAMONGO-1158
	public void constructsMongoClientAccordingToMongoUri() {

		ConnectionString uri = new ConnectionString(
				"mongodb://myUserName:myPassWord@127.0.0.1:27017/myDataBase.myCollection");
		SimpleMongoClientDbFactory factory = new SimpleMongoClientDbFactory(uri);

		assertThat(getField(factory, "databaseName").toString()).isEqualTo("myDataBase");
	}

	@Test // DATAMONGO-1880
	public void cascadedWithSessionUsesRootFactory() {

		when(mongo.getDatabase("foo")).thenReturn(database);

		MongoDbFactory factory = new SimpleMongoClientDbFactory(mongo, "foo");
		MongoDbFactory wrapped = factory.withSession(clientSession).withSession(clientSession);

		InvocationHandler invocationHandler = Proxy.getInvocationHandler(wrapped.getMongoDatabase());

		Object singletonTarget = AopProxyUtils
				.getSingletonTarget(ReflectionTestUtils.getField(invocationHandler, "advised"));

		assertThat(singletonTarget).isSameAs(database);
	}

	private void rejectsDatabaseName(String databaseName) {
		assertThatThrownBy(() -> new SimpleMongoClientDbFactory(mongo, databaseName))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
