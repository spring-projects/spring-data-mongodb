/*
 * Copyright 2011-present the original author or authors.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ConnectionString;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Unit tests for {@link SimpleMongoClientDatabaseFactory}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class SimpleMongoClientDatabaseFactoryUnitTests {

	@Mock MongoClient mongo;
	@Mock ClientSession clientSession;
	@Mock MongoDatabase database;

	@Test // DATADOC-254, DATAMONGO-1903
	void rejectsIllegalDatabaseNames() {

		rejectsDatabaseName("foo.bar");
		rejectsDatabaseName("foo$bar");
		rejectsDatabaseName("foo\\bar");
		rejectsDatabaseName("foo//bar");
		rejectsDatabaseName("foo bar");
		rejectsDatabaseName("foo\"bar");
	}

	@Test // DATADOC-254
	void allowsDatabaseNames() {
		new SimpleMongoClientDatabaseFactory(mongo, "foo-bar");
		new SimpleMongoClientDatabaseFactory(mongo, "foo_bar");
		new SimpleMongoClientDatabaseFactory(mongo, "foo01231bar");
	}

	@Test // DATADOC-295
	void mongoUriConstructor() {

		ConnectionString mongoURI = new ConnectionString(
				"mongodb://myUsername:myPassword@localhost/myDatabase.myCollection");
		SimpleMongoClientDatabaseFactory mongoDbFactory = MongoDatabaseFactory.create(mongoURI);

		assertThat(mongoDbFactory).hasFieldOrPropertyWithValue("databaseName", "myDatabase");
	}

	@Test // DATAMONGO-1158
	void constructsMongoClientAccordingToMongoUri() {

		ConnectionString uri = new ConnectionString(
				"mongodb://myUserName:myPassWord@127.0.0.1:27017/myDataBase.myCollection");
		SimpleMongoClientDatabaseFactory factory = MongoDatabaseFactory.create(uri);

		assertThat(factory).hasFieldOrPropertyWithValue("databaseName", "myDataBase");
	}

	@Test // DATAMONGO-1880
	void cascadedWithSessionUsesRootFactory() {

		when(mongo.getDatabase("foo")).thenReturn(database);

		MongoDatabaseFactory factory = MongoDatabaseFactory.create(mongo, "foo");
		MongoDatabaseFactory wrapped = factory.withSession(clientSession).withSession(clientSession);

		InvocationHandler invocationHandler = Proxy.getInvocationHandler(wrapped.getMongoDatabase());

		Object singletonTarget = AopProxyUtils
				.getSingletonTarget(ReflectionTestUtils.getField(invocationHandler, "advised"));

		assertThat(singletonTarget).isSameAs(database);
	}

	@Test // GH-5087
	void passesOnClientSessionWhenInvokingMethodsOnMongoCluster() {

		MongoDatabaseFactory factory = MongoDatabaseFactory.create(mongo, "foo");
		MongoDatabaseFactory wrapped = factory.withSession(clientSession);

		assertThat(wrapped.getCluster()).isInstanceOf(Proxy.class);
		wrapped.getCluster().bulkWrite(List.of());

		verify(mongo).bulkWrite(eq(clientSession), any());
	}

	private void rejectsDatabaseName(String databaseName) {
		assertThatThrownBy(() -> new SimpleMongoClientDatabaseFactory(mongo, databaseName))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
