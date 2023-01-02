/*
 * Copyright 2018-2023 the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Unit tests for {@link SimpleReactiveMongoDatabaseFactory}.
 *
 * @author Mark Paluch
 * @author Mathieu Ouellet
 */
@ExtendWith(MockitoExtension.class)
class SimpleReactiveMongoDatabaseFactoryUnitTests {

	@Mock MongoClient mongoClient;
	@Mock ClientSession clientSession;
	@Mock MongoDatabase database;

	@Test // DATAMONGO-1880
	void cascadedWithSessionUsesRootFactory() {

		when(mongoClient.getDatabase("foo")).thenReturn(database);

		ReactiveMongoDatabaseFactory factory = new SimpleReactiveMongoDatabaseFactory(mongoClient, "foo");
		ReactiveMongoDatabaseFactory wrapped = factory.withSession(clientSession).withSession(clientSession);

		InvocationHandler invocationHandler = Proxy.getInvocationHandler(wrapped.getMongoDatabase().block());

		Object singletonTarget = AopProxyUtils
				.getSingletonTarget(ReflectionTestUtils.getField(invocationHandler, "advised"));

		assertThat(singletonTarget).isSameAs(database);
	}

	@Test // DATAMONGO-1903
	void rejectsIllegalDatabaseNames() {

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
