/*
 * Copyright 2018 the original author or authors.
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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.mongodb.core.MongoTemplate.SessionAwareMethodInterceptor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.session.ClientSession;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class SessionAwareMethodInterceptorUnitTests {

	@Mock ClientSession session;
	@Mock MongoCollection<Document> targetCollection;
	@Mock MongoDatabase targetDatabase;

	MongoCollection collection;
	MongoDatabase database;

	@Before
	public void setUp() {

		ProxyFactory collectionProxyFactory = new ProxyFactory();
		collectionProxyFactory.setTarget(targetCollection);
		collectionProxyFactory.setInterfaces(MongoCollection.class);
		collectionProxyFactory.setOpaque(true);
		collectionProxyFactory.addAdvice(new SessionAwareMethodInterceptor(session, targetCollection, MongoCollection.class));

		collection = (MongoCollection) collectionProxyFactory.getProxy();

		ProxyFactory databaseProxyFactory = new ProxyFactory();
		databaseProxyFactory.setTarget(targetDatabase);
		databaseProxyFactory.setInterfaces(MongoDatabase.class);
		databaseProxyFactory.setOpaque(true);
		databaseProxyFactory.addAdvice(new SessionAwareMethodInterceptor(session, targetDatabase, MongoDatabase.class));

		database = (MongoDatabase) databaseProxyFactory.getProxy();
	}

	@Test // DATAMONGO-1880
	public void proxyFactoryOnCollectionDelegatesToMethodWithSession() {

		collection.find();

		verify(targetCollection).find(eq(session));
	}

	@Test // DATAMONGO-1880
	public void proxyFactoryOnCollectionWithSessionInArgumentListProceedsWithExecution() {

		ClientSession yetAnotherSession = mock(ClientSession.class);
		collection.find(yetAnotherSession);

		verify(targetCollection).find(eq(yetAnotherSession));
	}

	@Test // DATAMONGO-1880
	public void proxyFactoryOnDatabaseDelegatesToMethodWithSession() {

		database.drop();

		verify(targetDatabase).drop(eq(session));
	}

	@Test // DATAMONGO-1880
	public void proxyFactoryOnDatabaseWithSessionInArgumentListProceedsWithExecution() {

		ClientSession yetAnotherSession = mock(ClientSession.class);
		database.drop(yetAnotherSession);

		verify(targetDatabase).drop(eq(yetAnotherSession));
	}

}
