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
package org.springframework.data.mongodb;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.mongodb.SessionAwareMethodInterceptor.MethodCache;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ClassUtils;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Unit tests for {@link SessionAwareMethodInterceptor}.
 *
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

		collection = createProxyInstance(session, targetCollection, MongoCollection.class);
		database = createProxyInstance(session, targetDatabase, MongoDatabase.class);
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

	@Test // DATAMONGO-1880
	public void justMoveOnIfNoOverloadWithSessionAvailable() {

		collection.getReadPreference();

		verify(targetCollection).getReadPreference();
	}

	@Test // DATAMONGO-1880
	public void usesCacheForMethodLookup() {

		MethodCache cache = (MethodCache) ReflectionTestUtils.getField(SessionAwareMethodInterceptor.class, "METHOD_CACHE");
		Method countMethod = ClassUtils.getMethod(MongoCollection.class, "count");

		assertThat(cache.contains(countMethod, MongoCollection.class)).isFalse();

		collection.count();

		assertThat(cache.contains(countMethod, MongoCollection.class)).isTrue();
	}

	@Test // DATAMONGO-1880
	public void cachesNullForMethodsThatDoNotHaveASessionOverload() {

		MethodCache cache = (MethodCache) ReflectionTestUtils.getField(SessionAwareMethodInterceptor.class, "METHOD_CACHE");
		Method readConcernMethod = ClassUtils.getMethod(MongoCollection.class, "getReadConcern");

		assertThat(cache.contains(readConcernMethod, MongoCollection.class)).isFalse();

		collection.getReadConcern();

		collection.getReadConcern();

		assertThat(cache.contains(readConcernMethod, MongoCollection.class)).isTrue();
		assertThat(cache.lookup(readConcernMethod, MongoCollection.class, ClientSession.class)).isEmpty();
	}

	@Test // DATAMONGO-1880
	public void proxiesNewDbInstanceReturnedByMethod() {

		MongoDatabase otherDb = mock(MongoDatabase.class);
		when(targetDatabase.withCodecRegistry(any())).thenReturn(otherDb);

		MongoDatabase target = database.withCodecRegistry(MongoClient.getDefaultCodecRegistry());
		assertThat(target).isInstanceOf(Proxy.class).isNotSameAs(database).isNotSameAs(targetDatabase);

		target.drop();

		verify(otherDb).drop(eq(session));
	}

	@Test // DATAMONGO-1880
	public void proxiesNewCollectionInstanceReturnedByMethod() {

		MongoCollection otherCollection = mock(MongoCollection.class);
		when(targetCollection.withCodecRegistry(any())).thenReturn(otherCollection);

		MongoCollection target = collection.withCodecRegistry(MongoClient.getDefaultCodecRegistry());
		assertThat(target).isInstanceOf(Proxy.class).isNotSameAs(collection).isNotSameAs(targetCollection);

		target.drop();

		verify(otherCollection).drop(eq(session));
	}

	private MongoDatabase proxyDatabase(com.mongodb.session.ClientSession session, MongoDatabase database) {
		return createProxyInstance(session, database, MongoDatabase.class);
	}

	private MongoCollection proxyCollection(com.mongodb.session.ClientSession session, MongoCollection collection) {
		return createProxyInstance(session, collection, MongoCollection.class);
	}

	private <T> T createProxyInstance(com.mongodb.session.ClientSession session, T target, Class<T> targetType) {

		ProxyFactory factory = new ProxyFactory();
		factory.setTarget(target);
		factory.setInterfaces(targetType);
		factory.setOpaque(true);

		factory.addAdvice(new SessionAwareMethodInterceptor<>(session, target, ClientSession.class, MongoDatabase.class,
				this::proxyDatabase, MongoCollection.class, this::proxyCollection));

		return targetType.cast(factory.getProxy());
	}

}
