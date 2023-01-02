/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import static org.junit.platform.commons.util.AnnotationUtils.*;
import static org.junit.platform.commons.util.ReflectionUtils.*;

import java.lang.reflect.Field;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;

import org.springframework.util.ClassUtils;

import com.mongodb.client.MongoClient;

/**
 * JUnit {@link Extension} providing parameter resolution for synchronous and reactive MongoDB client instances.
 *
 * @author Christoph Strobl
 * @see Client
 * @see ReplSetClient
 */
public class MongoClientExtension implements Extension, BeforeAllCallback, AfterAllCallback, ParameterResolver {

	private static final Log LOGGER = LogFactory.getLog(MongoClientExtension.class);

	private static final Namespace NAMESPACE = MongoExtensions.Client.NAMESPACE;

	private static final String SYNC_KEY = MongoExtensions.Client.SYNC_KEY;
	private static final String REACTIVE_KEY = MongoExtensions.Client.REACTIVE_KEY;
	private static final String SYNC_REPLSET_KEY = MongoExtensions.Client.SYNC_REPLSET_KEY;
	private static final String REACTIVE_REPLSET_KEY = MongoExtensions.Client.REACTIVE_REPLSET_KEY;

	@Override
	public void afterAll(ExtensionContext extensionContext) throws Exception {

	}

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		injectFields(context, null, ReflectionUtils::isStatic);
	}

	private void injectFields(ExtensionContext context, Object testInstance, Predicate<Field> predicate) {

		findAnnotatedFields(context.getRequiredTestClass(), Client.class, predicate).forEach(field -> {
			assertValidFieldCandidate(field);
			try {
				makeAccessible(field).set(testInstance, getMongoClient(field.getType(), context, false));
			} catch (Throwable t) {
				ExceptionUtils.throwAsUncheckedException(t);
			}
		});

		findAnnotatedFields(context.getRequiredTestClass(), ReplSetClient.class, predicate).forEach(field -> {
			assertValidFieldCandidate(field);
			try {
				makeAccessible(field).set(testInstance, getMongoClient(field.getType(), context, true));
			} catch (Throwable t) {
				ExceptionUtils.throwAsUncheckedException(t);
			}
		});
	}

	protected Object getMongoClient(Class<?> type, ExtensionContext extensionContext, boolean replSet) {

		Store store = extensionContext.getStore(NAMESPACE);

		if (ClassUtils.isAssignable(com.mongodb.client.MongoClient.class, type)) {

			LOGGER.debug("Obtaining sync client from store.");
			return store.getOrComputeIfAbsent(replSet ? SYNC_REPLSET_KEY : SYNC_KEY, it -> syncClient(replSet),
					SyncClientHolder.class).client;
		}

		if (ClassUtils.isAssignable(com.mongodb.reactivestreams.client.MongoClient.class, type)) {

			LOGGER.debug("Obtaining reactive client from store.");
			return store.getOrComputeIfAbsent(replSet ? REACTIVE_REPLSET_KEY : REACTIVE_KEY, key -> reactiveClient(replSet),
					ReactiveClientHolder.class).client;
		}

		throw new IllegalStateException("Damn - something went wrong.");
	}

	private ReactiveClientHolder reactiveClient(boolean replSet) {

		LOGGER.debug(String.format("Creating new reactive %sclient.", replSet ? "replica set " : ""));
		return new ReactiveClientHolder(replSet ? MongoTestUtils.reactiveReplSetClient() : MongoTestUtils.reactiveClient());
	}

	private SyncClientHolder syncClient(boolean replSet) {

		LOGGER.debug(String.format("Creating new sync %sclient.", replSet ? "replica set " : ""));
		return new SyncClientHolder(replSet ? MongoTestUtils.replSetClient() : MongoTestUtils.client());
	}

	boolean holdsReplSetClient(ExtensionContext context) {

		Store store = context.getStore(NAMESPACE);
		return store.get(SYNC_REPLSET_KEY) != null || store.get(REACTIVE_REPLSET_KEY) != null;
	}

	private void assertValidFieldCandidate(Field field) {

		assertSupportedType("field", field.getType());
	}

	private void assertSupportedType(String target, Class<?> type) {

		if (type != com.mongodb.client.MongoClient.class && type != com.mongodb.reactivestreams.client.MongoClient.class) {
			throw new ExtensionConfigurationException(String.format(
					"Can only resolve @MongoClient %s of type %s or %s but was: %s", target, MongoClient.class.getName(),
					com.mongodb.reactivestreams.client.MongoClient.class.getName(), type.getName()));
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return parameterContext.isAnnotated(Client.class) || parameterContext.isAnnotated(ReplSetClient.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		Class<?> parameterType = parameterContext.getParameter().getType();
		boolean replSet = parameterContext.getParameter().getAnnotation(ReplSetClient.class) != null;
		return getMongoClient(parameterType, extensionContext, replSet);
	}

	static class SyncClientHolder implements Store.CloseableResource {

		final MongoClient client;

		SyncClientHolder(MongoClient client) {
			this.client = client;
		}

		@Override
		public void close() {
			try {
				client.close();
			} catch (RuntimeException e) {
				// so what?
			}
		}
	}

	static class ReactiveClientHolder implements Store.CloseableResource {

		final com.mongodb.reactivestreams.client.MongoClient client;

		ReactiveClientHolder(com.mongodb.reactivestreams.client.MongoClient client) {
			this.client = client;
		}

		@Override
		public void close() {

			try {
				client.close();
			} catch (RuntimeException e) {
				// so what?
			}
		}
	}
}
