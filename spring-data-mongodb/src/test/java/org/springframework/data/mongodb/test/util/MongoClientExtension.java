/*
 * Copyright 2020 the original author or authors.
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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.function.Predicate;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 */
public class MongoClientExtension implements Extension, BeforeAllCallback, AfterAllCallback, ParameterResolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoClientExtension.class);

	private static final Namespace NAMESPACE = Namespace.create(MongoClientExtension.class);
	private static final String SYNC_KEY = "mongo.client.sync";
	private static final String REACTIVE_KEY = "mongo.client.reactive";
	private static final String SYNC_REPLSET_KEY = "mongo.client.replset.sync";
	private static final String REACTIVE_REPLSET_KEY = "mongo.client.replset.reactive";

	@Override
	public void afterAll(ExtensionContext extensionContext) throws Exception {
		closeClients(extensionContext);
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

	private Object getMongoClient(Class<?> type, ExtensionContext extensionContext, boolean replSet) {

		Store store = extensionContext.getStore(NAMESPACE);

		if (ClassUtils.isAssignable(com.mongodb.client.MongoClient.class, type)) {

			LOGGER.debug("Obtaining sync client from store.");
			return store.getOrComputeIfAbsent(replSet ? SYNC_REPLSET_KEY : SYNC_KEY, it -> syncClient(replSet),
					com.mongodb.client.MongoClient.class);
		}

		if (ClassUtils.isAssignable(com.mongodb.reactivestreams.client.MongoClient.class, type)) {

			LOGGER.debug("Obtaining reactive client from store.");
			return store.getOrComputeIfAbsent(replSet ? REACTIVE_REPLSET_KEY : REACTIVE_KEY, key -> reactiveClient(replSet),
					com.mongodb.reactivestreams.client.MongoClient.class);
		}

		throw new IllegalStateException("Damn - something went wrong.");
	}

	private com.mongodb.reactivestreams.client.MongoClient reactiveClient(boolean replSet) {

		LOGGER.debug("Creating new reactive {}client.", replSet ? "replica set " : "");
		return replSet ? MongoTestUtils.reactiveReplSetClient() : MongoTestUtils.reactiveClient();
	}

	private com.mongodb.client.MongoClient syncClient(boolean replSet) {

		LOGGER.debug("Creating new sync {}client.", replSet ? "replica set " : "");
		return replSet ? MongoTestUtils.replSetClient() : MongoTestUtils.client();
	}

	private void assertValidFieldCandidate(Field field) {

		assertSupportedType("field", field.getType());
		if (isPrivate(field)) {
			throw new ExtensionConfigurationException("@MongoClient field [" + field + "] must not be private.");
		}
	}

	private void assertSupportedType(String target, Class<?> type) {

		if (type != com.mongodb.client.MongoClient.class && type != com.mongodb.reactivestreams.client.MongoClient.class) {
			throw new ExtensionConfigurationException("Can only resolve @MongoClient " + target + " of type "
					+ com.mongodb.client.MongoClient.class.getName() + " or "
					+ com.mongodb.reactivestreams.client.MongoClient.class.getName() + " but was: " + type.getName());
		}
	}

	private void closeClients(ExtensionContext extensionContext) {

		Store store = extensionContext.getStore(NAMESPACE);

		closeClient(store, SYNC_KEY);
		closeClient(store, SYNC_REPLSET_KEY);
		closeClient(store, REACTIVE_KEY);
		closeClient(store, REACTIVE_REPLSET_KEY);
	}

	private void closeClient(Store store, String key) {

		Closeable client = store.remove(key, Closeable.class);
		if (client != null) {

			LOGGER.debug("Closing {} {}client.", key.contains("reactive") ? "reactive" : "sync",
					key.contains("replset") ? "replset " : "");

			try {
				client.close();
			} catch (IOException e) {
				// so what?
			}
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
}
