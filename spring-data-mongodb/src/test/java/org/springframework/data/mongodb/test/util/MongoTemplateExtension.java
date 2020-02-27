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

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Predicate;

import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.model.CamelCaseSplittingFieldNamingStrategy;
import org.springframework.data.mongodb.test.util.MongoExtensions.Termplate;
import org.springframework.data.util.ParsingUtils;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 */
public class MongoTemplateExtension extends MongoClientExtension {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoTemplateExtension.class);
	CamelCaseSplittingFieldNamingStrategy splitter = new CamelCaseSplittingFieldNamingStrategy("-");

	@Override
	public void afterAll(ExtensionContext context) throws Exception {

	}

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		super.beforeAll(context);

		injectFields(context, null, ReflectionUtils::isStatic);
	}

	private void injectFields(ExtensionContext context, Object testInstance, Predicate<Field> predicate) {

		findAnnotatedFields(context.getRequiredTestClass(), Template.class, predicate).forEach(field -> {
			assertValidFieldCandidate(field);
			try {

				makeAccessible(field).set(testInstance,
						getMongoTemplate(field.getType(), field.getAnnotation(Template.class), context));
			} catch (Throwable t) {
				ExceptionUtils.throwAsUncheckedException(t);
			}
		});
	}

	private void assertValidFieldCandidate(Field field) {

		assertSupportedType("field", field.getType());
		if (isPrivate(field)) {
			throw new ExtensionConfigurationException("@MongoClient field [" + field + "] must not be private.");
		}
	}

	private void assertSupportedType(String target, Class<?> type) {

		if (!ClassUtils.isAssignable(org.springframework.data.mongodb.core.MongoOperations.class, type)
				&& !ClassUtils.isAssignable(org.springframework.data.mongodb.core.ReactiveMongoOperations.class, type)) {
			throw new ExtensionConfigurationException("Can only resolve @MongoClient " + target + " of type "
					+ org.springframework.data.mongodb.core.MongoOperations.class.getName() + " or "
					+ org.springframework.data.mongodb.core.ReactiveMongoOperations.class.getName() + " but was: "
					+ type.getName());
		}
	}

	private Object getMongoTemplate(Class<?> type, Template options, ExtensionContext extensionContext) {

		Store templateStore = extensionContext.getStore(MongoExtensions.Termplate.NAMESPACE);

		boolean replSetClient = holdsReplSetClient(extensionContext) || options.replicaSet();

		String dbName = StringUtils.isNotBlank(options.database()) ? options.database()
				: extensionContext.getTestClass().map(it -> {
					List<String> target = ParsingUtils.splitCamelCaseToLower(it.getSimpleName());
					return org.springframework.util.StringUtils.collectionToDelimitedString(target, "-");
				}).orElse("database");

		if (ClassUtils.isAssignable(org.springframework.data.mongodb.core.MongoOperations.class, type)) {

			String key = Termplate.SYNC + "-" + dbName;
			return templateStore.getOrComputeIfAbsent(key, it -> {

				com.mongodb.client.MongoClient client = (com.mongodb.client.MongoClient) getMongoClient(
						com.mongodb.client.MongoClient.class, extensionContext, replSetClient);
				return new MongoTestTemplate(client, dbName, options.initialEntitySet());
			});
		}

		if (ClassUtils.isAssignable(org.springframework.data.mongodb.core.ReactiveMongoOperations.class, type)) {

			String key = Termplate.REACTIVE + "-" + dbName;
			return templateStore.getOrComputeIfAbsent(key, it -> {

				com.mongodb.reactivestreams.client.MongoClient client = (com.mongodb.reactivestreams.client.MongoClient) getMongoClient(
						com.mongodb.reactivestreams.client.MongoClient.class, extensionContext, replSetClient);
				return new ReactiveMongoTestTemplate(client, dbName, options.initialEntitySet());
			});
		}

		throw new IllegalStateException("Damn - something went wrong.");
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return super.supportsParameter(parameterContext, extensionContext) || parameterContext.isAnnotated(Template.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		if (parameterContext.getParameter().getAnnotation(Template.class) == null) {
			return super.resolveParameter(parameterContext, extensionContext);
		}
		Class<?> parameterType = parameterContext.getParameter().getType();
		return getMongoTemplate(parameterType, parameterContext.getParameter().getAnnotation(Template.class),
				extensionContext);
	}
}
