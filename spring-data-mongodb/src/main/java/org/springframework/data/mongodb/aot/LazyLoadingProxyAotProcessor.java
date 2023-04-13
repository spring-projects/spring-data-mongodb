/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.aot;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.TypeReference;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.data.annotation.Reference;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxyFactory;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxyFactory.LazyLoadingInterceptor;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;

/**
 * @author Christoph Strobl
 * @since 4.0
 */
public class LazyLoadingProxyAotProcessor {

	private boolean generalLazyLoadingProxyContributed = false;

	public void registerLazyLoadingProxyIfNeeded(Class<?> type, GenerationContext generationContext) {

		Set<Field> refFields = getFieldsWithAnnotationPresent(type, Reference.class);
		if (refFields.isEmpty()) {
			return;
		}

		refFields.stream() //
				.filter(LazyLoadingProxyAotProcessor::isLazyLoading) //
				.forEach(field -> {

					if (!generalLazyLoadingProxyContributed) {
						generationContext.getRuntimeHints().proxies().registerJdkProxy(
								TypeReference.of(org.springframework.data.mongodb.core.convert.LazyLoadingProxy.class),
								TypeReference.of(org.springframework.aop.SpringProxy.class),
								TypeReference.of(org.springframework.aop.framework.Advised.class),
								TypeReference.of(org.springframework.core.DecoratingProxy.class));
						generalLazyLoadingProxyContributed = true;
					}

					if (field.getType().isInterface()) {

						List<Class<?>> interfaces = new ArrayList<>(
								Arrays.asList(LazyLoadingProxyFactory.prepareFactory(field.getType()).getProxiedInterfaces()));
						interfaces.add(org.springframework.aop.SpringProxy.class);
						interfaces.add(org.springframework.aop.framework.Advised.class);
						interfaces.add(org.springframework.core.DecoratingProxy.class);

						generationContext.getRuntimeHints().proxies().registerJdkProxy(interfaces.toArray(Class[]::new));
					} else {

						Class<?> proxyClass = LazyLoadingProxyFactory.resolveProxyType(field.getType(),
								LazyLoadingInterceptor::none);

						// see: spring-projects/spring-framework/issues/29309
						generationContext.getRuntimeHints().reflection().registerType(proxyClass,
								MemberCategory.INVOKE_DECLARED_CONSTRUCTORS, MemberCategory.INVOKE_DECLARED_METHODS, MemberCategory.DECLARED_FIELDS);
					}
				});
	}

	private static boolean isLazyLoading(Field field) {
		if (AnnotatedElementUtils.isAnnotated(field, DBRef.class)) {
			return AnnotatedElementUtils.findMergedAnnotation(field, DBRef.class).lazy();
		}
		if (AnnotatedElementUtils.isAnnotated(field, DocumentReference.class)) {
			return AnnotatedElementUtils.findMergedAnnotation(field, DocumentReference.class).lazy();
		}
		return false;
	}

	private static Set<Field> getFieldsWithAnnotationPresent(Class<?> type, Class<? extends Annotation> annotation) {

		Set<Field> fields = new LinkedHashSet<>();
		for (Field field : type.getDeclaredFields()) {
			if (MergedAnnotations.from(field).get(annotation).isPresent()) {
				fields.add(field);
			}
		}
		return fields;
	}

}
