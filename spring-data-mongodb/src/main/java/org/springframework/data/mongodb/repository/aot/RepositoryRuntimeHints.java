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
package org.springframework.data.mongodb.repository.aot;

import static org.springframework.data.mongodb.aot.MongoAotPredicates.*;

import java.util.List;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.mongodb.aot.MongoAotPredicates;
import org.springframework.data.mongodb.repository.support.CrudMethodMetadata;
import org.springframework.data.mongodb.repository.support.QuerydslMongoPredicateExecutor;
import org.springframework.data.mongodb.repository.support.ReactiveQuerydslMongoPredicateExecutor;
import org.springframework.data.querydsl.QuerydslUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @since 4.0
 */
class RepositoryRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		hints.reflection().registerTypes(
				List.of(TypeReference.of("org.springframework.data.mongodb.repository.support.SimpleMongoRepository")),
				builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
						MemberCategory.INVOKE_PUBLIC_METHODS));

		if (isAopPresent(classLoader)) {

			// required for pushing ReadPreference,... into the default repository implementation
			hints.proxies().registerJdkProxy(CrudMethodMetadata.class, //
					org.springframework.aop.SpringProxy.class, //
					org.springframework.aop.framework.Advised.class, //
					org.springframework.core.DecoratingProxy.class);
		}

		if (isReactorPresent()) {

			hints.reflection().registerTypes(
					List.of(
							TypeReference.of("org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository")),
					builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
							MemberCategory.INVOKE_PUBLIC_METHODS));
		}

		if (QuerydslUtils.QUERY_DSL_PRESENT) {
			registerQuerydslHints(hints, classLoader);
		}
	}

	/**
	 * Register hints for Querydsl integration.
	 *
	 * @param hints must not be {@literal null}.
	 * @param classLoader can be {@literal null}.
	 * @since 4.0.2
	 */
	private static void registerQuerydslHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		if (isReactorPresent()) {
			hints.reflection().registerType(ReactiveQuerydslMongoPredicateExecutor.class,
					MemberCategory.INVOKE_PUBLIC_METHODS, MemberCategory.INVOKE_DECLARED_CONSTRUCTORS);

		}

		if (MongoAotPredicates.isSyncClientPresent(classLoader)) {
			hints.reflection().registerType(QuerydslMongoPredicateExecutor.class, MemberCategory.INVOKE_PUBLIC_METHODS,
					MemberCategory.INVOKE_DECLARED_CONSTRUCTORS);
		}
	}

	private static boolean isAopPresent(@Nullable ClassLoader classLoader) {
		return ClassUtils.isPresent("org.springframework.aop.Pointcut", classLoader);
	}
}
