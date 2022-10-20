/*
 * Copyright 2022 the original author or authors.
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

import java.util.Arrays;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.QuerydslUtils;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 4.0
 */
class RepositoryRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		hints.reflection().registerTypes(
				Arrays.asList(TypeReference.of("org.springframework.data.mongodb.repository.support.SimpleMongoRepository")),
				builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
						MemberCategory.INVOKE_PUBLIC_METHODS));

		if (isReactorPresent()) {

			hints.reflection().registerTypes(
					Arrays.asList(
							TypeReference.of("org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository")),
					builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
							MemberCategory.INVOKE_PUBLIC_METHODS));
		}

		if (QuerydslUtils.QUERY_DSL_PRESENT) {

			hints.reflection().registerType(
					TypeReference.of("org.springframework.data.mongodb.repository.support.QuerydslMongoPredicateExecutor"),
					hint -> hint.withMembers(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS, MemberCategory.INVOKE_PUBLIC_METHODS)
							.onReachableType(QuerydslPredicateExecutor.class));
		}
	}
}
