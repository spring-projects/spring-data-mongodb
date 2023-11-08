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

import static org.springframework.data.mongodb.aot.MongoAotPredicates.*;

import java.util.Arrays;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAfterConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

/**
 * {@link RuntimeHintsRegistrar} for repository types and entity callbacks.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.0
 */
class MongoRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		hints.reflection().registerTypes(
				Arrays.asList(TypeReference.of(BeforeConvertCallback.class), TypeReference.of(BeforeSaveCallback.class),
						TypeReference.of(AfterConvertCallback.class), TypeReference.of(AfterSaveCallback.class)),
				builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
						MemberCategory.INVOKE_PUBLIC_METHODS));

		registerTransactionProxyHints(hints, classLoader);

		if (isReactorPresent()) {

			hints.reflection()
					.registerTypes(Arrays.asList(TypeReference.of(ReactiveBeforeConvertCallback.class),
							TypeReference.of(ReactiveBeforeSaveCallback.class), TypeReference.of(ReactiveAfterConvertCallback.class),
							TypeReference.of(ReactiveAfterSaveCallback.class)),
							builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
									MemberCategory.INVOKE_PUBLIC_METHODS));
		}

	}

	private static void registerTransactionProxyHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		if (MongoAotPredicates.isSyncClientPresent(classLoader)
				&& ClassUtils.isPresent("org.springframework.aop.SpringProxy", classLoader)) {

			hints.proxies().registerJdkProxy(TypeReference.of("com.mongodb.client.MongoDatabase"),
					TypeReference.of("org.springframework.aop.SpringProxy"),
					TypeReference.of("org.springframework.core.DecoratingProxy"));
			hints.proxies().registerJdkProxy(TypeReference.of("com.mongodb.client.MongoCollection"),
					TypeReference.of("org.springframework.aop.SpringProxy"),
					TypeReference.of("org.springframework.core.DecoratingProxy"));
		}
	}

}
