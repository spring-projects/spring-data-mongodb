/*
 * Copyright 2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;
import org.springframework.data.mongodb.classloading.HidingClassLoader;
import org.springframework.data.mongodb.repository.support.CrudMethodMetadata;
import org.springframework.data.mongodb.repository.support.QuerydslMongoPredicateExecutor;
import org.springframework.data.mongodb.repository.support.ReactiveQuerydslMongoPredicateExecutor;

import com.mongodb.client.MongoClient;

/**
 * Unit tests for {@link RepositoryRuntimeHints}.
 *
 * @author Christoph Strobl
 */
class RepositoryRuntimeHintsUnitTests {

	@Test // GH-4244
	void registersTypesForQuerydslIntegration() {

		RuntimeHints runtimeHints = new RuntimeHints();
		new RepositoryRuntimeHints().registerHints(runtimeHints, null);

		assertThat(runtimeHints).matches(RuntimeHintsPredicates.reflection().onType(QuerydslMongoPredicateExecutor.class)
				.and(RuntimeHintsPredicates.reflection().onType(ReactiveQuerydslMongoPredicateExecutor.class)));
	}

	@Test // GH-4244
	void onlyRegistersReactiveTypesForQuerydslIntegrationWhenNoSyncClientPresent() {

		RuntimeHints runtimeHints = new RuntimeHints();
		new RepositoryRuntimeHints().registerHints(runtimeHints, HidingClassLoader.hide(MongoClient.class));

		assertThat(runtimeHints).matches(RuntimeHintsPredicates.reflection().onType(QuerydslMongoPredicateExecutor.class)
				.negate().and(RuntimeHintsPredicates.reflection().onType(ReactiveQuerydslMongoPredicateExecutor.class)));
	}

	@Test // GH-4244
	@Disabled("TODO: ReactiveWrappers does not support ClassLoader")
	void doesNotRegistersReactiveTypesForQuerydslIntegrationWhenReactorNotPresent() {

		RuntimeHints runtimeHints = new RuntimeHints();
		new RepositoryRuntimeHints().registerHints(runtimeHints, new HidingClassLoader("reactor.core"));

		assertThat(runtimeHints).matches(RuntimeHintsPredicates.reflection().onType(QuerydslMongoPredicateExecutor.class)
				.and(RuntimeHintsPredicates.reflection().onType(ReactiveQuerydslMongoPredicateExecutor.class).negate()));
	}

	@Test // GH-2971, GH-4534
	void registersProxyForCrudMethodMetadata() {

		RuntimeHints runtimeHints = new RuntimeHints();
		new RepositoryRuntimeHints().registerHints(runtimeHints, null);

		assertThat(runtimeHints).matches(RuntimeHintsPredicates.proxies().forInterfaces(CrudMethodMetadata.class, //
				org.springframework.aop.SpringProxy.class, //
				org.springframework.aop.framework.Advised.class, //
				org.springframework.core.DecoratingProxy.class));
	}
}
