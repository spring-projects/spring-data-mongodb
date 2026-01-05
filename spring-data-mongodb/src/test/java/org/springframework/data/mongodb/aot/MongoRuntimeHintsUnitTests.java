/*
 * Copyright 2024-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.aot.hint.MemberCategory.*;
import static org.springframework.aot.hint.predicate.RuntimeHintsPredicates.*;

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.mongodb.test.util.ClassPathExclusions;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.MapReducePublisher;

/**
 * Unit Tests for {@link MongoRuntimeHints}.
 *
 * @author Christoph Strobl
 */
@SuppressWarnings("deprecation")
class MongoRuntimeHintsUnitTests {

	@Test // GH-4578
	@ClassPathExclusions(packages = { "com.mongodb.client", "com.mongodb.reactivestreams.client" })
	void shouldRegisterGeneralCompatibilityHints() {

		RuntimeHints runtimeHints = new RuntimeHints();

		new MongoRuntimeHints().registerHints(runtimeHints, this.getClass().getClassLoader());

		Predicate<RuntimeHints> expected = reflection().onType(MongoClientSettings.class)
				.withMemberCategory(INVOKE_PUBLIC_METHODS)
				.and(reflection().onType(MongoClientSettings.Builder.class).withMemberCategory(INVOKE_PUBLIC_METHODS))
				.and(reflection().onType(IndexOptions.class).withMemberCategory(INVOKE_PUBLIC_METHODS))
				.and(reflection().onType(ServerAddress.class).withMemberCategory(INVOKE_PUBLIC_METHODS))
				.and(reflection().onType(UnixServerAddress.class).withMemberCategory(INVOKE_PUBLIC_METHODS))
				.and(reflection().onType(TypeReference.of("com.mongodb.connection.StreamFactoryFactory"))
						.withMemberCategory(INTROSPECT_PUBLIC_METHODS));

		assertThat(runtimeHints).matches(expected);
	}

	@Test // GH-4578
	@ClassPathExclusions(packages = { "com.mongodb.reactivestreams.client" })
	void shouldRegisterSyncCompatibilityHintsIfPresent() {

		RuntimeHints runtimeHints = new RuntimeHints();

		new MongoRuntimeHints().registerHints(runtimeHints, this.getClass().getClassLoader());

		Predicate<RuntimeHints> expected = reflection().onType(MapReduceIterable.class)
				.withMemberCategory(INVOKE_PUBLIC_METHODS)
				.and(reflection().onType(TypeReference.of("com.mongodb.client.internal.MapReduceIterableImpl"))
						.withMemberCategory(INVOKE_PUBLIC_METHODS));

		assertThat(runtimeHints).matches(expected);
	}

	@Test // GH-4578
	@ClassPathExclusions(packages = { "com.mongodb.client" })
	void shouldNotRegisterSyncCompatibilityHintsIfClientNotPresent() {

		RuntimeHints runtimeHints = new RuntimeHints();

		new MongoRuntimeHints().registerHints(runtimeHints, this.getClass().getClassLoader());

		Predicate<RuntimeHints> expected = reflection().onType(TypeReference.of("com.mongodb.client.MapReduceIterable"))
				.withMemberCategory(INVOKE_PUBLIC_METHODS).negate()
				.and(reflection().onType(TypeReference.of("com.mongodb.client.internal.MapReduceIterableImpl"))
						.withMemberCategory(INVOKE_PUBLIC_METHODS).negate());

		assertThat(runtimeHints).matches(expected);
	}

	@Test // GH-4578
	@ClassPathExclusions(packages = { "com.mongodb.client" })
	void shouldRegisterReactiveCompatibilityHintsIfPresent() {

		RuntimeHints runtimeHints = new RuntimeHints();

		new MongoRuntimeHints().registerHints(runtimeHints, this.getClass().getClassLoader());

		Predicate<RuntimeHints> expected = reflection().onType(MapReducePublisher.class)
				.withMemberCategory(INVOKE_PUBLIC_METHODS)
				.and(reflection().onType(TypeReference.of("com.mongodb.reactivestreams.client.internal.MapReducePublisherImpl"))
						.withMemberCategory(INVOKE_PUBLIC_METHODS));

		assertThat(runtimeHints).matches(expected);
	}

	@Test // GH-4578
	@ClassPathExclusions(packages = { "com.mongodb.reactivestreams.client" })
	void shouldNotRegisterReactiveCompatibilityHintsIfClientNotPresent() {

		RuntimeHints runtimeHints = new RuntimeHints();

		new MongoRuntimeHints().registerHints(runtimeHints, this.getClass().getClassLoader());

		Predicate<RuntimeHints> expected = reflection()
				.onType(TypeReference.of("com.mongodb.reactivestreams.client.MapReducePublisher"))
				.withMemberCategory(INVOKE_PUBLIC_METHODS).negate()
				.and(reflection().onType(TypeReference.of("com.mongodb.reactivestreams.client.internal.MapReducePublisherImpl"))
						.withMemberCategory(INVOKE_PUBLIC_METHODS).negate());

		assertThat(runtimeHints).matches(expected);
	}

}
