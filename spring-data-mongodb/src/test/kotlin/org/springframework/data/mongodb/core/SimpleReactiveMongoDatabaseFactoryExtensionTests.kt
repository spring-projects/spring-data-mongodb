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

package org.springframework.data.mongodb.core

import com.mongodb.kotlin.client.coroutine.MongoClient
import org.bson.Document
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

/**
 * @author Christoph Strobl
 */
class SimpleReactiveMongoDatabaseFactoryExtensionTests {

	@Test // GH-4393
	fun `extension allows to create SimpleReactiveMongoDatabaseFactory with a Kotlin Coroutine Driver instance`() {

		val factory = SimpleReactiveMongoDatabaseFactory(MongoClient.create(), "test")

		factory.mongoDatabase.flatMap { Mono.from(it.runCommand(Document("ping", 1))) }
			.`as` { StepVerifier.create(it) }
			.expectNextCount(1)
			.verifyComplete()
	}
}
