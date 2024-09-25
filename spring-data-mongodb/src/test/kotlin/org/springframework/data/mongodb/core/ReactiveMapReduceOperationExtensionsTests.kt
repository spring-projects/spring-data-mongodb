/*
 * Copyright 2018-2024 the original author or authors.
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

import example.first.First
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Flux

/**
 * @author Christoph Strobl
 * @author Sebastien Deleuze
 */
class ReactiveMapReduceOperationExtensionsTests {

	val operation = mockk<ReactiveMapReduceOperation>(relaxed = true)

	val operationWithProjection = mockk<ReactiveMapReduceOperation.MapReduceWithProjection<First>>(relaxed = true)

	@Test // DATAMONGO-1929
	fun `ReactiveMapReduceOperation#mapReduce() with reified type parameter extension should call its Java counterpart`() {

		operation.mapReduce<First>()
		verify { operation.mapReduce(First::class.java) }
	}

	@Test // DATAMONGO-1929, DATAMONGO-2086
	fun `ReactiveMapReduceOperation#MapReduceWithProjection#asType() with reified type parameter extension should call its Java counterpart`() {

		operationWithProjection.asType<User>()
		verify { operationWithProjection.`as`(User::class.java) }
	}

	@Test // DATAMONGO-2255
	fun terminatingMapReduceAllAsFlow() {

		val spec = mockk<ReactiveMapReduceOperation.TerminatingMapReduce<String>>()
		every { spec.all() } returns Flux.just("foo", "bar", "baz")

		runBlocking {
			assertThat(spec.flow().toList()).contains("foo", "bar", "baz")
		}

		verify {
			spec.all()
		}
	}
}
