/*
 * Copyright 2017-2024 the original author or authors.
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
import reactor.core.publisher.Mono

/**
 * @author Mark Paluch
 * @author Sebastien Deleuze
 */
class ReactiveInsertOperationExtensionsTests {

	val operation = mockk<ReactiveInsertOperation>(relaxed = true)

	@Test // DATAMONGO-1719
	fun `insert() with reified type parameter extension should call its Java counterpart`() {

		operation.insert<First>()
		verify { operation.insert(First::class.java) }
	}

	@Test // DATAMONGO-2209
	fun terminatingInsertOneAndAwait() {

		val insert = mockk<ReactiveInsertOperation.TerminatingInsert<String>>()
		every { insert.one("foo") } returns Mono.just("foo")

		runBlocking {
			assertThat(insert.oneAndAwait("foo")).isEqualTo("foo")
		}

		verify {
			insert.one("foo")
		}
	}

	@Test // DATAMONGO-2255
	fun terminatingInsertAllAsFlow() {

		val insert = mockk<ReactiveInsertOperation.TerminatingInsert<String>>()
		val list = listOf("foo", "bar")
		every { insert.all(any()) } returns Flux.fromIterable(list)

		runBlocking {
			assertThat(insert.flow(list).toList()).containsAll(list)
		}

		verify {
			insert.all(list)
		}
	}
}
