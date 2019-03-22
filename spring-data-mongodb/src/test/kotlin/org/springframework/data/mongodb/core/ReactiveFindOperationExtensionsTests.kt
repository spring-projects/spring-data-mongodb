/*
 * Copyright 2017-2019 the original author or authors.
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
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.assertEquals
import org.junit.Test
import reactor.core.publisher.Mono

/**
 * @author Mark Paluch
 * @author Sebastien Deleuze
 */
class ReactiveFindOperationExtensionsTests {

	val operation = mockk<ReactiveFindOperation>(relaxed = true)

	val operationWithProjection = mockk<ReactiveFindOperation.FindWithProjection<First>>(relaxed = true)

	val distinctWithProjection = mockk<ReactiveFindOperation.DistinctWithProjection>(relaxed = true)

	@Test // DATAMONGO-1719
	fun `ReactiveFind#query(KClass) extension should call its Java counterpart`() {

		operation.query(First::class)
		verify { operation.query(First::class.java) }
	}

	@Test // DATAMONGO-1719
	fun `ReactiveFind#query() with reified type parameter extension should call its Java counterpart`() {

		operation.query<First>()
		verify { operation.query(First::class.java) }
	}

	@Test // DATAMONGO-1719, DATAMONGO-2086
	fun `ReactiveFind#FindOperatorWithProjection#asType(KClass) extension should call its Java counterpart`() {

		operationWithProjection.asType(User::class)
		verify { operationWithProjection.`as`(User::class.java) }
	}

	@Test // DATAMONGO-1719, DATAMONGO-2086
	fun `ReactiveFind#FindOperatorWithProjection#asType() with reified type parameter extension should call its Java counterpart`() {

		operationWithProjection.asType<User>()
		verify { operationWithProjection.`as`(User::class.java) }
	}

	@Test // DATAMONGO-1761, DATAMONGO-2086
	fun `ReactiveFind#DistinctWithProjection#asType(KClass) extension should call its Java counterpart`() {

		distinctWithProjection.asType(User::class)
		verify { distinctWithProjection.`as`(User::class.java) }
	}

	@Test // DATAMONGO-2086
	fun `ReactiveFind#DistinctWithProjection#asType() with reified type parameter extension should call its Java counterpart`() {

		distinctWithProjection.asType<User>()
		verify { distinctWithProjection.`as`(User::class.java) }
	}

	@Test // DATAMONGO-2209
	fun terminatingFindAwaitOne() {

		val find = mockk<ReactiveFindOperation.TerminatingFind<String>>()
		every { find.one() } returns Mono.just("foo")

		runBlocking {
			assertThat(find.awaitOne()).isEqualTo("foo")
		}

		verify {
			find.one()
		}
	}

	@Test // DATAMONGO-2209
	fun terminatingFindAwaitFirst() {

		val find = mockk<ReactiveFindOperation.TerminatingFind<String>>()
		every { find.first() } returns Mono.just("foo")

		runBlocking {
			assertThat(find.awaitFirst()).isEqualTo("foo")
		}

		verify {
			find.first()
		}
	}

	@Test // DATAMONGO-2209
	fun terminatingFindAwaitCount() {

		val find = mockk<ReactiveFindOperation.TerminatingFind<String>>()
		every { find.count() } returns Mono.just(1)

		runBlocking {
			assertThat(find.awaitCount()).isEqualTo(1)
		}

		verify {
			find.count()
		}
	}

	@Test // DATAMONGO-2209
	fun terminatingFindAwaitExists() {

		val find = mockk<ReactiveFindOperation.TerminatingFind<String>>()
		every { find.exists() } returns Mono.just(true)

		runBlocking {
			assertThat(find.awaitExists()).isTrue()
		}

		verify {
			find.exists()
		}
	}
}
