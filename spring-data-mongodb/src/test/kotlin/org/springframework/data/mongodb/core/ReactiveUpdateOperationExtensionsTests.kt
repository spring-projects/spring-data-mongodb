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

import com.mongodb.client.result.UpdateResult
import example.first.First
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Mono

/**
 * Unit tests for `ReactiveExecutableUpdateOperationExtensions.kt`.
 *
 * @author Mark Paluch
 * @author Sebastien Deleuze
 */
class ReactiveUpdateOperationExtensionsTests {

	val operation = mockk<ReactiveUpdateOperation>(relaxed = true)

	@Test // DATAMONGO-1719
	fun `update(KClass) extension should call its Java counterpart`() {

		operation.update(First::class)
		verify { operation.update(First::class.java) }
	}

	@Test // DATAMONGO-1719
	fun `update() with reified type parameter extension should call its Java counterpart`() {

		operation.update<First>()
		verify { operation.update(First::class.java) }
	}

	@Test // DATAMONGO-2209
	fun findModifyAndAwait() {

		val find = mockk<ReactiveUpdateOperation.TerminatingFindAndModify<String>>()
		every { find.findAndModify() } returns Mono.just("foo")

		runBlocking {
			assertThat(find.findModifyAndAwait()).isEqualTo("foo")
		}

		verify {
			find.findAndModify()
		}
	}

	@Test // DATAMONGO-2209
	fun findReplaceAndAwait() {

		val find = mockk<ReactiveUpdateOperation.TerminatingFindAndReplace<String>>()
		every { find.findAndReplace() } returns Mono.just("foo")

		runBlocking {
			assertThat(find.findReplaceAndAwait()).isEqualTo("foo")
		}

		verify {
			find.findAndReplace()
		}
	}

	@Test // DATAMONGO-2209
	fun allAndAwait() {

		val update = mockk<ReactiveUpdateOperation.TerminatingUpdate<String>>()
		val result = mockk<UpdateResult>()
		every { update.all() } returns Mono.just(result)

		runBlocking {
			assertThat(update.allAndAwait()).isEqualTo(result)
		}

		verify {
			update.all()
		}
	}

	@Test // DATAMONGO-2209
	fun firstAndAwait() {

		val update = mockk<ReactiveUpdateOperation.TerminatingUpdate<String>>()
		val result = mockk<UpdateResult>()
		every { update.first() } returns Mono.just(result)

		runBlocking {
			assertThat(update.firstAndAwait()).isEqualTo(result)
		}

		verify {
			update.first()
		}
	}

	@Test // DATAMONGO-2209
	fun upsertAndAwait() {

		val update = mockk<ReactiveUpdateOperation.TerminatingUpdate<String>>()
		val result = mockk<UpdateResult>()
		every { update.upsert() } returns Mono.just(result)

		runBlocking {
			assertThat(update.upsertAndAwait()).isEqualTo(result)
		}

		verify {
			update.upsert()
		}
	}

	@Test // DATAMONGO-2209
	fun findAndReplaceWithProjectionAsType() {

		val update = mockk<ReactiveUpdateOperation.FindAndReplaceWithProjection<String>>()
		val result = mockk<ReactiveUpdateOperation.FindAndReplaceWithOptions<String>>()
		every { update.`as`(String::class.java) } returns result

		assertThat(update.asType<String>()).isEqualTo(result)

		verify {
			update.`as`(String::class.java)
		}
	}
}
