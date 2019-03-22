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

import com.mongodb.client.result.DeleteResult
import example.first.First
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Mono

/**
 * @author Mark Paluch
 * @author Sebastien Deleuze
 */
class ReactiveRemoveOperationExtensionsTests {

	val operation = mockk<ReactiveRemoveOperation>(relaxed = true)

	@Test // DATAMONGO-1719
	fun `remove(KClass) extension should call its Java counterpart`() {

		operation.remove(First::class)
		verify { operation.remove(First::class.java) }
	}

	@Test // DATAMONGO-1719
	fun `remove() with reified type parameter extension should call its Java counterpart`() {

		operation.remove<First>()
		verify { operation.remove(First::class.java) }
	}

	@Test // DATAMONGO-2209
	fun allAndAwait() {

		val remove = mockk<ReactiveRemoveOperation.TerminatingRemove<String>>()
		val result = mockk<DeleteResult>()
		every { remove.all() } returns Mono.just(result)

		runBlocking {
			assertThat(remove.allAndAwait()).isEqualTo(result)
		}

		verify {
			remove.all()
		}
	}
}
