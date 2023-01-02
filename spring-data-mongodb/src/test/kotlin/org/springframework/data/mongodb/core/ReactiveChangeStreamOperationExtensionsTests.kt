/*
 * Copyright 2019-2023 the original author or authors.
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
import org.bson.Document
import org.junit.Test
import reactor.core.publisher.Flux

/**
 * @author Christoph Strobl
 * @soundtrack Rage Against The Machine - Take the Power Back
 */
class ReactiveChangeStreamOperationExtensionsTests {

	val operation = mockk<ReactiveChangeStreamOperation>(relaxed = true)
	val changestream = mockk<ReactiveChangeStreamOperation.ReactiveChangeStream<First>>(relaxed = true)

	@Test // DATAMONGO-2089
	fun `ReactiveChangeStreamOperation#changeStream() with reified type parameter extension should call its Java counterpart`() {

		operation.changeStream<First>()
		verify { operation.changeStream(First::class.java) }
	}

	@Test // DATAMONGO-2089
	fun `TerminatingChangeStream#listen() flow extension`() {

		val doc1 = mockk<ChangeStreamEvent<Document>>()
		val doc2 = mockk<ChangeStreamEvent<Document>>()
		val doc3 = mockk<ChangeStreamEvent<Document>>()

		val spec = mockk<ReactiveChangeStreamOperation.TerminatingChangeStream<Document>>()
		every { spec.listen() } returns Flux.just(doc1, doc2, doc3)

		runBlocking {
			assertThat(spec.flow().toList()).contains(doc1, doc2, doc3)
		}

		verify {
			spec.listen()
		}
	}

	data class Last(val id: String)
}
