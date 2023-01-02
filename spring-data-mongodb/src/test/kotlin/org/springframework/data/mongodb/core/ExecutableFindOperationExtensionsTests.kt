/*
 * Copyright 2017-2023 the original author or authors.
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
import org.junit.Test

/**
 * @author Sebastien Deleuze
 * @author Mark Paluch
 */
class ExecutableFindOperationExtensionsTests {

	val operation = mockk<ExecutableFindOperation>(relaxed = true)

	val operationWithProjection = mockk<ExecutableFindOperation.FindWithProjection<First>>(relaxed = true)

	val distinctWithProjection = mockk<ExecutableFindOperation.DistinctWithProjection>(relaxed = true)

	val findDistinct = mockk<ExecutableFindOperation.FindDistinct>(relaxed = true)

	val executableFind = mockk<ExecutableFindOperation.ExecutableFind<KotlinUser>>(relaxed = true)

	@Test // DATAMONGO-1689
	fun `ExecutableFindOperation#query() with reified type parameter extension should call its Java counterpart`() {

		operation.query<First>()
		verify { operation.query(First::class.java) }
	}

	@Test // DATAMONGO-1689, DATAMONGO-2086
	fun `ExecutableFindOperation#FindOperationWithProjection#asType() with reified type parameter extension should call its Java counterpart`() {

		operationWithProjection.asType<User>()
		verify { operationWithProjection.`as`(User::class.java) }
	}

	@Test // DATAMONGO-2086
	fun `ExecutableFindOperation#DistinctWithProjection#asType() with reified type parameter extension should call its Java counterpart`() {

		distinctWithProjection.asType<User>()
		verify { distinctWithProjection.`as`(User::class.java) }
	}

	@Test // DATAMONGO-2417
	fun `ExecutableFindOperation#distrinct() using KProperty1 should call its Java counterpart`() {

		every { operation.query(KotlinUser::class.java) } returns executableFind

		operation.distinct(KotlinUser::username)
		verify {
			operation.query(KotlinUser::class.java)
			executableFind.distinct("username")
		}
	}

	@Test // DATAMONGO-2417
	fun `ExecutableFindOperation#FindDistinct#field() using KProperty should call its Java counterpart`() {

		findDistinct.distinct(KotlinUser::username)
		verify { findDistinct.distinct("username") }
	}

	data class KotlinUser(val username: String)
}
