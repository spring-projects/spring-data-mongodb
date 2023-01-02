/*
 * Copyright 2018-2023 the original author or authors.
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
import io.mockk.mockk
import io.mockk.verify
import org.junit.Test

/**
 * @author Christoph Strobl
 * @author Sebastien Deleuze
 */
class ExecutableMapReduceOperationExtensionsTests {

	val operation = mockk<ExecutableMapReduceOperation>(relaxed = true)

	val operationWithProjection = mockk<ExecutableMapReduceOperation.MapReduceWithProjection<First>>(relaxed = true)

	@Test // DATAMONGO-1929
	fun `ExecutableMapReduceOperation#mapReduce() with reified type parameter extension should call its Java counterpart`() {

		operation.mapReduce<First>()
		verify { operation.mapReduce(First::class.java) }
	}

	@Test // DATAMONGO-1929, DATAMONGO-2086
	fun `ExecutableMapReduceOperation#MapReduceWithProjection#asType() with reified type parameter extension should call its Java counterpart`() {

		operationWithProjection.asType<User>()
		verify { operationWithProjection.`as`(User::class.java) }
	}
}
