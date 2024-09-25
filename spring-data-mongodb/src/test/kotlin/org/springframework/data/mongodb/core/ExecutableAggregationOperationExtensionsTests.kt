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
import io.mockk.mockk
import io.mockk.verify
import org.junit.Test

/**
 * @author Sebastien Deleuze
 * @author Mark Paluch
 */
class ExecutableAggregationOperationExtensionsTests {

	val operation = mockk<ExecutableAggregationOperation>(relaxed = true)

	@Test // DATAMONGO-1689
	fun `aggregateAndReturn() with reified type parameter extension should call its Java counterpart`() {

		operation.aggregateAndReturn<First>()
		verify { operation.aggregateAndReturn(First::class.java) }
	}
}
