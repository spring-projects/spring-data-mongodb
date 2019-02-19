/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.query

import io.mockk.mockk
import io.mockk.verify
import org.junit.Test

/**
 * @author Sebastien Deleuze
 */
class CriteriaExtensionsTests {

	val criteria = mockk<Criteria>(relaxed = true)

	@Test
	fun `isEqualTo() extension should call its Java counterpart`() {

		val foo = "foo"

		criteria.isEqualTo(foo)

		verify(exactly = 1) { criteria.`is`(foo) }
	}

	@Test
	fun `isEqualTo() extension should support nullable value`() {

		criteria.isEqualTo(null)

		verify(exactly = 1) { criteria.`is`(null) }
	}

	@Test
	fun `inValues(varags) extension should call its Java counterpart`() {

		val foo = "foo"
		val bar = "bar"

		criteria.inValues(foo, bar)

		verify(exactly = 1) { criteria.`in`(foo, bar) }
	}

	@Test
	fun `inValues(varags) extension should support nullable values`() {

		criteria.inValues(null, null)

		verify(exactly = 1) { criteria.`in`(null, null) }
	}

	@Test
	fun `inValues(Collection) extension should call its Java counterpart`() {

		val c = listOf("foo", "bar")

		criteria.inValues(c)

		verify(exactly = 1) { criteria.`in`(c) }
	}

	@Test
	fun `inValues(Collection) extension should support nullable values`() {

		val c = listOf("foo", null, "bar")

		criteria.inValues(c)

		verify(exactly = 1) { criteria.`in`(c) }
	}
}
