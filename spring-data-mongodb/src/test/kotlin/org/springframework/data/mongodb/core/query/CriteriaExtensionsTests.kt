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

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Answers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.MockitoJUnitRunner

/**
 * @author Sebastien Deleuze
 * @author Tjeu Kayim
 */
@RunWith(MockitoJUnitRunner::class)
class CriteriaExtensionsTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var criteria: Criteria

	@Test
	fun `isEqualTo() extension should call its Java counterpart`() {

		val foo = "foo"

		criteria.isEqualTo(foo)

		Mockito.verify(criteria, Mockito.times(1)).`is`(foo)
	}

	@Test
	fun `isEqualTo() extension should support nullable value`() {

		criteria.isEqualTo(null)

		Mockito.verify(criteria, Mockito.times(1)).`is`(null)
	}

	@Test
	fun `inValues(varags) extension should call its Java counterpart`() {

		val foo = "foo"
		val bar = "bar"

		criteria.inValues(foo, bar)

		Mockito.verify(criteria, Mockito.times(1)).`in`(foo, bar)
	}

	@Test
	fun `inValues(varags) extension should support nullable values`() {

		criteria.inValues(null, null)

		Mockito.verify(criteria, Mockito.times(1)).`in`(null, null)
	}

	@Test
	fun `inValues(Collection) extension should call its Java counterpart`() {

		val c = listOf("foo", "bar")

		criteria.inValues(c)

		Mockito.verify(criteria, Mockito.times(1)).`in`(c)
	}

	@Test
	fun `inValues(Collection) extension should support nullable values`() {

		val c = listOf("foo", null, "bar")

		criteria.inValues(c)

		Mockito.verify(criteria, Mockito.times(1)).`in`(c)
	}

	@Test
	fun `and(KProperty) extension should call its Java counterpart`() {

		criteria.and(Book::title)

		Mockito.verify(criteria, Mockito.times(1)).and("title")
	}

	@Test
	fun `and(KProperty) extension should support nested properties`() {

		criteria.and(Book::author / Author::name)

		Mockito.verify(criteria, Mockito.times(1)).and("author.name")
	}

	@Test
	fun `where(KProperty) should equal Criteria where()`() {

		class Book(val title: String)

		val typedCriteria = where(Book::title)
		val classicCriteria = Criteria.where("title")

		assertThat(typedCriteria).isEqualTo(classicCriteria)
	}

	@Test
	fun `where(KProperty) should support nested properties`() {

		val typedCriteria = where(Book::author / Author::name)
		val classicCriteria = Criteria.where("author.name")

		assertThat(typedCriteria).isEqualTo(classicCriteria)
	}

	class Book(val title: String, val author: Author)
	class Author(val name: String)
}
