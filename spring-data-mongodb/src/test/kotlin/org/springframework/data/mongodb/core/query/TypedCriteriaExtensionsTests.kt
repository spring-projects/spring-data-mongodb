/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.query

import org.assertj.core.api.Assertions.assertThat
import org.bson.BsonRegularExpression
import org.junit.Test
import org.springframework.data.geo.Circle
import org.springframework.data.geo.Point
import org.springframework.data.mongodb.core.geo.GeoJsonPoint
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type
import java.util.regex.Pattern

/**
 * @author Tjeu Kayim
 * @author Mark Paluch
 */
class TypedCriteriaExtensionsTests {

	@Test
	fun `isEqualTo() should equal expected criteria`() {

		val typed = Book::title isEqualTo "Moby-Dick"
		val expected = Criteria("title").isEqualTo("Moby-Dick")

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `ne() should equal expected criteria`() {

		val typed = Book::title ne "Moby-Dick"
		val expected = Criteria("title").ne("Moby-Dick")

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `lt() should equal expected criteria`() {

		val typed = Book::price lt 100
		val expected = Criteria("price").lt(100)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `lte() should equal expected criteria`() {

		val typed = Book::price lte 100
		val expected = Criteria("price").lte(100)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `gt() should equal expected criteria`() {

		val typed = Book::price gt 100
		val expected = Criteria("price").gt(100)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `gte() should equal expected criteria`() {

		val typed = Book::price gte 100
		val expected = Criteria("price").gte(100)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `inValues(vararg) should equal expected criteria`() {

		val typed = Book::price.inValues(1, 2, 3)
		val expected = Criteria("price").inValues(1, 2, 3)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `inValues(list) should equal expected criteria`() {

		val typed = Book::price inValues listOf(1, 2, 3)
		val expected = Criteria("price").inValues(listOf(1, 2, 3))

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `nin(vararg) should equal expected criteria`() {

		val typed = Book::price.nin(1, 2, 3)
		val expected = Criteria("price").nin(1, 2, 3)
		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `nin(list) should equal expected criteria`() {

		val typed = Book::price nin listOf(1, 2, 3)
		val expected = Criteria("price").nin(listOf(1, 2, 3))

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `mod() should equal expected criteria`() {

		val typed = Book::price.mod(2, 3)
		val expected = Criteria("price").mod(2, 3)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `all(vararg) should equal expected criteria`() {

		val typed = Book::categories.all(1, 2, 3)
		val expected = Criteria("categories").all(1, 2, 3)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `all(list) should equal expected criteria`() {

		val typed = Book::categories all listOf(1, 2, 3)
		val expected = Criteria("categories").all(listOf(1, 2, 3))

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `size() should equal expected criteria`() {

		val typed = Book::categories size 4
		val expected = Criteria("categories").size(4)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `exists() should equal expected criteria`() {

		val typed = Book::title exists true
		val expected = Criteria("title").exists(true)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `type(Int) should equal expected criteria`() {

		val typed = Book::title type 2
		val expected = Criteria("title").type(2)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `type(List) should equal expected criteria`() {

		val typed = Book::title type listOf(Type.STRING, Type.BOOLEAN)
		val expected = Criteria("title").type(Type.STRING, Type.BOOLEAN)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `type(vararg) should equal expected criteria`() {

		val typed = Book::title.type(Type.STRING, Type.BOOLEAN)
		val expected = Criteria("title").type(Type.STRING, Type.BOOLEAN)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `not() should equal expected criteria`() {

		val typed = Book::price.not().lt(123)
		val expected = Criteria("price").not().lt(123)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `regex(string) should equal expected criteria`() {

		val typed = Book::title regex "ab+c"
		val expected = Criteria("title").regex("ab+c")
		assertEqualCriteriaByJson(typed, expected)
	}

	@Test
	fun `regex(string, options) should equal expected criteria`() {

		val typed = Book::title.regex("ab+c", "g")
		val expected = Criteria("title").regex("ab+c", "g")

		assertEqualCriteriaByJson(typed, expected)
	}

	@Test
	fun `regex(Regex) should equal expected criteria`() {

		val typed = Book::title regex Regex("ab+c")
		val expected = Criteria("title").regex(Pattern.compile("ab+c"))

		assertEqualCriteriaByJson(typed, expected)
	}

	private fun assertEqualCriteriaByJson(typed: Criteria, expected: Criteria) {
		assertThat(typed.criteriaObject.toJson()).isEqualTo(expected.criteriaObject.toJson())
	}

	@Test
	fun `regex(Pattern) should equal expected criteria`() {

		val value = Pattern.compile("ab+c")
		val typed = Book::title regex value
		val expected = Criteria("title").regex(value)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `regex(BsonRegularExpression) should equal expected criteria`() {

		val expression = BsonRegularExpression("ab+c")
		val typed = Book::title regex expression
		val expected = Criteria("title").regex(expression)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `withinSphere() should equal expected criteria`() {

		val value = Circle(Point(928.76, 28.345), 65.243)
		val typed = Building::location withinSphere value
		val expected = Criteria("location").withinSphere(value)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `within() should equal expected criteria`() {

		val value = Circle(Point(5.43421, 12.456), 52.67)
		val typed = Building::location within value
		val expected = Criteria("location").within(value)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `near() should equal expected criteria`() {

		val value = Point(57.431, 71.345)
		val typed = Building::location near value
		val expected = Criteria("location").near(value)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `nearSphere() should equal expected criteria`() {

		val value = Point(5.4321, 12.345)
		val typed = Building::location nearSphere value
		val expected = Criteria("location").nearSphere(value)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `intersects() should equal expected criteria`() {

		val value = GeoJsonPoint(5.481573, 51.451726)
		val typed = Building::location intersects value
		val expected = Criteria("location").intersects(value)
		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `maxDistance() should equal expected criteria`() {

		val typed = Building::location maxDistance 3.0
		val expected = Criteria("location").maxDistance(3.0)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `minDistance() should equal expected criteria`() {

		val typed = Building::location minDistance 3.0
		val expected = Criteria("location").minDistance(3.0)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `elemMatch() should equal expected criteria`() {

		val value = Criteria("price").lt(950)
		val typed = Book::title elemMatch value
		val expected = Criteria("title").elemMatch(value)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `elemMatch(TypedCriteria) should equal expected criteria`() {

		val typed = Book::title elemMatch (Book::price lt 950)
		val expected = Criteria("title").elemMatch(Criteria("price").lt(950))

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `bits() should equal expected criteria`() {

		val typed = Book::title bits { allClear(123) }
		val expected = Criteria("title").bits().allClear(123)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `One level nested should equal expected criteria`() {

		val typed = Book::author / Author::name isEqualTo "Herman Melville"
		val expected = Criteria("author.name").isEqualTo("Herman Melville")

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `Two levels nested should equal expected criteria`() {

		data class Entity(val book: Book)

		val typed = Entity::book / Book::author / Author::name isEqualTo "Herman Melville"
		val expected = Criteria("book.author.name").isEqualTo("Herman Melville")

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `typed criteria inside orOperator() should equal expected criteria`() {

		val typed = (Book::title isEqualTo "Moby-Dick").orOperator(
				Book::price lt 1200,
				Book::price gt 240
		)
		val expected = Criteria("title").isEqualTo("Moby-Dick")
				.orOperator(
						Criteria("price").lt(1200),
						Criteria("price").gt(240)
				)

		assertThat(typed).isEqualTo(expected)
	}

	@Test
	fun `chaining gt & isEqualTo() should equal expected criteria`() {

		val typed = (Book::title isEqualTo "Moby-Dick")
				.and(Book::price).lt(950)
		val expected = Criteria("title").isEqualTo("Moby-Dick")
				.and("price").lt(950)

		assertThat(typed).isEqualTo(expected)
	}

	data class Book(
			val title: String = "Moby-Dick",
			val price: Int = 123,
			val available: Boolean = true,
			val categories: List<String> = emptyList(),
			val author: Author = Author()
	)

	data class Author(
			val name: String = "Herman Melville"
	)

	data class Building(
			val location: GeoJsonPoint
	)
}
