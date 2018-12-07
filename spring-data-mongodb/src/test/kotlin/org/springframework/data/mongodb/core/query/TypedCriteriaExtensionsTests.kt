/*
 * Copyright 2018 the original author or authors.
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

import org.assertj.core.api.Assertions.*
import org.bson.BsonRegularExpression
import org.junit.Test
import org.springframework.data.geo.Circle
import org.springframework.data.geo.Point
import org.springframework.data.mongodb.core.geo.GeoJsonPoint
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.*
import java.util.regex.Pattern

/**
 * @author Tjeu Kayim
 */
class TypedCriteriaExtensionsTests {

	@Test
	fun `isEqualTo() should equal classic criteria`() {

		val typed = Book::title isEqualTo "Moby-Dick"
		val classic = Criteria("title").isEqualTo("Moby-Dick")
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `ne() should equal classic criteria`() {

		val typed = Book::title ne "Moby-Dick"
		val classic = Criteria("title").ne("Moby-Dick")
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `lt() should equal classic criteria`() {

		val typed = Book::price lt 100
		val classic = Criteria("price").lt(100)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `lte() should equal classic criteria`() {

		val typed = Book::price lte 100
		val classic = Criteria("price").lte(100)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `gt() should equal classic criteria`() {

		val typed = Book::price gt 100
		val classic = Criteria("price").gt(100)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `gte() should equal classic criteria`() {

		val typed = Book::price gte 100
		val classic = Criteria("price").gte(100)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `inValues(vararg) should equal classic criteria`() {

		val typed = Book::price.inValues(1, 2, 3)
		val classic = Criteria("price").inValues(1, 2, 3)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `inValues(list) should equal classic criteria`() {

		val typed = Book::price inValues listOf(1, 2, 3)
		val classic = Criteria("price").inValues(listOf(1, 2, 3))
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `nin(vararg) should equal classic criteria`() {

		val typed = Book::price.nin(1, 2, 3)
		val classic = Criteria("price").nin(1, 2, 3)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `nin(list) should equal classic criteria`() {

		val typed = Book::price nin listOf(1, 2, 3)
		val classic = Criteria("price").nin(listOf(1, 2, 3))
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `mod() should equal classic criteria`() {

		val typed = Book::price.mod(2, 3)
		val classic = Criteria("price").mod(2, 3)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `all(vararg) should equal classic criteria`() {

		val typed = Book::categories.all(1, 2, 3)
		val classic = Criteria("categories").all(1, 2, 3)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `all(list) should equal classic criteria`() {

		val typed = Book::categories all listOf(1, 2, 3)
		val classic = Criteria("categories").all(listOf(1, 2, 3))
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `size() should equal classic criteria`() {

		val typed = Book::categories size 4
		val classic = Criteria("categories").size(4)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `exists() should equal classic criteria`() {

		val typed = Book::title exists true
		val classic = Criteria("title").exists(true)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `type(Int) should equal classic criteria`() {

		val typed = Book::title type 2
		val classic = Criteria("title").type(2)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `type(List) should equal classic criteria`() {

		val typed = Book::title type listOf(Type.STRING, Type.BOOLEAN)
		val classic = Criteria("title").type(Type.STRING, Type.BOOLEAN)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `type(vararg) should equal classic criteria`() {

		val typed = Book::title.type(Type.STRING, Type.BOOLEAN)
		val classic = Criteria("title").type(Type.STRING, Type.BOOLEAN)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `not() should equal classic criteria`() {

		val typed = Book::price.not().lt(123)
		val classic = Criteria("price").not().lt(123)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `regex(string) should equal classic criteria`() {

		val typed = Book::title regex "ab+c"
		val classic = Criteria("title").regex("ab+c")
		assertEqualCriteriaByJson(typed, classic)
	}

	@Test
	fun `regex(string, options) should equal classic criteria`() {

		val typed = Book::title.regex("ab+c", "g")
		val classic = Criteria("title").regex("ab+c", "g")
		assertEqualCriteriaByJson(typed, classic)
	}

	@Test
	fun `regex(Regex) should equal classic criteria`() {

		val typed = Book::title regex Regex("ab+c")
		val classic = Criteria("title").regex(Pattern.compile("ab+c"))
		assertEqualCriteriaByJson(typed, classic)
	}

	private fun assertEqualCriteriaByJson(typed: Criteria, classic: Criteria) {
		assertThat(typed.criteriaObject.toJson()).isEqualTo(classic.criteriaObject.toJson())
	}

	@Test
	fun `regex(Pattern) should equal classic criteria`() {

		val value = Pattern.compile("ab+c")
		val typed = Book::title regex value
		val classic = Criteria("title").regex(value)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `regex(BsonRegularExpression) should equal classic criteria`() {

		val expression = BsonRegularExpression("ab+c")
		val typed = Book::title regex expression
		val classic = Criteria("title").regex(expression)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `withinSphere() should equal classic criteria`() {

		val value = Circle(Point(928.76, 28.345), 65.243)
		val typed = Building::location withinSphere value
		val classic = Criteria("location").withinSphere(value)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `within() should equal classic criteria`() {

		val value = Circle(Point(5.43421, 12.456), 52.67)
		val typed = Building::location within value
		val classic = Criteria("location").within(value)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `near() should equal classic criteria`() {

		val value = Point(57.431, 71.345)
		val typed = Building::location near value
		val classic = Criteria("location").near(value)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `nearSphere() should equal classic criteria`() {

		val value = Point(5.4321, 12.345)
		val typed = Building::location nearSphere value
		val classic = Criteria("location").nearSphere(value)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `intersects() should equal classic criteria`() {

		val value = GeoJsonPoint(5.481573, 51.451726)
		val typed = Building::location intersects value
		val classic = Criteria("location").intersects(value)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `maxDistance() should equal classic criteria`() {

		val typed = Building::location maxDistance 3.0
		val classic = Criteria("location").maxDistance(3.0)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `minDistance() should equal classic criteria`() {

		val typed = Building::location minDistance 3.0
		val classic = Criteria("location").minDistance(3.0)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `elemMatch() should equal classic criteria`() {

		val value = Criteria("price").lt(950)
		val typed = Book::title elemMatch value
		val classic = Criteria("title").elemMatch(value)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `elemMatch(TypedCriteria) should equal classic criteria`() {

		val typed = Book::title elemMatch (Book::price lt 950)
		val classic = Criteria("title").elemMatch(Criteria("price").lt(950))
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `bits() should equal classic criteria`() {

		val typed = Book::title bits { allClear(123) }
		val classic = Criteria("title").bits().allClear(123)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `One level nested should equal classic criteria`() {

		val typed = Book::author / Author::name isEqualTo "Herman Melville"

		val classic = Criteria("author.name").isEqualTo("Herman Melville")
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `Two levels nested should equal classic criteria`() {

		data class Entity(val book: Book)

		val typed = Entity::book / Book::author / Author::name isEqualTo "Herman Melville"
		val classic = Criteria("book.author.name").isEqualTo("Herman Melville")
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `typed criteria inside orOperator() should equal classic criteria`() {

		val typed = (Book::title isEqualTo "Moby-Dick").orOperator(
			Book::price lt 1200,
			Book::price gt 240
		)
		val classic = Criteria("title").isEqualTo("Moby-Dick")
			.orOperator(
				Criteria("price").lt(1200),
				Criteria("price").gt(240)
			)
		assertThat(typed).isEqualTo(classic)
	}

	@Test
	fun `chaining gt & isEqualTo() should equal classic criteria`() {

		val typed = (Book::title isEqualTo "Moby-Dick")
			.and(Book::price).lt(950)
		val classic = Criteria("title").isEqualTo("Moby-Dick")
			.and("price").lt(950)
		assertThat(typed).isEqualTo(classic)
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