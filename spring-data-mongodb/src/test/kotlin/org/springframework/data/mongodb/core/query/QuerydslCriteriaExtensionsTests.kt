/*
 * Copyright 2018-2022 the original author or authors.
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

import com.querydsl.core.types.Path
import org.assertj.core.api.Assertions.assertThat
import org.bson.BsonRegularExpression
import org.junit.Test
import org.springframework.data.geo.Circle
import org.springframework.data.geo.Point
import org.springframework.data.mongodb.core.geo.GeoJson
import org.springframework.data.mongodb.core.geo.GeoJsonPoint
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type
import org.springframework.data.mongodb.repository.QBook
import org.springframework.data.mongodb.repository.QBuilding
import java.util.regex.Pattern

/**
 * Unit tests for [Criteria] extensions.
 *
 * @author Sangyong Choi
 */
class QuerydslCriteriaExtensionsTests {

    @Test
    fun `isEqualTo() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title isEqualTo "Moby-Dick"
        val expected = Criteria("title").isEqualTo("Moby-Dick")

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `ne() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title ne "Moby-Dick"
        val expected = Criteria("title").ne("Moby-Dick")

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `lt() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price lt 100
        val expected = Criteria("price").lt(100)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `lte() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price lte 100
        val expected = Criteria("price").lte(100)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `gt() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price gt 100
        val expected = Criteria("price").gt(100)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `gte() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price gte 100
        val expected = Criteria("price").gte(100)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `inValues(vararg) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price.inValues(1, 2, 3)
        val expected = Criteria("price").inValues(1, 2, 3)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `inValues(list) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price inValues listOf(1, 2, 3)
        val expected = Criteria("price").inValues(listOf(1, 2, 3))

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `nin(vararg) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price.nin(1, 2, 3)
        val expected = Criteria("price").nin(1, 2, 3)
        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `nin(list) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price nin listOf(1, 2, 3)
        val expected = Criteria("price").nin(listOf(1, 2, 3))

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `all(vararg) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.categories.all(1, 2, 3)
        val expected = Criteria("categories").all(1, 2, 3)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `all(list) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.categories all listOf(1, 2, 3)
        val expected = Criteria("categories").all(listOf(1, 2, 3))

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `size() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.categories size 4
        val expected = Criteria("categories").size(4)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `exists() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title exists true
        val expected = Criteria("title").exists(true)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `type(Int) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title type 2
        val expected = Criteria("title").type(2)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `type(List) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title type listOf(Type.STRING, Type.BOOLEAN)
        val expected = Criteria("title").type(Type.STRING, Type.BOOLEAN)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `type(vararg) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title.type(Type.STRING, Type.BOOLEAN)
        val expected = Criteria("title").type(Type.STRING, Type.BOOLEAN)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `not() should equal expected criteria`() {
        val book = QBook.book
        val typed = book.price.not().lt(123)
        val expected = Criteria("price").not().lt(123)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `regex(string) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title regex "ab+c"
        val expected = Criteria("title").regex("ab+c")
        assertEqualCriteriaByJson(typed, expected)
    }

    @Test
    fun `regex(string, options) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title.regex("ab+c", "g")
        val expected = Criteria("title").regex("ab+c", "g")

        assertEqualCriteriaByJson(typed, expected)
    }

    @Test
    fun `regex(Regex) should equal expected criteria`() {
        val book = QBook.book
        val typed = book.title regex Regex("ab+c")
        val expected = Criteria("title").regex(Pattern.compile("ab+c"))

        assertEqualCriteriaByJson(typed, expected)
    }

    private fun assertEqualCriteriaByJson(typed: Criteria, expected: Criteria) {
        assertThat(typed.criteriaObject.toJson()).isEqualTo(expected.criteriaObject.toJson())
    }

    @Test
    fun `regex(Pattern) should equal expected criteria`() {
        val book = QBook.book
        val value = Pattern.compile("ab+c")
        val typed = book.title regex value
        val expected = Criteria("title").regex(value)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `regex(BsonRegularExpression) should equal expected criteria`() {
        val book = QBook.book
        val expression = BsonRegularExpression("ab+c")
        val typed = book.title regex expression
        val expected = Criteria("title").regex(expression)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `withinSphere() should equal expected criteria`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val value = Circle(Point(928.76, 28.345), 65.243)
        val typed = location withinSphere value
        val expected = Criteria("location").withinSphere(value)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `within() should equal expected criteria`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val value = Circle(Point(5.43421, 12.456), 52.67)
        val typed = location within value
        val expected = Criteria("location").within(value)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `near() should equal expected criteria`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val value = Point(57.431, 71.345)
        val typed = location near value
        val expected = Criteria("location").near(value)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `nearSphere() should equal expected criteria`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val value = Point(5.4321, 12.345)
        val typed = location nearSphere value
        val expected = Criteria("location").nearSphere(value)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `intersects() should equal expected criteria`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val value = GeoJsonPoint(5.481573, 51.451726)
        val typed = location intersects value
        val expected = Criteria("location").intersects(value)
        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `maxDistance() should equal expected criteria with nearSphere`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val point = Point(0.0, 0.0)
        val typed = location nearSphere point maxDistance 3.0
        val expected = Criteria("location")
            .nearSphere(point)
            .maxDistance(3.0)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `minDistance() should equal expected criteria with nearSphere`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val point = Point(0.0, 0.0)
        val typed = location nearSphere point minDistance 3.0
        val expected = Criteria("location")
            .nearSphere(point)
            .minDistance(3.0)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `maxDistance() should equal expected criteria with near`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val point = Point(0.0, 0.0)
        val typed = location near point maxDistance 3.0
        val expected = Criteria("location")
            .near(point)
            .maxDistance(3.0)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `minDistance() should equal expected criteria with near`() {
        val building = QBuilding.building
        val location = building.location as Path<GeoJson<*>>

        val point = Point(0.0, 0.0)
        val typed = location near point minDistance 3.0
        val expected = Criteria("location")
            .near(point)
            .minDistance(3.0)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `elemMatch() should equal expected criteria`() {
        val book = QBook.book

        val value = Criteria("price").lt(950)
        val typed = book.title elemMatch value
        val expected = Criteria("title").elemMatch(value)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `elemMatch(TypedCriteria) should equal expected criteria`() {
        val book = QBook.book

        val typed = book.title elemMatch (book.price lt 950)
        val expected = Criteria("title").elemMatch(Criteria("price").lt(950))

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `bits() should equal expected criteria`() {
        val book = QBook.book

        val typed = book.title bits { allClear(123) }
        val expected = Criteria("title").bits().allClear(123)

        assertThat(typed).isEqualTo(expected)
    }

    @Test
    fun `typed criteria inside orOperator() should equal expected criteria`() {
        val book = QBook.book

        val typed = (book.title isEqualTo "Moby-Dick").orOperator(
            book.price lt 1200,
            book.price gt 240
        )
        val expected = Criteria("title").isEqualTo("Moby-Dick")
            .orOperator(
                Criteria("price").lt(1200),
                Criteria("price").gt(240)
            )

        assertThat(typed).isEqualTo(expected)
    }
}
