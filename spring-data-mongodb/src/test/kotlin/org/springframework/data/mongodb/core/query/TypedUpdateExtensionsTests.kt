/*
 * Copyright 2024 the original author or authors.
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
import org.junit.Test
import org.springframework.data.mapping.div
import java.time.Instant

/**
 * Unit tests for [Update] extensions.
 *
 * @author Pawel Matysek
 */
class TypedUpdateExtensionsTests {

    @Test // GH-3028
    fun `update() should equal expected Update`() {

        val typed = update(Book::title, "Moby-Dick")
        val expected = Update.update("title", "Moby-Dick")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `set() should equal expected Update`() {

        val typed = Update().set(Book::title, "Moby-Dick")
        val expected = Update().set("title", "Moby-Dick")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `setOnInsert() should equal expected Update`() {

        val typed = Update().setOnInsert(Book::title, "Moby-Dick")
        val expected = Update().setOnInsert("title", "Moby-Dick")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `unset() should equal expected Update`() {

        val typed = Update().unset(Book::title)
        val expected = Update().unset("title")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `inc(key, inc) should equal expected Update`() {

        val typed = Update().inc(Book::price, 5)
        val expected = Update().inc("price", 5)

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `inc(key) should equal expected Update`() {

        val typed = Update().inc(Book::price)
        val expected = Update().inc("price")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `push(key, value) should equal expected Update`() {

        val typed = Update().push(Book::categories, "someCategory")
        val expected = Update().push("categories", "someCategory")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `push(key) should equal expected Update`() {

        val typed = Update().push(Book::categories)
        val expected = Update().push("categories")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `addToSet(key) should equal expected Update`() {

        val typed = Update().addToSet(Book::categories).each("category", "category2")
        val expected = Update().addToSet("categories").each("category", "category2")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `addToSet(key, value) should equal expected Update`() {

        val typed = Update().addToSet(Book::categories, "someCategory")
        val expected = Update().addToSet("categories", "someCategory")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `pop() should equal expected Update`() {

        val typed = Update().pop(Book::categories, Update.Position.FIRST)
        val expected = Update().pop("categories", Update.Position.FIRST)

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `pull() should equal expected Update`() {

        val typed = Update().pull(Book::categories, "someCategory")
        val expected = Update().pull("categories", "someCategory")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `pullAll() should equal expected Update`() {

        val typed = Update().pullAll(Book::categories, arrayOf("someCategory", "someCategory2"))
        val expected = Update().pullAll("categories", arrayOf("someCategory", "someCategory2"))

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `currentDate() should equal expected Update`() {

        val typed = Update().currentDate(Book::releaseDate)
        val expected = Update().currentDate("releaseDate")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `currentTimestamp() should equal expected Update`() {

        val typed = Update().currentTimestamp(Book::releaseDate)
        val expected = Update().currentTimestamp("releaseDate")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `multiply() should equal expected Update`() {

        val typed = Update().multiply(Book::price, 2)
        val expected = Update().multiply("price", 2)

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `max() should equal expected Update`() {

        val typed = Update().max(Book::price, 200)
        val expected = Update().max("price", 200)

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `min() should equal expected Update`() {

        val typed = Update().min(Book::price, 100)
        val expected = Update().min("price", 100)

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `bitwise() should equal expected Update`() {

        val typed = Update().bitwise(Book::price).and(2)
        val expected = Update().bitwise("price").and(2)

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `filterArray() should equal expected Update`() {

        val typed = Update().filterArray(Book::categories, "someCategory")
        val expected = Update().filterArray("categories", "someCategory")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `typed modifies() should equal expected modifies()`() {

        val typed = update(Book::title, "Moby-Dick")

        assertThat(typed.modifies(Book::title)).isEqualTo(typed.modifies("title"))
        assertThat(typed.modifies(Book::price)).isEqualTo(typed.modifies("price"))
    }

    @Test // GH-3028
    fun `One level nested should equal expected Update`() {

        val typed = update(Book::author / Author::name, "Herman Melville")
        val expected = Update.update("author.name", "Herman Melville")

        assertThat(typed).isEqualTo(expected)
    }

    @Test // GH-3028
    fun `Two levels nested should equal expected Update`() {

        data class Entity(val book: Book)

        val typed = update(Entity::book / Book::author / Author::name, "Herman Melville")
        val expected = Update.update("book.author.name", "Herman Melville")

        assertThat(typed).isEqualTo(expected)
    }

    data class Book(
        val title: String = "Moby-Dick",
        val price: Int = 123,
        val available: Boolean = true,
        val categories: List<String> = emptyList(),
        val author: Author = Author(),
        val releaseDate: Instant,
    )

    data class Author(
		val name: String = "Herman Melville",
	)
}
