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
import org.junit.Test

/**
 * @author Tjeu Kayim
 */
class KPropertyPathTests {

	@Test
	fun `Convert normal KProperty to field name`() {

		val property = asString(Book::title)

		assertThat(property).isEqualTo("title")
	}

	@Test
	fun `Convert nested KProperty to field name`() {

		val property = asString(Book::author / Author::name)

		assertThat(property).isEqualTo("author.name")
	}

	@Test
	fun `Convert double nested KProperty to field name`() {

		class Entity(val book: Book)

		val property = asString(Entity::book / Book::author / Author::name)

		assertThat(property).isEqualTo("book.author.name")
	}

	@Test
	fun `Convert triple nested KProperty to field name`() {

		class Entity(val book: Book)
		class AnotherEntity(val entity: Entity)

		val property = asString(AnotherEntity::entity / Entity::book / Book::author / Author::name)

		assertThat(property).isEqualTo("entity.book.author.name")
	}

	class Book(val title: String, val author: Author)
	class Author(val name: String)
}
