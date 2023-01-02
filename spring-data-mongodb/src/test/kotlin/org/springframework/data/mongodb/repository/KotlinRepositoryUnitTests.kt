/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.convert.MappingMongoConverter
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver
import org.springframework.data.mongodb.core.mapping.MongoMappingContext
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory
import org.springframework.data.repository.CrudRepository

/**
 * Unit tests for Kotlin repositories.
 *
 * @author Mark Paluch
 */
class KotlinRepositoryUnitTests {

	val operations = mockk<MongoOperations>(relaxed = true)
	lateinit var repositoryFactory: MongoRepositoryFactory

	@BeforeEach
	fun before() {

		every { operations.getConverter() } returns MappingMongoConverter(NoOpDbRefResolver.INSTANCE, MongoMappingContext())
		repositoryFactory = MongoRepositoryFactory(operations)
	}

	@Test // DATAMONGO-2601
	fun should() {

		val repository = repositoryFactory.getRepository(PersonRepository::class.java)

		repository.deleteAllByName("foo")
	}

	interface PersonRepository : CrudRepository<Person, Long> {

		fun deleteAllByName(name: String)
	}

	data class Person(@Id var id: Long, var name: String)
}
