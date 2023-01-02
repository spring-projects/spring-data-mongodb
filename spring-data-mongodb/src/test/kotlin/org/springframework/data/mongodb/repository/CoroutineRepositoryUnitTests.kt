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

import com.mongodb.client.result.DeleteResult
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.convert.MappingMongoConverter
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver
import org.springframework.data.mongodb.core.mapping.MongoMappingContext
import org.springframework.data.mongodb.repository.support.ReactiveMongoRepositoryFactory
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import reactor.core.publisher.Mono

/**
 * Unit tests for Kotlin Coroutine repositories.
 *
 * @author Mark Paluch
 */
class CoroutineRepositoryUnitTests {

	val operations = mockk<ReactiveMongoOperations>(relaxed = true)
	lateinit var repositoryFactory: ReactiveMongoRepositoryFactory

	@BeforeEach
	fun before() {

		every { operations.getConverter() } returns MappingMongoConverter(NoOpDbRefResolver.INSTANCE, MongoMappingContext())
		repositoryFactory = ReactiveMongoRepositoryFactory(operations)
	}

	@Test // DATAMONGO-2601
	fun `should discard result of suspended query method without result`() {

		every { operations.remove(any(), any(), any()) } returns Mono.just(DeleteResult.acknowledged(1))

		val repository = repositoryFactory.getRepository(PersonRepository::class.java)

		runBlocking {
			repository.deleteAllByName("foo")
		}
	}

	interface PersonRepository : CoroutineCrudRepository<Person, Long> {

		suspend fun deleteAllByName(name: String)
	}

	data class Person(@Id var id: Long, var name: String)
}
