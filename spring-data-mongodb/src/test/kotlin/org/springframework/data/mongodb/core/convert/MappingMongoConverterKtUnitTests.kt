/*
 * Copyright 2024-present the original author or authors.
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
package org.springframework.data.mongodb.core.convert

import org.assertj.core.api.Assertions.assertThat
import org.bson.Document
import org.junit.jupiter.api.Test
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.MongoMappingContext
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query

/**
 * Kotlin unit tests for [MappingMongoConverter].
 *
 * @author Mark Paluch
 * @author Jonghun Ju
 */
class MappingMongoConverterKtUnitTests {

	@Test // GH-4485
	fun shouldIgnoreNonReadableProperties() {

		val document = Document.parse("{_id: 'baz', type: 'SOME_VALUE'}")
		val converter =
			MappingMongoConverter(NoOpDbRefResolver.INSTANCE, MongoMappingContext())

		val tx = converter.read(SpecialTransaction::class.java, document)

		assertThat(tx.id).isEqualTo("baz")
		assertThat(tx.type).isEqualTo("SOME_DEFAULT_VALUE")
	}

	@Test // GH-4078
	fun shouldMapValueClassIdQueryAsScalar() {

		val context = MongoMappingContext()
		val converter = MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context)
		val mapper = QueryMapper(converter)
		val entity = context.getRequiredPersistentEntity(ValueClassIdDocument::class.java)
		val query = Query.query(Criteria.where("id").`is`(PersonId("id-1")))

		val mapped = mapper.getMappedObject(query.queryObject, entity)

		assertThat(mapped["_id"]).isEqualTo("id-1")
	}

	@JvmInline
	value class PersonId(val value: String)

	data class ValueClassIdDocument(@Id val id: PersonId? = null, val name: String)
}
