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
package org.springframework.data.mongodb.core.convert

import org.assertj.core.api.Assertions.assertThat
import org.bson.Document
import org.junit.jupiter.api.Test
import org.springframework.data.mongodb.core.mapping.MongoMappingContext

/**
 * Kotlin unit tests for [MappingMongoConverter].
 *
 * @author Mark Paluch
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
}
