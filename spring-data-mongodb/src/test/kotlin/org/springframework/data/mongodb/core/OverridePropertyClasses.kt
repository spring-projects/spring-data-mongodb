/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.mongodb.core

import com.mongodb.client.MongoClients
import org.junit.Test
import org.springframework.data.mongodb.core.mapping.Field
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory
import org.springframework.data.mongodb.test.util.Assertions.assertThat

open class SuperType(open var field: Int)

class SubType(val id: String, @Field("foo") override var field: Int = 1) :
	SuperType(field) {

	fun setFields(v: Int) {
		field = v
		super.field = v
	}
}

interface MyRepository : MongoRepository<SubType, String>

class `KotlinOverridePropertyTests` {

	val template = MongoTemplate(MongoClients.create(), "kotlin-tests")

	@Test // DATAMONGO-2250
	fun `Ambiguous field mapping for override val field`() {

		val repository =
			MongoRepositoryFactory(template).getRepository(MyRepository::class.java)

		var subType = SubType("id-1")
		subType.setFields(3)

		repository.save(subType)

		assertThat(repository.findById(subType.id).get().field).isEqualTo(subType.field)
	}
}
