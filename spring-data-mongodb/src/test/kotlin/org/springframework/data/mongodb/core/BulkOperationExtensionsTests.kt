/*
 * Copyright 2025-present the original author or authors.
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

import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.data.mongodb.core.query.UpdateDefinition
import org.springframework.data.util.Pair.of

/**
 * Unit tests for BulkOperationExtensions.
 * @author 2tsumo-hitori
 */
class BulkOperationExtensionsTests {

	private val bulkOperation = mockk<BulkOperations>(relaxed = true)

	@Test // GH-4911
	fun `BulkOperation#updateMulti using kotlin#Pair should call its Java counterpart`() {

		val list: MutableList<Pair<Query, UpdateDefinition>> = mutableListOf()
		list.add(where("value", "v2") to set("value", "v3"))

		bulkOperation.updateMulti(list)

		val expected = list.map { (query, update) -> of(query, update) }
		verify { bulkOperation.updateMulti(expected) }
	}

	@Test // GH-4911
	fun `BulkOperation#upsert using kotlin#Pair should call its Java counterpart`() {

		val list: MutableList<Pair<Query, Update>> = mutableListOf()
		list.add(where("value", "v2") to set("value", "v3"))

		bulkOperation.upsert(list)

		val expected = list.map { (query, update) -> of(query, update) }
		verify { bulkOperation.upsert(expected) }
	}

	@Test // GH-4911
	fun `BulkOperation#updateOne using kotlin#Pair should call its Java counterpart`() {

		val list: MutableList<Pair<Query, UpdateDefinition>> = mutableListOf()
		list.add(where("value", "v2") to set("value", "v3"))

		bulkOperation.updateOne(list)

		val expected = list.map { (query, update) -> of(query, update) }
		verify { bulkOperation.updateOne(expected) }
	}

	private fun where(field: String, value: String): Query {
		return Query().addCriteria(Criteria.where(field).`is`(value))
	}

	private fun set(field: String, value: String): Update {
		return Update().set(field, value)
	}

}
