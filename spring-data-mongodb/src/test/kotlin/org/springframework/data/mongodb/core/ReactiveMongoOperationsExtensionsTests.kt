/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.mongodb.core

import com.nhaarman.mockito_kotlin.mock
import example.first.First
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Answers
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.junit.MockitoJUnitRunner
import org.springframework.data.mongodb.core.query.NearQuery
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import reactor.core.publisher.Mono

/**
 * @author Sebastien Deleuze
 */
@RunWith(MockitoJUnitRunner::class)
class ReactiveMongoOperationsExtensionsTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operations: ReactiveMongoOperations

	@Test
	fun `indexOps(KClass) extension should call its Java counterpart`() {
		operations.indexOps(First::class)
		verify(operations, times(1)).indexOps(First::class.java)
	}

	@Test
	fun `indexOps() with reified type parameter extension should call its Java counterpart`() {
		operations.indexOps<First>()
		verify(operations, times(1)).indexOps(First::class.java)
	}

	@Test
	fun `execute(ReactiveCollectionCallback) with reified type parameter extension should call its Java counterpart`() {
		val collectionCallback = mock<ReactiveCollectionCallback<First>>()
		operations.execute(collectionCallback)
		verify(operations, times(1)).execute(First::class.java, collectionCallback)
	}

	@Test
	fun `createCollection(KClass) extension should call its Java counterpart`() {
		operations.createCollection(First::class)
		verify(operations, times(1)).createCollection(First::class.java)
	}

	@Test
	fun `createCollection(KClass, CollectionOptions) extension should call its Java counterpart`() {
		val collectionOptions = mock<CollectionOptions>()
		operations.createCollection(First::class, collectionOptions)
		verify(operations, times(1)).createCollection(First::class.java, collectionOptions)
	}

	@Test
	fun `createCollection() with reified type parameter extension should call its Java counterpart`() {
		operations.createCollection<First>()
		verify(operations, times(1)).createCollection(First::class.java)
	}

	@Test
	fun `createCollection(CollectionOptions) with reified type parameter extension should call its Java counterpart`() {
		val collectionOptions = mock<CollectionOptions>()
		operations.createCollection<First>(collectionOptions)
		verify(operations, times(1)).createCollection(First::class.java, collectionOptions)
	}

	@Test
	fun `collectionExists(KClass) extension should call its Java counterpart`() {
		operations.collectionExists(First::class)
		verify(operations, times(1)).collectionExists(First::class.java)
	}

	@Test
	fun `collectionExists() with reified type parameter extension should call its Java counterpart`() {
		operations.collectionExists<First>()
		verify(operations, times(1)).collectionExists(First::class.java)
	}

	@Test
	fun `dropCollection(KClass) extension should call its Java counterpart`() {
		operations.dropCollection(First::class)
		verify(operations, times(1)).dropCollection(First::class.java)
	}

	@Test
	fun `dropCollection() with reified type parameter extension should call its Java counterpart`() {
		operations.dropCollection<First>()
		verify(operations, times(1)).dropCollection(First::class.java)
	}

	@Test
	fun `findAll() with reified type parameter extension should call its Java counterpart`() {
		operations.findAll<First>()
		verify(operations, times(1)).findAll(First::class.java)
	}

	@Test
	fun `findAll(String) with reified type parameter extension should call its Java counterpart`() {
		val collectionName = "foo"
		operations.findAll<First>(collectionName)
		verify(operations, times(1)).findAll(First::class.java, collectionName)
	}

	@Test
	fun `findOne(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.findOne<First>(query)
		verify(operations, times(1)).findOne(query, First::class.java)
	}

	@Test
	fun `findOne(Query, String) with reified type parameter extension should call its Java counterpart`() {
		val collectionName = "foo"
		val query = mock<Query>()
		operations.findOne<First>(query, collectionName)
		verify(operations, times(1)).findOne(query, First::class.java, collectionName)
	}

	@Test
	fun `exists(Query, KClass) extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.exists(query, First::class)
		verify(operations, times(1)).exists(query, First::class.java)
	}

	@Test
	fun `exists(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.exists<First>(query)
		verify(operations, times(1)).exists(query, First::class.java)
	}

	@Test
	fun `find(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.find<First>(query)
		verify(operations, times(1)).find(query, First::class.java)
	}

	@Test
	fun `find(Query, String) with reified type parameter extension should call its Java counterpart`() {
		val collectionName = "foo"
		val query = mock<Query>()
		operations.find<First>(query, collectionName)
		verify(operations, times(1)).find(query, First::class.java, collectionName)
	}

	@Test
	fun `findById(Any) with reified type parameter extension should call its Java counterpart`() {
		val id = 1L
		operations.findById<First>(id)
		verify(operations, times(1)).findById(id, First::class.java)
	}

	@Test
	fun `findById(Any, String) with reified type parameter extension should call its Java counterpart`() {
		val collectionName = "foo"
		val id = 1L
		operations.findById<First>(id, collectionName)
		verify(operations, times(1)).findById(id, First::class.java, collectionName)
	}

	@Test
	fun `geoNear(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = NearQuery.near(0.0, 0.0)
		operations.geoNear<First>(query)
		verify(operations, times(1)).geoNear(query, First::class.java)
	}

	@Test
	fun `geoNear(Query, String) with reified type parameter extension should call its Java counterpart`() {
		val collectionName = "foo"
		val query = NearQuery.near(0.0, 0.0)
		operations.geoNear<First>(query, collectionName)
		verify(operations, times(1)).geoNear(query, First::class.java, collectionName)
	}

	@Test
	fun `findAndModify(Query, Update, FindAndModifyOptions) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		val options = mock<FindAndModifyOptions>()
		operations.findAndModify<First>(query, update, options)
		verify(operations, times(1)).findAndModify(query, update, options, First::class.java)
	}

	@Test
	fun `findAndModify(Query, Update, FindAndModifyOptions, String) with reified type parameter extension should call its Java counterpart`() {
		val collectionName = "foo"
		val query = mock<Query>()
		val update = mock<Update>()
		val options = mock<FindAndModifyOptions>()
		operations.findAndModify<First>(query, update, options, collectionName)
		verify(operations, times(1)).findAndModify(query, update, options, First::class.java, collectionName)
	}

	@Test
	fun `findAndRemove(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.findAndRemove<First>(query)
		verify(operations, times(1)).findAndRemove(query, First::class.java)
	}

	@Test
	fun `findAndRemove(Query, String) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val collectionName = "foo"
		operations.findAndRemove<First>(query, collectionName)
		verify(operations, times(1)).findAndRemove(query, First::class.java, collectionName)
	}

	@Test
	fun `count() with reified type parameter extension should call its Java counterpart`() {
		operations.count<First>()
		verify(operations, times(1)).count(any<Query>(), eq(First::class.java))
	}

	@Test
	fun `count(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.count<First>(query)
		verify(operations, times(1)).count(query, First::class.java)
	}

	@Test
	fun `count(Query, String) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val collectionName = "foo"
		operations.count<First>(query, collectionName)
		verify(operations, times(1)).count(query, First::class.java, collectionName)
	}

	@Test
	fun `count(Query, KClass) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.count(query, First::class)
		verify(operations, times(1)).count(query, First::class.java)
	}

	@Test
	fun `count(Query, KClass, String) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val collectionName = "foo"
		operations.count(query, First::class, collectionName)
		verify(operations, times(1)).count(query, First::class.java, collectionName)
	}

	@Test
	fun `insert(Collection, KClass) extension should call its Java counterpart`() {
		val collection = listOf(First(), First())
		operations.insert(collection, First::class)
		verify(operations, times(1)).insert(collection, First::class.java)
	}

	@Test
	fun `insertAll(Mono, KClass) extension should call its Java counterpart`() {
		val collection = Mono.just(listOf(First(), First()))
		operations.insertAll(collection, First::class)
		verify(operations, times(1)).insertAll(collection, First::class.java)
	}

	@Test
	fun `upsert(Query, Update, KClass) extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		operations.upsert(query, update, First::class)
		verify(operations, times(1)).upsert(query, update, First::class.java)
	}

	@Test
	fun `upsert(Query, Update, KClass, String) extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		val collectionName = "foo"
		operations.upsert(query, update, First::class, collectionName)
		verify(operations, times(1)).upsert(query, update, First::class.java, collectionName)
	}

	@Test
	fun `upsert(Query, Update) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		operations.upsert<First>(query, update)
		verify(operations, times(1)).upsert(query, update, First::class.java)
	}

	@Test
	fun `upsert(Query, Update, String) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		val collectionName = "foo"
		operations.upsert<First>(query, update, collectionName)
		verify(operations, times(1)).upsert(query, update, First::class.java, collectionName)
	}

	@Test
	fun `updateFirst(Query, Update, KClass) extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		operations.updateFirst(query, update, First::class)
		verify(operations, times(1)).updateFirst(query, update, First::class.java)
	}

	@Test
	fun `updateFirst(Query, Update, KClass, String) extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		val collectionName = "foo"
		operations.updateFirst(query, update, First::class, collectionName)
		verify(operations, times(1)).updateFirst(query, update, First::class.java, collectionName)
	}

	@Test
	fun `updateFirst(Query, Update) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		operations.updateFirst<First>(query, update)
		verify(operations, times(1)).updateFirst(query, update, First::class.java)
	}

	@Test
	fun `updateFirst(Query, Update, String) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		val collectionName = "foo"
		operations.updateFirst<First>(query, update, collectionName)
		verify(operations, times(1)).updateFirst(query, update, First::class.java, collectionName)
	}

	@Test
	fun `updateMulti(Query, Update, KClass) extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		operations.updateMulti(query, update, First::class)
		verify(operations, times(1)).updateMulti(query, update, First::class.java)
	}

	@Test
	fun `updateMulti(Query, Update, KClass, String) extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		val collectionName = "foo"
		operations.updateMulti(query, update, First::class, collectionName)
		verify(operations, times(1)).updateMulti(query, update, First::class.java, collectionName)
	}

	@Test
	fun `updateMulti(Query, Update) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		operations.updateMulti<First>(query, update)
		verify(operations, times(1)).updateMulti(query, update, First::class.java)
	}

	@Test
	fun `updateMulti(Query, Update, String) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val update = mock<Update>()
		val collectionName = "foo"
		operations.updateMulti<First>(query, update, collectionName)
		verify(operations, times(1)).updateMulti(query, update, First::class.java, collectionName)
	}

	@Test
	fun `remove(Query, KClass) extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.remove(query, First::class)
		verify(operations, times(1)).remove(query, First::class.java)
	}

	@Test
	fun `remove(Query, KClass, String) extension should call its Java counterpart`() {
		val query = mock<Query>()
		val collectionName = "foo"
		operations.remove(query, First::class, collectionName)
		verify(operations, times(1)).remove(query, First::class.java, collectionName)
	}

	@Test
	fun `remove(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.remove<First>(query)
		verify(operations, times(1)).remove(query, First::class.java)
	}

	@Test
	fun `remove(Query, String) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val collectionName = "foo"
		operations.remove<First>(query, collectionName)
		verify(operations, times(1)).remove(query, First::class.java, collectionName)
	}

	@Test
	fun `findAllAndRemove(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.findAllAndRemove<First>(query)
		verify(operations, times(1)).findAllAndRemove(query, First::class.java)
	}

	@Test
	fun `tail(Query) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		operations.tail<First>(query)
		verify(operations, times(1)).tail(query, First::class.java)
	}

	@Test
	fun `tail(Query, String) with reified type parameter extension should call its Java counterpart`() {
		val query = mock<Query>()
		val collectionName = "foo"
		operations.tail<First>(query, collectionName)
		verify(operations, times(1)).tail(query, First::class.java, collectionName)
	}

}
