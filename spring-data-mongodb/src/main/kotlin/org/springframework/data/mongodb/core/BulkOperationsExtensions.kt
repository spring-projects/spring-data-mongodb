/*
 * Copyright 2025 the original author or authors.
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

import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.data.mongodb.core.query.UpdateDefinition
import org.springframework.data.util.Pair.of

/**
 * Extension for [BulkOperations.updateMulti] that converts a list of [kotlin.Pair] to list of [org.springframework.data.util.Pair].
 *
 * @author 2tsumo-hitori
 * @since 4.5
 */
fun BulkOperations.updateMulti(kotlinPairs: List<Pair<Query, UpdateDefinition>>): BulkOperations =
	updateMulti(kotlinPairs.toSpringPairs())

/**
 * Extension for [BulkOperations.upsert] that converts a list of [kotlin.Pair] to list of [org.springframework.data.util.Pair].
 *
 * @author 2tsumo-hitori
 * @since 4.5
 */
fun BulkOperations.upsert(kotlinPairs: List<Pair<Query, Update>>): BulkOperations =
	upsert(kotlinPairs.toSpringPairs())

/**
 * Extension for [BulkOperations.updateOne] that converts a [kotlin.Pair] to [org.springframework.data.util.Pair].
 *
 * @author 2tsumo-hitori
 * @since 4.5
 */
fun BulkOperations.updateOne(kotlinPairs: List<Pair<Query, UpdateDefinition>>): BulkOperations =
	updateOne(kotlinPairs.toSpringPairs())

private fun <A : Query, B : UpdateDefinition> List<Pair<A, B>>.toSpringPairs(): List<org.springframework.data.util.Pair<A, B>> {
	return map { (first, second) -> of(first, second) }
}
