/*
 * Copyright 2017-2023 the original author or authors.
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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.geo.GeoResult
import org.springframework.data.mapping.toDotPath
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

/**
 * Extension for [ReactiveFindOperation.query] leveraging reified type parameters.
 *
 * @author Mark Paluch
 * @since 2.0
 */
inline fun <reified T : Any> ReactiveFindOperation.query(): ReactiveFindOperation.ReactiveFind<T> =
		query(T::class.java)

/**
 * Extension for [ReactiveFindOperation.query] for a type-safe projection of distinct values.
 *
 * @author Mark Paluch
 * @since 3.0
 */
inline fun <reified T : Any> ReactiveFindOperation.distinct(field : KProperty1<T, *>): ReactiveFindOperation.TerminatingDistinct<Any> =
		query(T::class.java).distinct(field.name)

/**
 * Extension for [ReactiveFindOperation.FindWithProjection.as] leveraging reified type parameters.
 *
 * @author Mark Paluch
 * @since 2.0
 */
inline fun <reified T : Any> ReactiveFindOperation.FindWithProjection<*>.asType(): ReactiveFindOperation.FindWithQuery<T> =
		`as`(T::class.java)

/**
 * Extension for [ReactiveFindOperation.DistinctWithProjection.as] leveraging reified type parameters.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
inline fun <reified T : Any> ReactiveFindOperation.DistinctWithProjection.asType(): ReactiveFindOperation.TerminatingDistinct<T> =
		`as`(T::class.java)

/**
 * Extension for [ReactiveFindOperation.FindDistinct.distinct] leveraging KProperty.
 *
 * @author Mark Paluch
 * @since 3.0
 */
fun ReactiveFindOperation.FindDistinct.distinct(key: KProperty<*>): ReactiveFindOperation.TerminatingDistinct<Any> =
		distinct(key.toDotPath())

/**
 * Non-nullable Coroutines variant of [ReactiveFindOperation.TerminatingFind.one].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend inline fun <reified T : Any> ReactiveFindOperation.TerminatingFind<T>.awaitOne(): T =
		one().awaitSingle()

/**
 * Nullable Coroutines variant of [ReactiveFindOperation.TerminatingFind.one].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend inline fun <reified T : Any> ReactiveFindOperation.TerminatingFind<T>.awaitOneOrNull(): T? =
		one().awaitFirstOrNull()

/**
 * Non-nullable Coroutines variant of [ReactiveFindOperation.TerminatingFind.first].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend inline fun <reified T : Any> ReactiveFindOperation.TerminatingFind<T>.awaitFirst(): T =
		first().awaitSingle()

/**
 * Nullable Coroutines variant of [ReactiveFindOperation.TerminatingFind.first].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend inline fun <reified T : Any> ReactiveFindOperation.TerminatingFind<T>.awaitFirstOrNull(): T? =
		first().awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveFindOperation.TerminatingFind.count].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend fun <T : Any> ReactiveFindOperation.TerminatingFind<T>.awaitCount(): Long =
		count().awaitSingle()

/**
 * Coroutines variant of [ReactiveFindOperation.TerminatingFind.exists].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend fun <T : Any> ReactiveFindOperation.TerminatingFind<T>.awaitExists(): Boolean =
		exists().awaitSingle()

/**
 * Coroutines [Flow] variant of [ReactiveFindOperation.TerminatingFind.all].
 *
 * @author Sebastien Deleuze
 */
fun <T : Any> ReactiveFindOperation.TerminatingFind<T>.flow(): Flow<T> =
		all().asFlow()

/**
 * Coroutines [Flow] variant of [ReactiveFindOperation.TerminatingFind.tail].
 *
 * @author Sebastien Deleuze
 */
fun <T : Any> ReactiveFindOperation.TerminatingFind<T>.tailAsFlow(): Flow<T> =
		tail().asFlow()

/**
 * Coroutines [Flow] variant of [ReactiveFindOperation.TerminatingFindNear.all].
 *
 * @author Sebastien Deleuze
 */
fun <T : Any> ReactiveFindOperation.TerminatingFindNear<T>.flow(): Flow<GeoResult<T>> =
		all().asFlow()

/**
 * Coroutines [Flow] variant of [ReactiveFindOperation.TerminatingDistinct.all].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <T : Any> ReactiveFindOperation.TerminatingDistinct<T>.flow(): Flow<T> =
		all().asFlow()
