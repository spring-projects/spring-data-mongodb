/*
 * Copyright 2017-2019 the original author or authors.
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

import com.mongodb.client.result.UpdateResult
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlin.reflect.KClass

/**
 * Extension for [ReactiveUpdateOperation.update] providing a [KClass] based variant.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("update<T>()"))
fun <T : Any> ReactiveUpdateOperation.update(entityClass: KClass<T>): ReactiveUpdateOperation.ReactiveUpdate<T> =
		update(entityClass.java)

/**
 * Extension for [ReactiveUpdateOperation.update] leveraging reified type parameters.
 *
 * @author Mark Paluch
 * @since 2.0
 */
inline fun <reified T : Any> ReactiveUpdateOperation.update(): ReactiveUpdateOperation.ReactiveUpdate<T> =
		update(T::class.java)

/**
 * Coroutines variant of [ReactiveUpdateOperation.TerminatingFindAndModify.findModifyAndAwait].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend fun <T : Any> ReactiveUpdateOperation.TerminatingFindAndModify<T>.findModifyAndAwait(): T? =
		findAndModify().awaitFirstOrNull()


/**
 * Coroutines variant of [ReactiveUpdateOperation.TerminatingFindAndReplace.findAndReplace].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend fun <T : Any> ReactiveUpdateOperation.TerminatingFindAndReplace<T>.findReplaceAndAwait(): T? =
		findAndReplace().awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveUpdateOperation.TerminatingUpdate.all].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend fun <T : Any> ReactiveUpdateOperation.TerminatingUpdate<T>.allAndAwait(): UpdateResult =
		all().awaitSingle()

/**
 * Coroutines variant of [ReactiveUpdateOperation.TerminatingUpdate.first].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend fun <T : Any> ReactiveUpdateOperation.TerminatingUpdate<T>.firstAndAwait(): UpdateResult =
		first().awaitSingle()

/**
 * Coroutines variant of [ReactiveUpdateOperation.TerminatingUpdate.upsert].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend fun <T : Any> ReactiveUpdateOperation.TerminatingUpdate<T>.upsertAndAwait(): UpdateResult = upsert().awaitSingle()

/**
 * Extension for [ReactiveUpdateOperation.FindAndReplaceWithProjection.as] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
inline fun <reified T : Any> ReactiveUpdateOperation.FindAndReplaceWithProjection<*>.asType(): ReactiveUpdateOperation.FindAndReplaceWithOptions<T> =
		`as`(T::class.java)
