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

import com.mongodb.client.result.DeleteResult
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle

/**
 * Extension for [ReactiveRemoveOperation.remove] leveraging reified type parameters.
 *
 * @author Mark Paluch
 * @since 2.0
 */
inline fun <reified T : Any> ReactiveRemoveOperation.remove(): ReactiveRemoveOperation.ReactiveRemove<T> =
		remove(T::class.java)

/**
 * Coroutines variant of [ReactiveRemoveOperation.TerminatingRemove.all].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend fun <T : Any> ReactiveRemoveOperation.TerminatingRemove<T>.allAndAwait(): DeleteResult =
		all().awaitSingle()


/**
 * Coroutines [Flow] variant of [ReactiveRemoveOperation.TerminatingRemove.findAndRemove].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <T : Any> ReactiveRemoveOperation.TerminatingRemove<T>.findAndRemoveAsFlow(): Flow<T> =
		findAndRemove().asFlow()
