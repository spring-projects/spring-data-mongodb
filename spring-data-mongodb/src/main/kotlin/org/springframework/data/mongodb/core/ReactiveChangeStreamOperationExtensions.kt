/*
 * Copyright 2019-2023 the original author or authors.
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

/**
 * Extension for [RactiveChangeStreamOperation.changeStream] leveraging reified type parameters.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
inline fun <reified T : Any> ReactiveChangeStreamOperation.changeStream(): ReactiveChangeStreamOperation.ReactiveChangeStream<T> =
		changeStream(T::class.java)

/**
 * Extension for [ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection.as] leveraging reified type parameters.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
inline fun <reified T : Any> ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection<*>.asType(): ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection<T> =
		`as`(T::class.java)

/**
 * Coroutines [Flow] variant of [ReactiveChangeStreamOperation.TerminatingChangeStream.listen].
 *
 * @author Christoph Strobl
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <T : Any> ReactiveChangeStreamOperation.TerminatingChangeStream<T>.flow(): Flow<ChangeStreamEvent<T>> =
		listen().asFlow()

