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

import org.springframework.data.mapping.toDotPath
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

/**
 * Extension for [ExecutableFindOperation.query] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @author Mark Paluch
 * @since 2.0
 */
inline fun <reified T : Any> ExecutableFindOperation.query(): ExecutableFindOperation.ExecutableFind<T> =
		query(T::class.java)

/**
 * Extension for [ExecutableFindOperation.query] for a type-safe projection of distinct values.
 *
 * @author Mark Paluch
 * @since 3.0
 */
inline fun <reified T : Any> ExecutableFindOperation.distinct(field : KProperty1<T, *>): ExecutableFindOperation.TerminatingDistinct<Any> =
		query(T::class.java).distinct(field.name)

/**
 * Extension for [ExecutableFindOperation.FindWithProjection.as] leveraging reified type parameters.
 *
 * @author Sebastien Deleuze
 * @author Mark Paluch
 * @since 2.0
 */
inline fun <reified T : Any> ExecutableFindOperation.FindWithProjection<*>.asType(): ExecutableFindOperation.FindWithQuery<T> =
		`as`(T::class.java)

/**
 * Extension for [ExecutableFindOperation.DistinctWithProjection.as] leveraging reified type parameters.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
inline fun <reified T : Any> ExecutableFindOperation.DistinctWithProjection.asType(): ExecutableFindOperation.TerminatingDistinct<T> =
		`as`(T::class.java)

/**
 * Extension for [ExecutableFindOperation.FindDistinct.distinct] leveraging KProperty.
 *
 * @author Mark Paluch
 * @since 3.0
 */
fun ExecutableFindOperation.FindDistinct.distinct(key: KProperty<*>): ExecutableFindOperation.TerminatingDistinct<Any> =
		distinct(key.toDotPath())
