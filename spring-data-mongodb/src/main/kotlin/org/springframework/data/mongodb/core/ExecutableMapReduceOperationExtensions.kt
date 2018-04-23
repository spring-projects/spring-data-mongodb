/*
 * Copyright 2018 the original author or authors.
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

import kotlin.reflect.KClass

/**
 * Extension for [ExecutableMapReduceOperation.mapReduce] providing a [KClass] based variant.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
fun <T : Any> ExecutableMapReduceOperation.mapReduce(entityClass: KClass<T>): ExecutableMapReduceOperation.MapReduceWithMapFunction<T> =
		mapReduce(entityClass.java)

/**
 * Extension for [ExecutableMapReduceOperation.mapReduce] leveraging reified type parameters.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
inline fun <reified T : Any> ExecutableMapReduceOperation.mapReduce(): ExecutableMapReduceOperation.MapReduceWithMapFunction<T> =
		mapReduce(T::class.java)

/**
 * Extension for [ExecutableMapReduceOperation.MapReduceWithProjection.as] providing a [KClass] based variant.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
fun <T : Any> ExecutableMapReduceOperation.MapReduceWithProjection<T>.asType(resultType: KClass<T>): ExecutableMapReduceOperation.MapReduceWithQuery<T> =
		`as`(resultType.java)

/**
 * Extension for [ExecutableMapReduceOperation.MapReduceWithProjection.as] leveraging reified type parameters.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
inline fun <reified T : Any> ExecutableMapReduceOperation.MapReduceWithProjection<T>.asType(): ExecutableMapReduceOperation.MapReduceWithQuery<T> =
		`as`(T::class.java)
