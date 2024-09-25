/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.query

import org.springframework.data.mapping.toDotPath
import kotlin.reflect.KProperty

/**
 * Extension for [Criteria.is] providing an `isEqualTo` alias since `is` is a reserved keyword in Kotlin.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
fun Criteria.isEqualTo(o: Any?): Criteria = `is`(o)

/**
 * Extension for [Criteria.in] providing an `inValues` alias since `in` is a reserved keyword in Kotlin.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
fun <T : Any?> Criteria.inValues(c: Collection<T>): Criteria = `in`(c)

/**
 * Extension for [Criteria.in] providing an `inValues` alias since `in` is a reserved keyword in Kotlin.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
fun Criteria.inValues(vararg o: Any?): Criteria = `in`(*o)

/**
 * Creates a Criteria using a KProperty as key.
 * Supports nested field names with [KPropertyPath].
 * @author Tjeu Kayim
 * @since 2.2
 */
fun where(key: KProperty<*>): Criteria = Criteria.where(key.toDotPath())

/**
 * Add new key to the criteria chain using a KProperty.
 * Supports nested field names with [KPropertyPath].
 * @author Tjeu Kayim
 * @since 2.2
 */
infix fun Criteria.and(key: KProperty<*>): Criteria = and(key.toDotPath())
