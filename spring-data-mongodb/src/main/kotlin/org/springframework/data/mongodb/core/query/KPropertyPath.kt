/*
 * Copyright 2018-2019 the original author or authors.
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

import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

/**
 * Abstraction of a property path consisting of [KProperty].
 * @author Tjeu Kayim
 * @author Mark Paluch
 * @since 2.2
 */
class KPropertyPath<T, U>(
		internal val parent: KProperty<U>,
		internal val child: KProperty1<U, T>
) : KProperty<T> by child

/**
 * Recursively construct field name for a nested property.
 * @author Tjeu Kayim
 */
internal fun asString(property: KProperty<*>): String {
	return when (property) {
		is KPropertyPath<*, *> ->
			"${asString(property.parent)}.${property.child.name}"
		else -> property.name
	}
}

/**
 * Builds [KPropertyPath] from Property References.
 * Refer to a field in an embedded/nested document.
 *
 * For example, referring to the field "book.author":
 * ```
 * Book::author / Author::name isEqualTo "Herman Melville"
 * ```
 * @author Tjeu Kayim
 * @since 2.2
 */
operator fun <T, U> KProperty<T>.div(other: KProperty1<T, U>) =
		KPropertyPath(this, other)
