/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.Collection;
import java.util.Collections;

import org.springframework.util.ClassUtils;

/**
 * {@link ReflectiveSimpleTypes} provides reflective access to MongoDB types that are not consistently available for
 * various driver versions.
 *
 * @author Mark Paluch
 * @since 1.10
 */
class ReflectiveSimpleTypes {

	private static final boolean HAS_DECIMAL_128 = ClassUtils.isPresent("org.bson.types.Decimal128",
			ReflectiveSimpleTypes.class.getClassLoader());

	/**
	 * Returns a {@link Collection} of simple MongoDB types (i.e. natively supported by the MongoDB driver) that are not
	 * consistently available for various driver versions.
	 * 
	 * @return a {@link Collection} of simple MongoDB types.
	 */
	public static Collection<Class<?>> getSupportedSimpleTypes() {

		if (HAS_DECIMAL_128) {
			return Collections.<Class<?>> singleton(getDecimal128Class());
		}

		return Collections.emptySet();
	}

	private static Class<?> getDecimal128Class() {
		return ClassUtils.resolveClassName("org.bson.types.Decimal128", ReflectiveSimpleTypes.class.getClassLoader());
	}
}
