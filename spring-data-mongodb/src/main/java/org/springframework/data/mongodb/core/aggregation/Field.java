/*
 * Copyright 2013-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

/**
 * Abstraction for a field.
 *
 * @author Oliver Gierke
 * @since 1.3
 */
public interface Field {

	/**
	 * Returns the name of the field.
	 *
	 * @return must not be {@literal null}.
	 */
	String getName();

	/**
	 * Returns the target of the field. In case no explicit target is available {@link #getName()} should be returned.
	 *
	 * @return must not be {@literal null}.
	 */
	String getTarget();

	/**
	 * Returns whether the Field is aliased, which means it has a name set different from the target.
	 *
	 * @return
	 */
	boolean isAliased();

	/**
	 * @return true if the field name references a local value such as {@code $$this}.
	 * @since 2.2
	 */
	default boolean isInternal() {
		return false;
	}
}
