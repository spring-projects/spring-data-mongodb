/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Rogério Meneguelli Gatto
 * @since 3.2
 */
class UnwrapEntityContext {

	private final MongoPersistentProperty property;

	public UnwrapEntityContext(MongoPersistentProperty property) {
		this.property = property;
	}

	public MongoPersistentProperty getProperty() {
		return property;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		UnwrapEntityContext that = (UnwrapEntityContext) obj;
		return ObjectUtils.nullSafeEquals(property, that.property);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(property);
	}
}
