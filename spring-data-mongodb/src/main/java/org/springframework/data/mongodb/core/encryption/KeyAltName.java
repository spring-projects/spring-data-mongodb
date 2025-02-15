/*
 * Copyright 2023-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.encryption;

import org.springframework.util.ObjectUtils;

record KeyAltName(String value) implements EncryptionKey {

	@Override
	public Type type() {
		return Type.ALT;
	}

	@Override
	public String toString() {

		if (value().length() <= 3) {
			return "KeyAltName('***')";
		}
		return String.format("KeyAltName('%s***')", value.substring(0, 3));
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KeyAltName that = (KeyAltName) o;
		return ObjectUtils.nullSafeEquals(value, that.value);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(value);
	}
}
