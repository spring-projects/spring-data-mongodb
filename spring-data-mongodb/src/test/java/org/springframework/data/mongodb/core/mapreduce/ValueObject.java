/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapreduce;

import java.util.Objects;

/**
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class ValueObject {

	private String id;

	private float value;

	public ValueObject() {}

	public ValueObject(String id, float value) {
		this.id = id;
		this.value = value;
	}

	public String getId() {
		return this.id;
	}

	public float getValue() {
		return this.value;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setValue(float value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "ValueObject [id=" + id + ", value=" + value + "]";
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ValueObject that = (ValueObject) o;
		return Float.compare(that.value, value) == 0 && Objects.equals(id, that.id);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, value);
	}
}
