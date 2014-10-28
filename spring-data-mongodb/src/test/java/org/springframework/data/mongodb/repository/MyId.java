/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import java.io.Serializable;

/**
 * @author Christoph Strobl
 */
public class MyId implements Serializable {

	String val1;
	String val2;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((val1 == null) ? 0 : val1.hashCode());
		result = prime * result + ((val2 == null) ? 0 : val2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof MyId)) {
			return false;
		}
		MyId other = (MyId) obj;
		if (val1 == null) {
			if (other.val1 != null) {
				return false;
			}
		} else if (!val1.equals(other.val1)) {
			return false;
		}
		if (val2 == null) {
			if (other.val2 != null) {
				return false;
			}
		} else if (!val2.equals(other.val2)) {
			return false;
		}
		return true;
	}

}
