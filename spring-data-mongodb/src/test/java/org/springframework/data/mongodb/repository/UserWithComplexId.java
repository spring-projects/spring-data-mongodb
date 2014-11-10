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

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
@Document
public class UserWithComplexId {

	@Id MyId id;
	String firstname;

	@Override
	public int hashCode() {

		int result = 31;

		result += 17 * ObjectUtils.nullSafeHashCode(id);

		return result;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj == this) {
			return true;
		}

		if (!(obj instanceof UserWithComplexId)) {
			return false;
		}

		UserWithComplexId that = (UserWithComplexId) obj;

		return ObjectUtils.nullSafeEquals(this.id, that.id);
	}
}
