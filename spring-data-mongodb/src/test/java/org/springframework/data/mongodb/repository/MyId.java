/*
 * Copyright 2014-2018 the original author or authors.
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

import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
public class MyId implements Serializable {

	private static final long serialVersionUID = -7129201311241750831L;

	String val1;
	String val2;

	@Override
	public int hashCode() {

		int result = 31;

		result += 17 * ObjectUtils.nullSafeHashCode(val1);
		result += 17 * ObjectUtils.nullSafeHashCode(val2);

		return result;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj == this) {
			return true;
		}

		if (!(obj instanceof MyId)) {
			return false;
		}

		MyId that = (MyId) obj;

		return ObjectUtils.nullSafeEquals(this.val1, that.val1) && ObjectUtils.nullSafeEquals(this.val2, that.val2);
	}
}
