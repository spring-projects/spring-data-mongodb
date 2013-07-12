/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.springframework.util.Assert;

/**
 * Utility class for mongo db reference operator <code>$</code>
 * 
 * @author Sebastian Herold
 * @author Thomas Darimont
 * @since 1.3
 */
class ReferenceUtil {

	public static final String ID_KEY = "_id";

	private static final String REFERENCE_PREFIX = "$";

	/**
	 * Ensures that the returned string begins with {@link #REFERENCE_PREFIX}.
	 * 
	 * @param key reference key with or without {@link #REFERENCE_PREFIX} at the beginning.
	 * @return key that definitely begins with {@link #REFERENCE_PREFIX}.
	 */
	public static String safeReference(String key) {

		Assert.hasText(key);

		if (!key.startsWith(REFERENCE_PREFIX)) {
			return REFERENCE_PREFIX + key;
		} else {
			return key;
		}
	}

	/**
	 * Ensures that the returned string does not start with {@link #REFERENCE_PREFIX}.
	 * 
	 * @param field reference key with or without {@link #REFERENCE_PREFIX} at the beginning.
	 * @return key that definitely does not begin with {@link #REFERENCE_PREFIX}.
	 */
	public static String safeNonReference(String field) {

		Assert.hasText(field);

		if (field.startsWith(REFERENCE_PREFIX)) {
			return field.substring(REFERENCE_PREFIX.length());
		}

		return field;
	}

	public static String $id() {
		return $(ID_KEY);
	}

	public static String $(String name) {
		return safeReference(name);
	}

	public static String $id(String name) {
		return $id() + "." + safeNonReference(name);
	}

}
