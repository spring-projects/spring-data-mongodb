/*
 * Copyright 2011 the original author or authors.
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

package org.springframework.data.mongodb;

import org.springframework.util.StringUtils;



/**
 * Helper class featuring helper methods for working with MongoDb collections.
 * <p/>
 * <p/>
 * Mainly intended for internal use within the framework.
 *
 * @author Thomas Risberg
 * @since 1.0
 */
public abstract class MongoCollectionUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private MongoCollectionUtils() {

	}

	/**
	 * Obtains the collection name to use for the provided class
	 *
	 * @param entityClass The class to determine the preferred collection name for
	 * @return The preferred collection name
	 */
	public static String getPreferredCollectionName(Class<?> entityClass) {
		return StringUtils.uncapitalize(entityClass.getSimpleName());
	}

}
