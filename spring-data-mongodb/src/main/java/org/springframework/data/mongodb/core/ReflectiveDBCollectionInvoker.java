/*
 * Copyright 2015-2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.bson.Document;
import org.springframework.data.mongodb.util.MongoClientVersion;

import com.mongodb.DBCollection;

/**
 * {@link ReflectiveDBCollectionInvoker} provides reflective access to {@link DBCollection} API that is not consistently
 * available for various driver versions.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
class ReflectiveDBCollectionInvoker {

	private ReflectiveDBCollectionInvoker() {}

	/**
	 * Convenience method to generate an index name from the set of fields it is over. Will fall back to a MongoDB Java
	 * driver version 2 compatible way of generating index name in case of {@link MongoClientVersion#isMongo3Driver()}.
	 * 
	 * @param keys the names of the fields used in this index
	 * @return
	 */
	public static String generateIndexName(Document keys) {
		return genIndexName(keys);
	}

	/**
	 * Borrowed from MongoDB Java driver version 2. See
	 * <a href="http://github.com/mongodb/mongo-java-driver/blob/r2.13.0/src/main/com/mongodb/DBCollection.java#L754" >
	 * http://github.com/mongodb/mongo-java-driver/blob/r2.13.0/src/main/com/mongodb/DBCollection.java#L754</a>
	 * 
	 * @param keys
	 * @return
	 */
	private static String genIndexName(Document keys) {

		StringBuilder name = new StringBuilder();

		for (String s : keys.keySet()) {

			if (name.length() > 0) {
				name.append('_');
			}

			name.append(s).append('_');
			Object val = keys.get(s);

			if (val instanceof Number || val instanceof String) {
				name.append(val.toString().replace(' ', '_'));
			}
		}

		return name.toString();
	}
}
