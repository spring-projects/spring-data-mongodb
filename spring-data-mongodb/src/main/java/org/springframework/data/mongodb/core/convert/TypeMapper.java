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
package org.springframework.data.mongodb.core.convert;

import org.springframework.data.util.TypeInformation;

import com.mongodb.DBObject;

/**
 * Interface to define strategies how to store type information in a {@link DBObject}.
 * 
 * @author Oliver Gierke
 */
public interface TypeMapper {

	/**
	 * Returns whether the given key is the key being used as type key.
	 * 
	 * @param key
	 * @return
	 */
	boolean isTypeKey(String key);

	/**
	 * Reads the {@link TypeInformation} from the given {@link DBObject}.
	 * 
	 * @param dbObject must not be {@literal null}.
	 * @return
	 */
	TypeInformation<?> readType(DBObject dbObject);

	/**
	 * Writes type information for the given type into the given {@link DBObject}.
	 * 
	 * @param type must not be {@literal null}.
	 * @param dbObject must not be {@literal null}.
	 */
	void writeType(Class<?> type, DBObject dbObject);

	/**
	 * Writes type information for the given {@link TypeInformation} into the given {@link DBObject}.
	 * 
	 * @param type must not be {@literal null}.
	 * @param dbObject must not be {@literal null}.
	 */
	void writeType(TypeInformation<?> type, DBObject dbObject);
}
