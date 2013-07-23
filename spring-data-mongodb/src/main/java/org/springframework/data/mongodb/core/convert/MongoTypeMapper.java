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

import java.util.Set;

import org.springframework.data.convert.TypeMapper;

import com.mongodb.DBObject;

/**
 * Mongo-specific {@link TypeMapper} exposing that {@link DBObject}s might contain a type key.
 * 
 * @author Oliver Gierke
 */
public interface MongoTypeMapper extends TypeMapper<DBObject> {

	/**
	 * Returns whether the given key is the type key.
	 * 
	 * @return
	 */
	boolean isTypeKey(String key);

	/**
	 * Writes type restrictions to the given {@link DBObject}. This usually results in an {@code $in}-clause to be
	 * generated that restricts the type-key (e.g. {@code _class}) to be in the set of type aliases for the given
	 * {@code restrictedTypes}.
	 * 
	 * @param result must not be {@literal null}
	 * @param restrictedTypes must not be {@literal null}
	 */
	void writeTypeRestrictions(DBObject result, Set<Class<?>> restrictedTypes);
}
