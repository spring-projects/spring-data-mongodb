/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.document.mongodb;

import com.mongodb.DBObject;

/**
 * A MongoWriter is responsible for converting a native MongoDB DBObject to an object of type T.
 * 
 * @param <T>
 *          the type of the object to convert from a DBObject
 * @author Mark Pollack
 * @author Thomas Risberg
 * @author Oliver Gierke
 */
public interface MongoReader<T> {

	/**
	 * Ready from the native MongoDB DBObject representation to an instance of the class T. The given type has to be the
	 * starting point for marshalling the {@link DBObject} into it. So in case there's no real valid data inside
	 * {@link DBObject} for the given type, just return an empty instance of the given type.
	 * 
	 * @param clazz
	 *          the type of the return value
	 * @param dbo
	 *          theDBObject
	 * @return the converted object
	 */
	<S extends T> S read(Class<S> clazz, DBObject dbo);
}
