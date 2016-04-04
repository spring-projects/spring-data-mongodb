/*
 * Copyright 2015 the original author or authors.
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

import static org.springframework.data.mongodb.util.MongoClientVersion.*;
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Method;

import org.bson.Document;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.util.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBRef;
import com.mongodb.client.model.Filters;

/**
 * {@link ReflectiveDBRefResolver} provides reflective access to {@link DBRef} API that is not consistently available
 * for various driver versions.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
class ReflectiveDBRefResolver {

	private static final Method FETCH_METHOD;

	static {
		FETCH_METHOD = findMethod(DBRef.class, "fetch");
	}

	/**
	 * Fetches the object referenced from the database either be directly calling {@link DBRef#fetch()} or
	 * {@link DBCollection#findOne(Object)}.
	 *
	 * @param db can be {@literal null} when using MongoDB Java driver in version 2.x.
	 * @param ref must not be {@literal null}.
	 * @return the document that this references.
	 */
	public static Document fetch(MongoDbFactory factory, DBRef ref) {

		Assert.notNull(ref, "DBRef to fetch must not be null!");

		if (isMongo3Driver()) {

			Assert.notNull(factory, "DbFactory to fetch DB from must not be null!");

			return factory.getDb().getCollection(ref.getCollectionName(), Document.class).find(Filters.eq("_id", ref.getId()))
					.first();
		}

		return (Document) invokeMethod(FETCH_METHOD, ref);
	}
}
