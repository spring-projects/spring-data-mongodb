/*
 * Copyright 2012-2018 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import org.springframework.data.mongodb.core.MongoOperations;

/**
 * Spring Data specific simple {@link com.querydsl.core.Fetchable} {@link com.querydsl.core.SimpleQuery Query}
 * implementation.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class SpringDataMongodbQuery<T> extends QuerydslFetchableMongodbQuery<T, SpringDataMongodbQuery<T>> {

	/**
	 * Creates a new {@link SpringDataMongodbQuery}.
	 *
	 * @param operations must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 */
	public SpringDataMongodbQuery(final MongoOperations operations, final Class<? extends T> type) {
		this(operations, type, operations.getCollectionName(type));
	}

	/**
	 * Creates a new {@link SpringDataMongodbQuery} to query the given collection.
	 *
	 * @param operations must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 */
	public SpringDataMongodbQuery(final MongoOperations operations, final Class<? extends T> type,
			String collectionName) {

		super(new SpringDataMongodbSerializer(operations.getConverter()), type, collectionName, operations);
	}
}
