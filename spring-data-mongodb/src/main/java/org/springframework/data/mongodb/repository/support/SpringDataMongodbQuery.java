/*
 * Copyright 2012 the original author or authors.
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

import com.google.common.base.Function;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mysema.query.mongodb.MongodbQuery;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;

/**
 * Spring Data specfic {@link MongodbQuery} implementation.
 * 
 * @author Oliver Gierke
 * @author Jordi Llach
 */
class SpringDataMongodbQuery<T> extends MongodbQuery<T> {

	private final MongoOperations operations;

	/**
	 * Creates a new {@link SpringDataMongodbQuery}.
	 * 
	 * @param operations must not be {@literal null}.
	 * @param type must not be {@literal null}.
     * @param eventPublisher.
	 */
	public SpringDataMongodbQuery(final MongoOperations operations, final Class<? extends T> type,
                                 final ApplicationEventPublisher eventPublisher) {
		this(operations, type, operations.getCollectionName(type), eventPublisher);
	}

	/**
	 * Creates a new {@link SpringDataMongodbQuery} to query the given collection.
	 * 
	 * @param operations must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
     * @param eventPublisher.
	 */
	public SpringDataMongodbQuery(final MongoOperations operations, final Class<? extends T> type, final String collectionName,
                                  final ApplicationEventPublisher eventPublisher) {

		super(operations.getCollection(collectionName), new Function<DBObject, T>() {
			public T apply(DBObject input) {
                if (eventPublisher != null) eventPublisher.publishEvent(new AfterLoadEvent<T>(input, (Class<T>)type, collectionName));
				T read = operations.getConverter().read(type, input);
                if (eventPublisher != null) eventPublisher.publishEvent(new AfterConvertEvent<T>(input, read, collectionName));
                return read;
			}
		}, new SpringDataMongodbSerializer(operations.getConverter()));

		this.operations = operations;
	}

	/*
	 * (non-Javadoc)
	 * @see com.mysema.query.mongodb.MongodbQuery#getCollection(java.lang.Class)
	 */
	@Override
	protected DBCollection getCollection(Class<?> type) {
		return operations.getCollection(operations.getCollectionName(type));
	}
}