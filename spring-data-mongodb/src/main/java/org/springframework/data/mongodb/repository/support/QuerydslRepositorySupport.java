/*
 * Copyright 2011-2012 the original author or authors.
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

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.util.Assert;

import com.mysema.query.mongodb.MongodbQuery;
import com.mysema.query.types.EntityPath;

/**
 * Base class to create repository implementations based on Querydsl.
 * 
 * @author Oliver Gierke
 */
public abstract class QuerydslRepositorySupport {

	private final MongoOperations template;
	private final MappingContext<? extends MongoPersistentEntity<?>, ?> context;

	/**
	 * Creates a new {@link QuerydslRepositorySupport} for the given {@link MongoOperations}.
	 * 
	 * @param operations must not be {@literal null}
	 */
	public QuerydslRepositorySupport(MongoOperations operations) {

		Assert.notNull(operations);

		this.template = operations;
		this.context = operations.getConverter().getMappingContext();
	}

	/**
	 * Returns a {@link MongodbQuery} for the given {@link EntityPath}. The collection being queried is derived from the
	 * entity metadata.
	 * 
	 * @param path
	 * @return
	 */
	protected <T> MongodbQuery<T> from(final EntityPath<T> path) {
		Assert.notNull(path);
		MongoPersistentEntity<?> entity = context.getPersistentEntity(path.getType());
		return from(path, entity.getCollection());
	}

	/**
	 * Returns a {@link MongodbQuery} for the given {@link EntityPath} querying the given collection.
	 * 
	 * @param path must not be {@literal null}
	 * @param collection must not be blank or {@literal null}
	 * @return
	 */
	protected <T> MongodbQuery<T> from(final EntityPath<T> path, String collection) {

		Assert.notNull(path);
		Assert.hasText(collection);

		return new SpringDataMongodbQuery<T>(template, path.getType(), collection);
	}
}
