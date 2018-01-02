/*
 * Copyright 2011-2018 the original author or authors.
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

import java.io.Serializable;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.querydsl.EntityPathResolver;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;

import com.querydsl.core.types.Predicate;

/**
 * Special Querydsl based repository implementation that allows execution {@link Predicate}s in various forms.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 * @deprecated since 2.0. Querydsl execution is now linked via composable repositories and no longer requires to be a
 *             subclass of {@link SimpleMongoRepository}. Use {@link QuerydslMongoPredicateExecutor} for standalone
 *             Querydsl {@link Predicate} execution.
 */
@Deprecated
public class QuerydslMongoRepository<T, ID extends Serializable> extends QuerydslMongoPredicateExecutor<T>
		implements QuerydslPredicateExecutor<T> {

	public QuerydslMongoRepository(MongoEntityInformation<T, ?> entityInformation, MongoOperations mongoOperations) {
		super(entityInformation, mongoOperations);
	}

	public QuerydslMongoRepository(MongoEntityInformation<T, ?> entityInformation, MongoOperations mongoOperations,
			EntityPathResolver resolver) {
		super(entityInformation, mongoOperations, resolver);
	}
}
