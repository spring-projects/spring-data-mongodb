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

import java.util.List;

import org.springframework.data.mongodb.core.MongoOperations;

import com.mysema.commons.lang.CloseableIterator;
import com.querydsl.core.NonUniqueResultException;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Predicate;
import com.querydsl.mongodb.AbstractMongodbQuery;

/**
 * Spring Data specific {@link AbstractMongodbQuery} implementation.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class SpringDataMongodbQuery<T> implements SimpleFetchableQuery<T> {

	private final OperationsMongodbQuery<T> query;

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

		query = new OperationsMongodbQuery<>(new SpringDataMongodbSerializer(operations.getConverter()), type,
				collectionName, operations);
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetch()
	 */
	@Override
	public List<T> fetch() {
		return query.fetch();
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchFirst()
	 */
	@Override
	public T fetchFirst() {
		return query.fetchFirst();
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchOne()
	 */
	@Override
	public T fetchOne() throws NonUniqueResultException {
		return query.fetchOne();
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#iterate()
	 */
	@Override
	public CloseableIterator<T> iterate() {
		return query.iterate();
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchResults()
	 */
	@Override
	public QueryResults<T> fetchResults() {
		return query.fetchResults();
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchCount()
	 */
	@Override
	public long fetchCount() {
		return query.fetchCount();
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#limit(long)
	 */
	@Override
	public SpringDataMongodbQuery<T> limit(long limit) {
		query.limit(limit);
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#offset(long)
	 */
	@Override
	public SpringDataMongodbQuery<T> offset(long offset) {
		query.offset(offset);
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#restrict(com.querydsl.core.QueryModifiers)
	 */
	@Override
	public SpringDataMongodbQuery<T> restrict(QueryModifiers modifiers) {
		query.restrict(modifiers);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#orderBy(com.querydsl.core.types.OrderSpecifier[])
	 */
	@Override
	public SpringDataMongodbQuery<T> orderBy(OrderSpecifier<?>... o) {
		query.orderBy(o);
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#set(com.querydsl.core.types.ParamExpression, java.lang.Object)
	 */
	@Override
	public <V> SpringDataMongodbQuery<T> set(ParamExpression<V> param, V value) {
		query.set(param, value);
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#distinct()
	 */
	@Override
	public SpringDataMongodbQuery<T> distinct() {
		query.distinct();
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see com.querydsl.core.FilteredClause#where(com.querydsl.core.types.Predicate[])
	 */
	@Override
	public SpringDataMongodbQuery<T> where(Predicate... o) {
		query.where(o);
		return this;
	}

	/**
	 * Concrete implementation of {@link FetchableMongodbQuery}.
	 *
	 * @param <T>
	 */
	static class OperationsMongodbQuery<T> extends FetchableMongodbQuery<T, OperationsMongodbQuery<T>> {

		public OperationsMongodbQuery(MongodbDocumentSerializer serializer, Class<? extends T> entityClass,
				String collection, MongoOperations mongoOperations) {
			super(serializer, entityClass, collection, mongoOperations);
		}
	}
}
