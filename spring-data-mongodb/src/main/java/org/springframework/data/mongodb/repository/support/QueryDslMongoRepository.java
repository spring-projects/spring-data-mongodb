/*
 * Copyright 2011-2017 the original author or authors.
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
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.querydsl.EntityPathResolver;
import org.springframework.data.querydsl.QSort;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.SimpleEntityPathResolver;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.util.Assert;

import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.mongodb.AbstractMongodbQuery;

/**
 * Special QueryDsl based repository implementation that allows execution {@link Predicate}s in various forms.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class QueryDslMongoRepository<T, ID extends Serializable> extends SimpleMongoRepository<T, ID>
		implements QuerydslPredicateExecutor<T> {

	private final PathBuilder<T> builder;
	private final EntityInformation<T, ID> entityInformation;
	private final MongoOperations mongoOperations;

	/**
	 * Creates a new {@link QueryDslMongoRepository} for the given {@link EntityMetadata} and {@link MongoTemplate}. Uses
	 * the {@link SimpleEntityPathResolver} to create an {@link EntityPath} for the given domain class.
	 * 
	 * @param entityInformation must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public QueryDslMongoRepository(MongoEntityInformation<T, ID> entityInformation, MongoOperations mongoOperations) {
		this(entityInformation, mongoOperations, SimpleEntityPathResolver.INSTANCE);
	}

	/**
	 * Creates a new {@link QueryDslMongoRepository} for the given {@link MongoEntityInformation}, {@link MongoTemplate}
	 * and {@link EntityPathResolver}.
	 * 
	 * @param entityInformation must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param resolver must not be {@literal null}.
	 */
	public QueryDslMongoRepository(MongoEntityInformation<T, ID> entityInformation, MongoOperations mongoOperations,
			EntityPathResolver resolver) {

		super(entityInformation, mongoOperations);

		Assert.notNull(resolver, "EntityPathResolver must not be null!");

		EntityPath<T> path = resolver.createPath(entityInformation.getJavaType());

		this.builder = new PathBuilder<T>(path.getType(), path.getMetadata());
		this.entityInformation = entityInformation;
		this.mongoOperations = mongoOperations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QuerydslPredicateExecutor#findById(com.querydsl.core.types.Predicate)
	 */
	@Override
	public T findOne(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null!");

		return createQueryFor(predicate).fetchOne();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QueryDslPredicateExecutor#findAll(com.mysema.query.types.Predicate)
	 */
	@Override
	public List<T> findAll(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null!");

		return createQueryFor(predicate).fetchResults().getResults();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QueryDslPredicateExecutor#findAll(com.mysema.query.types.Predicate, com.mysema.query.types.OrderSpecifier<?>[])
	 */
	@Override
	public List<T> findAll(Predicate predicate, OrderSpecifier<?>... orders) {

		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(orders, "Order specifiers must not be null!");

		return createQueryFor(predicate).orderBy(orders).fetchResults().getResults();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QueryDslPredicateExecutor#findAll(com.mysema.query.types.Predicate, org.springframework.data.domain.Sort)
	 */
	@Override
	public List<T> findAll(Predicate predicate, Sort sort) {

		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(sort, "Sort must not be null!");

		return applySorting(createQueryFor(predicate), sort).fetchResults().getResults();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QueryDslPredicateExecutor#findAll(com.mysema.query.types.OrderSpecifier[])
	 */
	@Override
	public Iterable<T> findAll(OrderSpecifier<?>... orders) {

		Assert.notNull(orders, "Order specifiers must not be null!");

		return createQuery().orderBy(orders).fetchResults().getResults();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QueryDslPredicateExecutor#findAll(com.mysema.query.types.Predicate, org.springframework.data.domain.Pageable)
	 */
	@Override
	public Page<T> findAll(Predicate predicate, Pageable pageable) {

		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(pageable, "Pageable must not be null!");

		AbstractMongodbQuery<T, SpringDataMongodbQuery<T>> query = createQueryFor(predicate);

		return PageableExecutionUtils.getPage(applyPagination(query, pageable).fetchResults().getResults(), pageable,
				() -> createQueryFor(predicate).fetchCount());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.support.SimpleMongoRepository#findAll(org.springframework.data.domain.Pageable)
	 */
	@Override
	public Page<T> findAll(Pageable pageable) {

		Assert.notNull(pageable, "Pageable must not be null!");

		AbstractMongodbQuery<T, SpringDataMongodbQuery<T>> query = createQuery();

		return PageableExecutionUtils.getPage(applyPagination(query, pageable).fetchResults().getResults(), pageable,
				() -> createQuery().fetchCount());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.support.SimpleMongoRepository#findAll(org.springframework.data.domain.Sort)
	 */
	@Override
	public List<T> findAll(Sort sort) {

		Assert.notNull(sort, "Sort must not be null!");

		return applySorting(createQuery(), sort).fetchResults().getResults();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QueryDslPredicateExecutor#count(com.mysema.query.types.Predicate)
	 */
	@Override
	public long count(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null!");

		return createQueryFor(predicate).fetchCount();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.QueryDslPredicateExecutor#exists(com.mysema.query.types.Predicate)
	 */
	@Override
	public boolean exists(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null!");

		return createQueryFor(predicate).fetchCount() > 0;
	}

	/**
	 * Creates a {@link MongodbQuery} for the given {@link Predicate}.
	 * 
	 * @param predicate
	 * @return
	 */
	private AbstractMongodbQuery<T, SpringDataMongodbQuery<T>> createQueryFor(Predicate predicate) {
		return createQuery().where(predicate);
	}

	/**
	 * Creates a {@link MongodbQuery}.
	 * 
	 * @return
	 */
	private AbstractMongodbQuery<T, SpringDataMongodbQuery<T>> createQuery() {
		return new SpringDataMongodbQuery<T>(mongoOperations, entityInformation.getJavaType());
	}

	/**
	 * Applies the given {@link Pageable} to the given {@link MongodbQuery}.
	 * 
	 * @param query
	 * @param pageable
	 * @return
	 */
	private AbstractMongodbQuery<T, SpringDataMongodbQuery<T>> applyPagination(
			AbstractMongodbQuery<T, SpringDataMongodbQuery<T>> query, Pageable pageable) {

		query = query.offset(pageable.getOffset()).limit(pageable.getPageSize());
		return applySorting(query, pageable.getSort());
	}

	/**
	 * Applies the given {@link Sort} to the given {@link MongodbQuery}.
	 * 
	 * @param query
	 * @param sort
	 * @return
	 */
	private AbstractMongodbQuery<T, SpringDataMongodbQuery<T>> applySorting(
			AbstractMongodbQuery<T, SpringDataMongodbQuery<T>> query, Sort sort) {

		// TODO: find better solution than instanceof check
		if (sort instanceof QSort) {

			List<OrderSpecifier<?>> orderSpecifiers = ((QSort) sort).getOrderSpecifiers();
			query.orderBy(orderSpecifiers.toArray(new OrderSpecifier<?>[orderSpecifiers.size()]));

			return query;
		}

		sort.stream().map(this::toOrder).forEach(it -> query.orderBy(it));

		return query;
	}

	/**
	 * Transforms a plain {@link Order} into a QueryDsl specific {@link OrderSpecifier}.
	 * 
	 * @param order
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private OrderSpecifier<?> toOrder(Order order) {

		Expression<Object> property = builder.get(order.getProperty());

		return new OrderSpecifier(
				order.isAscending() ? com.querydsl.core.types.Order.ASC : com.querydsl.core.types.Order.DESC, property);
	}
}
