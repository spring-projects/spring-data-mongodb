/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.support;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.querydsl.EntityPathResolver;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor;
import org.springframework.data.querydsl.SimpleEntityPathResolver;
import org.springframework.util.Assert;

import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Predicate;

/**
 * MongoDB-specific {@link QuerydslPredicateExecutor} that allows execution {@link Predicate}s in various forms.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
public class ReactiveQuerydslMongoPredicateExecutor<T> extends QuerydslPredicateExecutorSupport<T>
		implements ReactiveQuerydslPredicateExecutor<T> {

	private final ReactiveMongoOperations mongoOperations;

	/**
	 * Creates a new {@link ReactiveQuerydslMongoPredicateExecutor} for the given {@link MongoEntityInformation} and
	 * {@link ReactiveMongoOperations}. Uses the {@link SimpleEntityPathResolver} to create an {@link EntityPath} for the
	 * given domain class.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public ReactiveQuerydslMongoPredicateExecutor(MongoEntityInformation<T, ?> entityInformation,
			ReactiveMongoOperations mongoOperations) {

		this(entityInformation, mongoOperations, SimpleEntityPathResolver.INSTANCE);
	}

	/**
	 * Creates a new {@link ReactiveQuerydslMongoPredicateExecutor} for the given {@link MongoEntityInformation},
	 * {@link ReactiveMongoOperations} and {@link EntityPathResolver}.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param resolver must not be {@literal null}.
	 */
	public ReactiveQuerydslMongoPredicateExecutor(MongoEntityInformation<T, ?> entityInformation,
			ReactiveMongoOperations mongoOperations, EntityPathResolver resolver) {

		super(mongoOperations.getConverter(), pathBuilderFor(resolver.createPath(entityInformation.getJavaType())),
				entityInformation);
		this.mongoOperations = mongoOperations;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor#findOne(com.querydsl.core.types.Predicate)
	 */
	@Override
	public Mono<T> findOne(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null!");

		return createQueryFor(predicate).fetchOne();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor#findAll(com.querydsl.core.types.Predicate)
	 */
	@Override
	public Flux<T> findAll(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null!");

		return createQueryFor(predicate).fetch();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor#findAll(com.querydsl.core.types.Predicate, com.querydsl.core.types.OrderSpecifier[])
	 */
	@Override
	public Flux<T> findAll(Predicate predicate, OrderSpecifier<?>... orders) {

		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(orders, "Order specifiers must not be null!");

		return createQueryFor(predicate).orderBy(orders).fetch();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor#findAll(com.querydsl.core.types.Predicate, org.springframework.data.domain.Sort)
	 */
	@Override
	public Flux<T> findAll(Predicate predicate, Sort sort) {

		Assert.notNull(predicate, "Predicate must not be null!");
		Assert.notNull(sort, "Sort must not be null!");

		return applySorting(createQueryFor(predicate), sort).fetch();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor#findAll(com.querydsl.core.types.OrderSpecifier[])
	 */
	@Override
	public Flux<T> findAll(OrderSpecifier<?>... orders) {

		Assert.notNull(orders, "Order specifiers must not be null!");

		return createQuery().orderBy(orders).fetch();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor#count(com.querydsl.core.types.Predicate)
	 */
	@Override
	public Mono<Long> count(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null!");

		return createQueryFor(predicate).fetchCount();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor#exists(com.querydsl.core.types.Predicate)
	 */
	@Override
	public Mono<Boolean> exists(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null!");

		return createQueryFor(predicate).fetchCount().map(it -> it != 0);
	}

	/**
	 * Creates a {@link ReactiveSpringDataMongodbQuery} for the given {@link Predicate}.
	 *
	 * @param predicate
	 * @return
	 */
	private ReactiveSpringDataMongodbQuery<T> createQueryFor(Predicate predicate) {
		return createQuery().where(predicate);
	}

	/**
	 * Creates a {@link ReactiveSpringDataMongodbQuery}.
	 *
	 * @return
	 */
	private ReactiveSpringDataMongodbQuery<T> createQuery() {

		Class<T> javaType = typeInformation().getJavaType();
		return new ReactiveSpringDataMongodbQuery<>(mongodbSerializer(), mongoOperations, javaType,
				mongoOperations.getCollectionName(javaType));
	}

	/**
	 * Applies the given {@link Sort} to the given {@link ReactiveSpringDataMongodbQuery}.
	 *
	 * @param query
	 * @param sort
	 * @return
	 */
	private ReactiveSpringDataMongodbQuery<T> applySorting(ReactiveSpringDataMongodbQuery<T> query, Sort sort) {

		toOrderSpecifiers(sort).forEach(query::orderBy);
		return query;
	}

}
