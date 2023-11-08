/*
 * Copyright 2019-2023 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.bson.Document;
import org.reactivestreams.Publisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Window;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.querydsl.EntityPathResolver;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor;
import org.springframework.data.querydsl.SimpleEntityPathResolver;
import org.springframework.data.repository.query.FluentQuery;
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

	@Override
	public Mono<T> findOne(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null");

		return createQueryFor(predicate).fetchOne();
	}

	@Override
	public Flux<T> findAll(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null");

		return createQueryFor(predicate).fetch();
	}

	@Override
	public Flux<T> findAll(Predicate predicate, OrderSpecifier<?>... orders) {

		Assert.notNull(predicate, "Predicate must not be null");
		Assert.notNull(orders, "Order specifiers must not be null");

		return createQueryFor(predicate).orderBy(orders).fetch();
	}

	@Override
	public Flux<T> findAll(Predicate predicate, Sort sort) {

		Assert.notNull(predicate, "Predicate must not be null");
		Assert.notNull(sort, "Sort must not be null");

		return applySorting(createQueryFor(predicate), sort).fetch();
	}

	@Override
	public Flux<T> findAll(OrderSpecifier<?>... orders) {

		Assert.notNull(orders, "Order specifiers must not be null");

		return createQuery().orderBy(orders).fetch();
	}

	@Override
	public Mono<Long> count(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null");

		return createQueryFor(predicate).fetchCount();
	}

	@Override
	public Mono<Boolean> exists(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null");

		return createQueryFor(predicate).fetchCount().map(it -> it != 0);
	}

	@Override
	public <S extends T, R, P extends Publisher<R>> P findBy(Predicate predicate,
			Function<FluentQuery.ReactiveFluentQuery<S>, P> queryFunction) {

		Assert.notNull(predicate, "Predicate must not be null");
		Assert.notNull(queryFunction, "Query function must not be null");

		return queryFunction.apply(new ReactiveFluentQuerydsl<S>(predicate, (Class<S>) typeInformation().getJavaType()));
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
		return new ReactiveSpringDataMongodbQuery<>(mongoOperations, javaType, javaType,
				mongoOperations.getCollectionName(javaType), it -> {});
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

	/**
	 * {@link org.springframework.data.repository.query.FluentQuery.ReactiveFluentQuery} using Querydsl {@link Predicate}.
	 *
	 * @since 3.3
	 * @author Mark Paluch
	 */
	class ReactiveFluentQuerydsl<T> extends ReactiveFluentQuerySupport<Predicate, T> {

		ReactiveFluentQuerydsl(Predicate predicate, Class<T> resultType) {
			this(predicate, Sort.unsorted(), 0, resultType, Collections.emptyList());
		}

		ReactiveFluentQuerydsl(Predicate predicate, Sort sort, int limit, Class<T> resultType,
				List<String> fieldsToInclude) {
			super(predicate, sort, limit, resultType, fieldsToInclude);
		}

		@Override
		protected <R> ReactiveFluentQuerydsl<R> create(Predicate predicate, Sort sort, int limit, Class<R> resultType,
				List<String> fieldsToInclude) {
			return new ReactiveFluentQuerydsl<>(predicate, sort, limit, resultType, fieldsToInclude);
		}

		@Override
		public Mono<T> one() {
			return createQuery().fetchOne();
		}

		@Override
		public Mono<T> first() {
			return createQuery().fetchFirst();
		}

		@Override
		public Flux<T> all() {
			return createQuery().fetch();
		}

		@Override
		public Mono<Window<T>> scroll(ScrollPosition scrollPosition) {
			return createQuery().scroll(scrollPosition);
		}

		@Override
		public Mono<Page<T>> page(Pageable pageable) {

			Assert.notNull(pageable, "Pageable must not be null");

			return createQuery().fetchPage(pageable);
		}

		@Override
		public Mono<Long> count() {
			return createQuery().fetchCount();
		}

		@Override
		public Mono<Boolean> exists() {
			return count().map(it -> it > 0).defaultIfEmpty(false);
		}

		private ReactiveSpringDataMongodbQuery<T> createQuery() {

			return new ReactiveSpringDataMongodbQuery<>(mongoOperations, typeInformation().getJavaType(), getResultType(),
					mongoOperations.getCollectionName(typeInformation().getJavaType()), this::customize).where(getPredicate());
		}

		private void customize(BasicQuery query) {

			List<String> fieldsToInclude = getFieldsToInclude();

			if (!fieldsToInclude.isEmpty()) {
				Document fields = new Document();
				fieldsToInclude.forEach(field -> fields.put(field, 1));
				query.setFieldsObject(fields);
			}

			if (getSort().isSorted()) {
				query.with(getSort());
			}

			query.limit(getLimit());
		}
	}

}
