/*
 * Copyright 2017-2023 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.Document;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Window;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.querydsl.EntityPathResolver;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.SimpleEntityPathResolver;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.util.Assert;

import com.querydsl.core.NonUniqueResultException;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Predicate;

/**
 * MongoDB-specific {@link QuerydslPredicateExecutor} that allows execution {@link Predicate}s in various forms.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public class QuerydslMongoPredicateExecutor<T> extends QuerydslPredicateExecutorSupport<T>
		implements QuerydslPredicateExecutor<T> {

	private final MongoOperations mongoOperations;

	/**
	 * Creates a new {@link QuerydslMongoPredicateExecutor} for the given {@link MongoEntityInformation} and
	 * {@link MongoOperations}. Uses the {@link SimpleEntityPathResolver} to create an {@link EntityPath} for the given
	 * domain class.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public QuerydslMongoPredicateExecutor(MongoEntityInformation<T, ?> entityInformation,
			MongoOperations mongoOperations) {
		this(entityInformation, mongoOperations, SimpleEntityPathResolver.INSTANCE);
	}

	/**
	 * Creates a new {@link QuerydslMongoPredicateExecutor} for the given {@link MongoEntityInformation},
	 * {@link MongoOperations} and {@link EntityPathResolver}.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param resolver must not be {@literal null}.
	 */
	public QuerydslMongoPredicateExecutor(MongoEntityInformation<T, ?> entityInformation, MongoOperations mongoOperations,
			EntityPathResolver resolver) {

		super(mongoOperations.getConverter(), pathBuilderFor(resolver.createPath(entityInformation.getJavaType())),
				entityInformation);
		this.mongoOperations = mongoOperations;
	}

	@Override
	public Optional<T> findOne(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null");

		try {
			return Optional.ofNullable(createQueryFor(predicate).fetchOne());
		} catch (NonUniqueResultException ex) {
			throw new IncorrectResultSizeDataAccessException(ex.getMessage(), 1, ex);
		}
	}

	@Override
	public List<T> findAll(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null");

		return createQueryFor(predicate).fetch();
	}

	@Override
	public List<T> findAll(Predicate predicate, OrderSpecifier<?>... orders) {

		Assert.notNull(predicate, "Predicate must not be null");
		Assert.notNull(orders, "Order specifiers must not be null");

		return createQueryFor(predicate).orderBy(orders).fetch();
	}

	@Override
	public List<T> findAll(Predicate predicate, Sort sort) {

		Assert.notNull(predicate, "Predicate must not be null");
		Assert.notNull(sort, "Sort must not be null");

		return applySorting(createQueryFor(predicate), sort).fetch();
	}

	@Override
	public Iterable<T> findAll(OrderSpecifier<?>... orders) {

		Assert.notNull(orders, "Order specifiers must not be null");

		return createQuery().orderBy(orders).fetch();
	}

	@Override
	public Page<T> findAll(Predicate predicate, Pageable pageable) {

		Assert.notNull(predicate, "Predicate must not be null");
		Assert.notNull(pageable, "Pageable must not be null");

		SpringDataMongodbQuery<T> query = createQueryFor(predicate);

		return PageableExecutionUtils.getPage(applyPagination(query, pageable).fetch(), pageable, query::fetchCount);
	}

	@Override
	public long count(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null");

		return createQueryFor(predicate).fetchCount();
	}

	@Override
	public boolean exists(Predicate predicate) {

		Assert.notNull(predicate, "Predicate must not be null");

		return createQueryFor(predicate).fetchCount() > 0;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S extends T, R> R findBy(Predicate predicate,
			Function<FluentQuery.FetchableFluentQuery<S>, R> queryFunction) {

		Assert.notNull(predicate, "Predicate must not be null");
		Assert.notNull(queryFunction, "Query function must not be null");

		return queryFunction.apply(new FluentQuerydsl<>(predicate, (Class<S>) typeInformation().getJavaType()));
	}

	/**
	 * Creates a {@link SpringDataMongodbQuery} for the given {@link Predicate}.
	 *
	 * @param predicate
	 * @return
	 */
	private SpringDataMongodbQuery<T> createQueryFor(Predicate predicate) {
		return createQuery().where(predicate);
	}

	/**
	 * Creates a {@link SpringDataMongodbQuery}.
	 *
	 * @return
	 */
	private SpringDataMongodbQuery<T> createQuery() {
		return new SpringDataMongodbQuery<>(mongoOperations, typeInformation().getJavaType());
	}

	/**
	 * Applies the given {@link Pageable} to the given {@link SpringDataMongodbQuery}.
	 *
	 * @param query
	 * @param pageable
	 * @return
	 */
	private SpringDataMongodbQuery<T> applyPagination(SpringDataMongodbQuery<T> query, Pageable pageable) {

		if (pageable.isUnpaged()) {
			return query;
		}

		query = query.offset(pageable.getOffset()).limit(pageable.getPageSize());
		return applySorting(query, pageable.getSort());
	}

	/**
	 * Applies the given {@link Sort} to the given {@link SpringDataMongodbQuery}.
	 *
	 * @param query
	 * @param sort
	 * @return
	 */
	private SpringDataMongodbQuery<T> applySorting(SpringDataMongodbQuery<T> query, Sort sort) {

		toOrderSpecifiers(sort).forEach(query::orderBy);
		return query;
	}

	/**
	 * {@link org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery} using Querydsl
	 * {@link Predicate}.
	 *
	 * @author Mark Paluch
	 * @since 3.3
	 */
	class FluentQuerydsl<T> extends FetchableFluentQuerySupport<Predicate, T> {

		FluentQuerydsl(Predicate predicate, Class<T> resultType) {
			this(predicate, Sort.unsorted(), 0, resultType, Collections.emptyList());
		}

		FluentQuerydsl(Predicate predicate, Sort sort, int limit, Class<T> resultType, List<String> fieldsToInclude) {
			super(predicate, sort, limit, resultType, fieldsToInclude);
		}

		@Override
		protected <R> FluentQuerydsl<R> create(Predicate predicate, Sort sort, int limit, Class<R> resultType,
				List<String> fieldsToInclude) {
			return new FluentQuerydsl<>(predicate, sort, limit, resultType, fieldsToInclude);
		}

		@Override
		public T oneValue() {
			return createQuery().fetchOne();
		}

		@Override
		public T firstValue() {
			return createQuery().fetchFirst();
		}

		@Override
		public List<T> all() {
			return createQuery().fetch();
		}

		@Override
		public Window<T> scroll(ScrollPosition scrollPosition) {
			return createQuery().scroll(scrollPosition);
		}

		@Override
		public Page<T> page(Pageable pageable) {

			Assert.notNull(pageable, "Pageable must not be null");

			return createQuery().fetchPage(pageable);
		}

		@Override
		public Stream<T> stream() {
			return createQuery().stream();
		}

		@Override
		public long count() {
			return createQuery().fetchCount();
		}

		@Override
		public boolean exists() {
			return count() > 0;
		}

		private SpringDataMongodbQuery<T> createQuery() {
			return new SpringDataMongodbQuery<>(mongoOperations, typeInformation().getJavaType(), getResultType(),
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
