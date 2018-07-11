/*
 * Copyright 2018 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithProjection;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedMultiValueMap;

import com.mysema.commons.lang.CloseableIterator;
import com.querydsl.core.Fetchable;
import com.querydsl.core.JoinExpression;
import com.querydsl.core.QueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Operation;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.CollectionPathBase;

/**
 * {@link Fetchable} MongoDB query with utilizing {@link MongoOperations} for command execution.
 *
 * @param <K> result type
 * @param <Q> concrete subtype
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
abstract class QuerydslFetchableMongodbQuery<K, Q extends QuerydslFetchableMongodbQuery<K, Q>>
		extends QuerydslAbstractMongodbQuery<K, Q> implements Fetchable<K> {

	private final Class<K> entityClass;
	private final String collection;
	private final MongoOperations mongoOperations;
	private final FindWithProjection<K> find;

	QuerydslFetchableMongodbQuery(MongodbDocumentSerializer serializer, Class<? extends K> entityClass, String collection,
			MongoOperations mongoOperations) {

		super(serializer);

		this.entityClass = (Class<K>) entityClass;
		this.collection = collection;
		this.mongoOperations = mongoOperations;
		find = mongoOperations.query(this.entityClass).inCollection(collection);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#iterable()
	 */
	@Override
	public CloseableIterator<K> iterate() {

		org.springframework.data.util.CloseableIterator<? extends K> stream = mongoOperations.stream(createQuery(),
				entityClass, collection);

		return new CloseableIterator<K>() {

			@Override
			public boolean hasNext() {
				return stream.hasNext();
			}

			@Override
			public K next() {
				return stream.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Cannot remove from iterator while streaming data.");
			}

			@Override
			public void close() {
				stream.close();
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetch()
	 */
	@Override
	public List<K> fetch() {
		return find.matching(createQuery()).all();
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchFirst()
	 */
	@Override
	public K fetchFirst() {
		return find.matching(createQuery()).firstValue();
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchOne()
	 */
	@Override
	public K fetchOne() {
		return find.matching(createQuery()).oneValue();
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchResults()
	 */
	@Override
	public QueryResults<K> fetchResults() {

		long total = fetchCount();
		return total > 0L ? new QueryResults<>(fetch(), getQueryMixin().getMetadata().getModifiers(), total)
				: QueryResults.emptyResults();
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchCount()
	 */
	@Override
	public long fetchCount() {
		return find.matching(createQuery()).count();
	}

	/**
	 * Define a join.
	 *
	 * @param ref reference
	 * @param target join target
	 * @return new instance of {@link QuerydslJoinBuilder}.
	 */
	public <T> QuerydslJoinBuilder<Q, K, T> join(Path<T> ref, Path<T> target) {
		return new QuerydslJoinBuilder<>(getQueryMixin(), ref, target);
	}

	/**
	 * Define a join.
	 *
	 * @param ref reference
	 * @param target join target
	 * @return new instance of {@link QuerydslJoinBuilder}.
	 */
	public <T> QuerydslJoinBuilder<Q, K, T> join(CollectionPathBase<?, T, ?> ref, Path<T> target) {
		return new QuerydslJoinBuilder<>(getQueryMixin(), ref, target);
	}

	/**
	 * Define a constraint for an embedded object.
	 *
	 * @param collection collection must not be {@literal null}.
	 * @param target target must not be {@literal null}.
	 * @return new instance of {@link QuerydslAnyEmbeddedBuilder}.
	 */
	public <T> QuerydslAnyEmbeddedBuilder<Q, K> anyEmbedded(Path<? extends Collection<T>> collection, Path<T> target) {
		return new QuerydslAnyEmbeddedBuilder<>(getQueryMixin(), collection);
	}

	protected org.springframework.data.mongodb.core.query.Query createQuery() {

		QueryMetadata metadata = getQueryMixin().getMetadata();

		return createQuery(createFilter(metadata), metadata.getProjection(), metadata.getModifiers(),
				metadata.getOrderBy());
	}

	protected org.springframework.data.mongodb.core.query.Query createQuery(@Nullable Predicate filter,
			@Nullable Expression<?> projection, QueryModifiers modifiers, List<OrderSpecifier<?>> orderBy) {

		BasicQuery basicQuery = new BasicQuery(createQuery(filter), createProjection(projection));

		Integer limit = modifiers.getLimitAsInteger();
		Integer offset = modifiers.getOffsetAsInteger();

		if (limit != null) {
			basicQuery.limit(limit);
		}
		if (offset != null) {
			basicQuery.skip(offset);
		}
		if (orderBy.size() > 0) {
			basicQuery.setSortObject(createSort(orderBy));
		}

		return basicQuery;
	}

	@Nullable
	protected Predicate createFilter(QueryMetadata metadata) {

		Predicate filter;
		if (!metadata.getJoins().isEmpty()) {
			filter = ExpressionUtils.allOf(metadata.getWhere(), createJoinFilter(metadata));
		} else {
			filter = metadata.getWhere();
		}
		return filter;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	protected Predicate createJoinFilter(QueryMetadata metadata) {

		LinkedMultiValueMap<Expression<?>, Predicate> predicates = new LinkedMultiValueMap<>();
		List<JoinExpression> joins = metadata.getJoins();

		for (int i = joins.size() - 1; i >= 0; i--) {

			JoinExpression join = joins.get(i);
			Path<?> source = (Path) ((Operation<?>) join.getTarget()).getArg(0);
			Path<?> target = (Path) ((Operation<?>) join.getTarget()).getArg(1);
			Collection<Predicate> extraFilters = predicates.get(target.getRoot());
			Predicate filter = ExpressionUtils.allOf(join.getCondition(), allOf(extraFilters));

			List<? extends Object> ids = getIds(target.getType(), filter);

			if (ids.isEmpty()) {
				return ExpressionUtils.predicate(QuerydslMongoOps.NO_MATCH, source);
			}

			Path<?> path = ExpressionUtils.path(String.class, source, "$id");
			predicates.add(source.getRoot(), ExpressionUtils.in((Path<Object>) path, ids));
		}

		Path<?> source = (Path) ((Operation) joins.get(0).getTarget()).getArg(0);
		return allOf(predicates.get(source.getRoot()));
	}

	private Predicate allOf(Collection<Predicate> predicates) {
		return predicates != null ? ExpressionUtils.allOf(predicates) : null;
	}

	/**
	 * Fetch the list of ids matching a given condition.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param condition must not be {@literal null}.
	 * @return empty {@link List} if none found.
	 */
	protected List<Object> getIds(Class<?> targetType, Predicate condition) {

		Query query = createQuery(condition, null, QueryModifiers.EMPTY, Collections.emptyList());
		return mongoOperations.findDistinct(query, "_id", targetType, Object.class);
	}
}
