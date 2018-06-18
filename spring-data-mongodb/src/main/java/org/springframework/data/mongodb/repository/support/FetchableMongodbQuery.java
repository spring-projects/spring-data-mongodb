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

import javax.annotation.Nullable;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
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

/**
 * {@link Fetchable} Mongodb query with a pluggable Document to Bean transformation.
 *
 * @param <K> result type
 * @param <Q> concrete subtype
 * @author Mark Paluch
 */
abstract class FetchableMongodbQuery<K, Q extends FetchableMongodbQuery<K, Q>> extends AbstractMongodbQuery<Q>
		implements Fetchable<K> {

	private final Class<K> entityClass;
	private final String collection;
	private final MongoOperations mongoOperations;

	public FetchableMongodbQuery(MongodbDocumentSerializer serializer, Class<? extends K> entityClass,
			MongoOperations mongoOperations) {

		super(serializer);

		this.entityClass = (Class<K>) entityClass;
		this.collection = mongoOperations.getCollectionName(entityClass);
		this.mongoOperations = mongoOperations;
	}

	public FetchableMongodbQuery(MongodbDocumentSerializer serializer, Class<? extends K> entityClass, String collection,
			MongoOperations mongoOperations) {

		super(serializer);

		this.entityClass = (Class<K>) entityClass;
		this.collection = collection;
		this.mongoOperations = mongoOperations;
	}

	/**
	 * Iterate with the specific fields
	 *
	 * @param paths fields to return
	 * @return iterator
	 */
	public CloseableIterator<K> iterate(Path<?>... paths) {
		getQueryMixin().setProjection(paths);
		return iterate();
	}

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

			}

			@Override
			public void close() {
				stream.close();
			}
		};
	}

	/**
	 * Fetch with the specific fields
	 *
	 * @param paths fields to return
	 * @return results
	 */
	public List<K> fetch(Path<?>... paths) {
		getQueryMixin().setProjection(paths);
		return fetch();
	}

	@Override
	public List<K> fetch() {
		return mongoOperations.query(entityClass).matching(createQuery()).all();
	}

	/**
	 * Fetch first with the specific fields
	 *
	 * @param paths fields to return
	 * @return first result
	 */
	public K fetchFirst(Path<?>... paths) {
		getQueryMixin().setProjection(paths);
		return fetchFirst();
	}

	@Override
	public K fetchFirst() {
		return mongoOperations.query(entityClass).matching(createQuery()).firstValue();
	}

	/**
	 * Fetch one with the specific fields
	 *
	 * @param paths fields to return
	 * @return first result
	 */
	public K fetchOne(Path<?>... paths) {
		getQueryMixin().setProjection(paths);
		return fetchOne();
	}

	@Override
	public K fetchOne() {
		return mongoOperations.query(entityClass).matching(createQuery()).oneValue();

	}

	/**
	 * Fetch results with the specific fields
	 *
	 * @param paths fields to return
	 * @return results
	 */
	public QueryResults<K> fetchResults(Path<?>... paths) {
		getQueryMixin().setProjection(paths);
		return fetchResults();
	}

	@Override
	public QueryResults<K> fetchResults() {
		long total = fetchCount();
		if (total > 0L) {
			return new QueryResults<>(fetch(), getQueryMixin().getMetadata().getModifiers(), total);
		} else {
			return QueryResults.emptyResults();
		}
	}

	@Override
	public long fetchCount() {
		return mongoOperations.query(entityClass).matching(createQuery()).count();
	}

	protected org.springframework.data.mongodb.core.query.Query createQuery() {
		QueryMetadata metadata = getQueryMixin().getMetadata();
		Predicate filter = createFilter(metadata);
		return createQuery(filter, metadata.getProjection(), metadata.getModifiers(), metadata.getOrderBy());
	}

	protected org.springframework.data.mongodb.core.query.Query createQuery(@Nullable Predicate where,
			Expression<?> projection, QueryModifiers modifiers, List<OrderSpecifier<?>> orderBy) {

		BasicQuery basicQuery = new BasicQuery(createQuery(where), createProjection(projection));

		Integer limit = modifiers.getLimitAsInteger();
		Integer offset = modifiers.getOffsetAsInteger();

		if (limit != null) {
			basicQuery.limit(limit);
		}
		if (offset != null) {
			basicQuery.skip(offset);
		}
		if (orderBy.size() > 0) {
			basicQuery.setSortObject(getSerializer().toSort(orderBy));
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
		Multimap<Expression<?>, Predicate> predicates = HashMultimap.create();
		List<JoinExpression> joins = metadata.getJoins();
		for (int i = joins.size() - 1; i >= 0; i--) {
			JoinExpression join = joins.get(i);
			Path<?> source = (Path) ((Operation<?>) join.getTarget()).getArg(0);
			Path<?> target = (Path) ((Operation<?>) join.getTarget()).getArg(1);
			Collection<Predicate> extraFilters = predicates.get(target.getRoot());
			Predicate filter = ExpressionUtils.allOf(join.getCondition(), allOf(extraFilters));
			List<? extends Object> ids = getIds(target.getType(), filter);
			if (ids.isEmpty()) {
				throw new NoResults();
			}
			Path<?> path = ExpressionUtils.path(String.class, source, "$id");
			predicates.put(source.getRoot(), ExpressionUtils.in((Path<Object>) path, ids));
		}
		Path<?> source = (Path) ((Operation) joins.get(0).getTarget()).getArg(0);
		return allOf(predicates.get(source.getRoot()));
	}

	private Predicate allOf(Collection<Predicate> predicates) {
		return predicates != null ? ExpressionUtils.allOf(predicates) : null;
	}

	protected List<Object> getIds(Class<?> targetType, Predicate condition) {
		// TODO : fetch only ids
		Query query = createQuery(condition, null, QueryModifiers.EMPTY, Collections.emptyList());

		return mongoOperations.findDistinct(query, "_id", targetType, Object.class);
	}
}
