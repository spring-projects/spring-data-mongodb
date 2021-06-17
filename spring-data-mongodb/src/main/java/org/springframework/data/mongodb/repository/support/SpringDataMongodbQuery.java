/*
 * Copyright 2012-2021 the original author or authors.
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

import org.bson.Document;

import org.springframework.data.mongodb.core.ExecutableFindOperation;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;

import com.mysema.commons.lang.CloseableIterator;
import com.mysema.commons.lang.EmptyCloseableIterator;
import com.querydsl.core.Fetchable;
import com.querydsl.core.QueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Predicate;
import com.querydsl.mongodb.document.MongodbDocumentSerializer;

/**
 * Spring Data specific simple {@link com.querydsl.core.Fetchable} {@link com.querydsl.core.SimpleQuery Query}
 * implementation.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class SpringDataMongodbQuery<T> extends SpringDataMongodbQuerySupport<SpringDataMongodbQuery<T>>
		implements Fetchable<T> {

	private final Class<T> entityClass;
	private final String collection;
	private final MongoOperations mongoOperations;
	private final ExecutableFindOperation.FindWithProjection<T> find;

	/**
	 * Creates a new {@link SpringDataMongodbQuery}.
	 *
	 * @param operations must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 */
	public SpringDataMongodbQuery(MongoOperations operations, Class<? extends T> type) {
		this(operations, type, operations.getCollectionName(type));
	}

	/**
	 * Creates a new {@link SpringDataMongodbQuery} to query the given collection.
	 *
	 * @param operations must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 */
	public SpringDataMongodbQuery(MongoOperations operations, Class<? extends T> type,
			String collectionName) {
		this(new SpringDataMongodbSerializer(operations.getConverter()), operations, type, collectionName);
	}

	private SpringDataMongodbQuery(MongodbDocumentSerializer serializer, MongoOperations operations,
			Class<? extends T> type, String collectionName) {

		super(serializer);

		this.entityClass = (Class<T>) type;
		this.collection = collectionName;
		this.mongoOperations = operations;
		this.find = mongoOperations.query(this.entityClass).inCollection(collection);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#iterable()
	 */
	@Override
	public CloseableIterator<T> iterate() {

		try {
			org.springframework.data.util.CloseableIterator<? extends T> stream = mongoOperations.stream(createQuery(),
					entityClass, collection);

			return new CloseableIterator<T>() {

				@Override
				public boolean hasNext() {
					return stream.hasNext();
				}

				@Override
				public T next() {
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
		} catch (RuntimeException e) {
			return handleException(e, new EmptyCloseableIterator<>());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetch()
	 */
	@Override
	public List<T> fetch() {
		try {
			return find.matching(createQuery()).all();
		} catch (RuntimeException e) {
			return handleException(e, Collections.emptyList());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchFirst()
	 */
	@Override
	public T fetchFirst() {
		try {
			return find.matching(createQuery()).firstValue();
		} catch (RuntimeException e) {
			return handleException(e, null);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchOne()
	 */
	@Override
	public T fetchOne() {
		try {
			return find.matching(createQuery()).oneValue();
		} catch (RuntimeException e) {
			return handleException(e, null);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.Fetchable#fetchResults()
	 */
	@Override
	public QueryResults<T> fetchResults() {

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
		try {
			return find.matching(Query.of(createQuery()).skip(-1).limit(-1)).count();
		} catch (RuntimeException e) {
			return handleException(e, 0L);
		}
	}

	protected org.springframework.data.mongodb.core.query.Query createQuery() {

		QueryMetadata metadata = getQueryMixin().getMetadata();

		return createQuery(createFilter(metadata), metadata.getProjection(), metadata.getModifiers(),
				metadata.getOrderBy());
	}

	protected org.springframework.data.mongodb.core.query.Query createQuery(@Nullable Predicate filter,
			@Nullable Expression<?> projection, QueryModifiers modifiers, List<OrderSpecifier<?>> orderBy) {

		Document fields = createProjection(projection);
		BasicQuery basicQuery = new BasicQuery(createQuery(filter), fields == null ? new Document() : fields);

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

	private static <T> T handleException(RuntimeException e, T defaultValue) {

		if (e.getClass().getName().endsWith("$NoResults")) {
			return defaultValue;
		}

		throw e;
	}

}
