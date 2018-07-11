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

import java.util.List;

import org.bson.Document;
import org.springframework.lang.Nullable;

import com.querydsl.core.DefaultQueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.SimpleQuery;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.FactoryExpression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Predicate;

/**
 * {@code QuerydslAbstractMongodbQuery} provides a base class for general Querydsl query implementation.
 * <p>
 * Original implementation source {@link com.querydsl.mongodb.AbstractMongodbQuery} by {@literal The Querydsl Team}
 * (<a href="http://www.querydsl.com/team">http://www.querydsl.com/team</a>) licensed under the Apache License, Version
 * 2.0.
 * </p>
 * Modified for usage with {@link MongodbDocumentSerializer}.
 * 
 * @param <Q> concrete subtype
 * @author laimw
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public abstract class QuerydslAbstractMongodbQuery<K, Q extends QuerydslAbstractMongodbQuery<K, Q>>
		implements SimpleQuery<Q> {

	private final MongodbDocumentSerializer serializer;
	private final QueryMixin<Q> queryMixin;

	/**
	 * Create a new MongodbQuery instance
	 *
	 * @param serializer serializer
	 */
	@SuppressWarnings("unchecked")
	QuerydslAbstractMongodbQuery(MongodbDocumentSerializer serializer) {

		this.queryMixin = new QueryMixin<>((Q) this, new DefaultQueryMetadata(), false);
		this.serializer = serializer;
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#distinct()
	 */
	@Override
	public Q distinct() {
		return queryMixin.distinct();
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.FilteredClause#where(com.querydsl.core.types.Predicate[])
	 */
	@Override
	public Q where(Predicate... e) {
		return queryMixin.where(e);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#limit(long)
	 */
	@Override
	public Q limit(long limit) {
		return queryMixin.limit(limit);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#offset()
	 */
	@Override
	public Q offset(long offset) {
		return queryMixin.offset(offset);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#restrict(com.querydsl.core.QueryModifiers)
	 */
	@Override
	public Q restrict(QueryModifiers modifiers) {
		return queryMixin.restrict(modifiers);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#orderBy(com.querydsl.core.types.OrderSpecifier)
	 */
	@Override
	public Q orderBy(OrderSpecifier<?>... o) {
		return queryMixin.orderBy(o);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.core.SimpleQuery#set(com.querydsl.core.types.ParamExpression, Object)
	 */
	@Override
	public <T> Q set(ParamExpression<T> param, T value) {
		return queryMixin.set(param, value);
	}

	/**
	 * Compute the actual projection {@link Document} from a given projectionExpression by serializing the contained
	 * {@link Expression expressions} individually.
	 *
	 * @param projectionExpression the computed projection {@link Document}.
	 * @return never {@literal null}. An empty {@link Document} by default.
	 * @see MongodbDocumentSerializer#handle(Expression)
	 */
	protected Document createProjection(@Nullable Expression<?> projectionExpression) {

		if (!(projectionExpression instanceof FactoryExpression)) {
			return new Document();
		}

		Document projection = new Document();
		((FactoryExpression<?>) projectionExpression).getArgs().stream() //
				.filter(Expression.class::isInstance) //
				.map(Expression.class::cast) //
				.map(serializer::handle) //
				.forEach(it -> projection.append(it.toString(), 1));

		return projection;
	}

	/**
	 * Compute the filer {@link Document} from the given {@link Predicate}.
	 *
	 * @param predicate can be {@literal null}.
	 * @return an empty {@link Document} if predicate is {@literal null}.
	 * @see MongodbDocumentSerializer#toQuery(Predicate)
	 */
	protected Document createQuery(@Nullable Predicate predicate) {

		if (predicate == null) {
			return new Document();
		}

		return serializer.toQuery(predicate);
	}

	/**
	 * Compute the sort {@link Document} from the given list of {@link OrderSpecifier order specifiers}.
	 *
	 * @param orderSpecifiers can be {@literal null}.
	 * @return an empty {@link Document} if predicate is {@literal null}.
	 * @see MongodbDocumentSerializer#toSort(List)
	 */
	protected Document createSort(List<OrderSpecifier<?>> orderSpecifiers) {
		return serializer.toSort(orderSpecifiers);
	}

	/**
	 * Get the actual {@link QueryMixin} delegate.
	 * 
	 * @return
	 */
	QueryMixin<Q> getQueryMixin() {
		return queryMixin;
	}

	/**
	 * Get the where definition as a Document instance
	 *
	 * @return
	 */
	Document asDocument() {
		return createQuery(queryMixin.getMetadata().getWhere());
	}

	@Override
	public String toString() {
		return asDocument().toString();
	}
}
