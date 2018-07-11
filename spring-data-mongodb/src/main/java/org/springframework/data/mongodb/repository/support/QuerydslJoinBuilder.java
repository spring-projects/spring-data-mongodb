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

import com.querydsl.core.JoinType;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;

/**
 * {@code QuerydslJoinBuilder} is a builder for join constraints.
 * <p>
 * Original implementation source {@link com.querydsl.mongodb.JoinBuilder} by {@literal The Querydsl Team}
 * (<a href="http://www.querydsl.com/team">http://www.querydsl.com/team</a>) licensed under the Apache License, Version
 * 2.0.
 * </p>
 * Modified for usage with {@link QuerydslAbstractMongodbQuery}.
 *
 * @param <Q>
 * @param <T>
 * @author tiwe
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public class QuerydslJoinBuilder<Q extends QuerydslAbstractMongodbQuery<K, Q>, K, T> {

	private final QueryMixin<Q> queryMixin;
	private final Path<?> ref;
	private final Path<T> target;

	QuerydslJoinBuilder(QueryMixin<Q> queryMixin, Path<?> ref, Path<T> target) {

		this.queryMixin = queryMixin;
		this.ref = ref;
		this.target = target;
	}

	/**
	 * Add the given join conditions.
	 *
	 * @param conditions must not be {@literal null}.
	 * @return the target {@link QueryMixin}.
	 * @see QueryMixin#on(Predicate)
	 */
	@SuppressWarnings("unchecked")
	public Q on(Predicate... conditions) {

		queryMixin.addJoin(JoinType.JOIN, ExpressionUtils.as((Path) ref, target));
		queryMixin.on(conditions);
		return queryMixin.getSelf();
	}
}
