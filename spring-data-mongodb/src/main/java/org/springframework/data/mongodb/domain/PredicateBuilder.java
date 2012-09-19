package org.springframework.data.mongodb.domain;

import com.mysema.query.types.ExpressionUtils;
import com.mysema.query.types.Predicate;

public class PredicateBuilder<T> {
	
	private QueryDslMongodbPredicate<T> queryDslMongodbPredicate;
	
	/**
	 * Apply {@link Predicate}s (if any) on query to filter on {@link T} document properties
	 * 
	 * @param predicate
	 * @return
	 */
	public QueryDslMongodbPredicate<T> where(Predicate... predicate) {
		queryDslMongodbPredicate = new QueryDslMongodbPredicate<T>(this);
		queryDslMongodbPredicate.predicate = ExpressionUtils.allOf(predicate);
		return queryDslMongodbPredicate;
	}
	
	/**
	 * Create a conjunction of the given expressions
	 * 
	 * @param first
	 * @param second
	 * @return
	 */
	public QueryDslMongodbPredicate<T> and(QueryDslMongodbPredicate<T> first,QueryDslMongodbPredicate<T> second) {
		queryDslMongodbPredicate = new QueryDslMongodbPredicate<T>(this);
		for (Join<?> join : first.joins) {
			queryDslMongodbPredicate.join(join.getRef(), join.getTarget()).on(join.getPredicate());
		}
		for (Join<?> join : second.joins) {
			queryDslMongodbPredicate.join(join.getRef(), join.getTarget()).on(join.getPredicate());
		}
		queryDslMongodbPredicate.predicate = ExpressionUtils.allOf(new Predicate[]{first.predicate,second.predicate});
		return queryDslMongodbPredicate;
	}
	
}
