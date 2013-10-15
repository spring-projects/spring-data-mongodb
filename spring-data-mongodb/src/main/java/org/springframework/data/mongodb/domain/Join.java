package org.springframework.data.mongodb.domain;

import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;

public class Join<T> {


	private QueryDslMongodbPredicate<T> queryDslMongodbPredicate;
	private Path<?> ref;
	private Path<?> target;
	private Predicate[] predicate;
	
	<K> Join(Path<K> ref,
			Path<K> target,QueryDslMongodbPredicate<T> queryDslMongodbPredicate) {
		this.queryDslMongodbPredicate = queryDslMongodbPredicate;
		this.ref = ref;
		this.target = target;
	}

	
	/**
	 * Apply {@link Predicate} on query to filter {@link T} documents based on joined DBRef properties.  
	 * 
	 * @param conditions
	 * @return
	 */
	public QueryDslMongodbPredicate<T> on(Predicate... conditions) {
		predicate = conditions;
		return this.queryDslMongodbPredicate;
	}
	
	@SuppressWarnings("unchecked")
	<K> Path<K> getRef() {
		return (Path<K>) ref;
	}
	
	@SuppressWarnings("unchecked")
	<K> Path<K> getTarget() {
		return (Path<K>) target;
	}
	
	Predicate[] getPredicate() {
		return predicate;
	}
	
}