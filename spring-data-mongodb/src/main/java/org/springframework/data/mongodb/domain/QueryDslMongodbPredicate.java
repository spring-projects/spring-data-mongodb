package org.springframework.data.mongodb.domain;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.mongodb.core.mapping.DBRef;

import com.mysema.query.mongodb.MongodbQuery;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;

public class QueryDslMongodbPredicate<T> {
	PredicateBuilder<T> predicateBuilder;
	
	QueryDslMongodbPredicate(PredicateBuilder<T> predicateBuilder) {
		this.predicateBuilder = predicateBuilder;
	}

	Predicate predicate;
	List<Join<?>> joins = new ArrayList<Join<?>>();
	
	private <K> Join<T> addJoin(Path<K> ref, Path<K> target){
		Join<T> join = new Join<T>(ref, target,this);
		joins.add(join);
		return join;
	}
	
	/**
	 * Create a pathway to {@link DBRef} properties on {@link T} document
	 * 
	 * @param ref
	 * @param target
	 * @return
	 */
	public <K> Join<T> join(Path<K> ref, Path<K> target) {
		return addJoin(ref, target);
	}
	
	public void applyPredicate(MongodbQuery<T> mongodbQuery) {
		for (Join<?> join : joins) {
			mongodbQuery.join(join.getRef(), join.getTarget()).on(join.getPredicate());
		}
		if(predicate != null){
			mongodbQuery.where(predicate);
		}
	}

}
