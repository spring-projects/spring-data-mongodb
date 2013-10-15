package org.springframework.data.mongodb.domain;




public interface Specification<T> {
	
	/**
	 * Build WHERE clause based on related Document.
	 * 
	 * @param predicateBuilder
	 */
	QueryDslMongodbPredicate<T> buildPredicate(PredicateBuilder<T> predicateBuilder); 

}
