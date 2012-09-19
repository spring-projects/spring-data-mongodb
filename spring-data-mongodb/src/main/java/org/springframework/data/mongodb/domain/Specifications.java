package org.springframework.data.mongodb.domain;

public class Specifications<T>  implements Specification<T> {
	
	private Specification<T> specification;
	
	public Specifications(Specification<T> specification) {
		this.specification = specification;
	}
	
	public static <T> Specifications<T> where(Specification<T> specification) {
		return new Specifications<T>(specification);
	}
	public Specifications<T> and(final Specification<T> spec) {
		return new Specifications<T>(new Specification<T>() {
			public QueryDslMongodbPredicate<T> buildPredicate(PredicateBuilder<T> predicateBuilder) {
				return predicateBuilder.and(specification.buildPredicate(predicateBuilder), spec.buildPredicate(predicateBuilder));
			}
		});
	}

	public QueryDslMongodbPredicate<T> buildPredicate(PredicateBuilder<T> predicateBuilder) {
			return specification.buildPredicate(predicateBuilder);
	}

}
