package org.springframework.data.mongodb.query;

public class OrQuery extends Query {

	public OrQuery(Query... q) {
		super.or(q);
	}

}
