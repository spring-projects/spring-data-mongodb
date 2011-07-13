package org.springframework.data.mongodb.core.query;

public class NorQuery extends Query {

	public NorQuery(Query... q) {
		super.nor(q);
	}

}
