package org.springframework.data.mongodb.query;

public class NorQuery extends Query {

	public NorQuery(Query... q) {
		super.nor(q);
	}

}
