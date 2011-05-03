package org.springframework.data.document.mongodb.query;

public class NorQuery extends Query {

	public NorQuery(Query... q) {
		super.nor(q);
	}

}
