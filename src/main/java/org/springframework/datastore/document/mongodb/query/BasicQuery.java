package org.springframework.datastore.document.mongodb.query;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class BasicQuery implements Query {

	private DBObject dbo = null;
	
	
	public BasicQuery(String query) {
		super();
		this.dbo = (DBObject) JSON.parse(query);
	}
	
	public BasicQuery(DBObject dbo) {
		super();
		this.dbo = dbo;
	}

	public DBObject getQueryObject() {
		return dbo;
	}

}
