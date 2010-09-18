package org.springframework.datastore.document.mongodb.query;

import com.mongodb.DBObject;


public interface Query {
	
	DBObject getQueryObject();
	
}
