package org.springframework.data.document.mongodb.query;

import com.mongodb.DBObject;

public interface IndexSpecification {

	DBObject getIndexObject();
	
	DBObject getIndexOptions();

}
