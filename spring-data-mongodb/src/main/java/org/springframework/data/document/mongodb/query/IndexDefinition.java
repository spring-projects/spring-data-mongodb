package org.springframework.data.document.mongodb.query;

import com.mongodb.DBObject;

public interface IndexDefinition {

	DBObject getIndexObject();
	
	DBObject getIndexOptions();

}
