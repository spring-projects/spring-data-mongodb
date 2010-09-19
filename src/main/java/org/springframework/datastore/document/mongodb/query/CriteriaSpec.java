package org.springframework.datastore.document.mongodb.query;

import com.mongodb.DBObject;

public interface CriteriaSpec {

	DBObject getCriteriaObject(String key);

}