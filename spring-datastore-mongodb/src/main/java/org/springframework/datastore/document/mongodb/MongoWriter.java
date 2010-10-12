package org.springframework.datastore.document.mongodb;

import com.mongodb.DBObject;

public interface MongoWriter<T> {

	void write(T t, DBObject dbo);
}
