package org.springframework.datastore.document.mongodb;

import com.mongodb.DBObject;

public interface MongoReader<T> {

	T read(Class<? extends T> clazz, DBObject dbo);
	//T read(DBObject dbo);
}
