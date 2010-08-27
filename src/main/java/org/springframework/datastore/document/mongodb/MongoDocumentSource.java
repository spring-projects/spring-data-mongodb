package org.springframework.datastore.document.mongodb;

import org.springframework.datastore.document.DocumentSource;

import com.mongodb.DBObject;

public interface MongoDocumentSource extends DocumentSource<DBObject> {
	
}
