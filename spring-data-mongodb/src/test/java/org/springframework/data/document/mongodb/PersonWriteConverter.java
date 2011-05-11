package org.springframework.data.document.mongodb;

import org.springframework.core.convert.converter.Converter;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class PersonWriteConverter implements Converter<Person, DBObject> {

	public DBObject convert(Person source) {
		DBObject dbo = new BasicDBObject();
		dbo.put("_id", source.getId());
		dbo.put("name", source.getFirstName());
		dbo.put("age", source.getAge());
		return dbo;
	}

}
