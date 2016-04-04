package org.springframework.data.mongodb.core;

import org.bson.Document;
import org.springframework.core.convert.converter.Converter;

public class PersonWriteConverter implements Converter<Person, Document> {

	public Document convert(Person source) {
		Document dbo = new Document();
		dbo.put("_id", source.getId());
		dbo.put("name", source.getFirstName());
		dbo.put("age", source.getAge());
		return dbo;
	}

}
