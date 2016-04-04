package org.springframework.data.mongodb.core;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.core.convert.converter.Converter;

public class PersonReadConverter implements Converter<Document, Person> {

	public Person convert(Document source) {
		Person p = new Person((ObjectId) source.get("_id"), (String) source.get("name"));
		p.setAge((Integer) source.get("age"));
		return p;
	}

}
