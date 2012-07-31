package org.springframework.data.mongodb.core.index;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class SampleEntity {

	@Id
	String id;

	@Indexed
	String prop;
}
