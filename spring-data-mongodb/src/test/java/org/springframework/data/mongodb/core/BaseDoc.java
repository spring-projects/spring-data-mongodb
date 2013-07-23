package org.springframework.data.mongodb.core;

import org.springframework.data.annotation.Id;

public class BaseDoc {
	@Id String id;
	String value;
}
