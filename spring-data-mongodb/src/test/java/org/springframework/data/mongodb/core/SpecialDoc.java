package org.springframework.data.mongodb.core;

public class SpecialDoc extends BaseDoc {
	String specialValue;

	public SpecialDoc() {}

	public SpecialDoc(String value) {
		super(value);
	}
}
