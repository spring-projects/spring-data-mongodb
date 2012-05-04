package org.springframework.data.mongodb.core.mapping;

public enum CascadeType {
	ALL,
	SAVE,
	DELETE,
	NONE;

	public boolean isSave() {
		return this == ALL || this == SAVE;
	}

	public boolean isDelete() {
		return this == ALL || this == DELETE;
	}
}
