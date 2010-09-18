package org.springframework.datastore.document.mongodb.query;


public class SortSpecification {
	
	public enum SortOrder {
		ASCENDING, DESCENDING
	}
	
	public SortSpecification on(String key, SortOrder order) {
		return this;
	}

}
