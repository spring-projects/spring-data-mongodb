package org.springframework.datastore.document.mongodb;

public class CollectionOptions {
	
	private Integer maxDocuments;
	
	private Integer size;
	
	private Boolean capped;
	
	
	
	public CollectionOptions(Integer size, Integer maxDocuments, Boolean capped) {
		super();
		this.maxDocuments = maxDocuments;
		this.size = size;
		this.capped = capped;
	}

	public Integer getMaxDocuments() {
		return maxDocuments;
	}

	public void setMaxDocuments(Integer maxDocuments) {
		this.maxDocuments = maxDocuments;
	}

	public Integer getSize() {
		return size;
	}

	public void setSize(Integer size) {
		this.size = size;
	}

	public Boolean getCapped() {
		return capped;
	}

	public void setCapped(Boolean capped) {
		this.capped = capped;
	}
	
	
}
