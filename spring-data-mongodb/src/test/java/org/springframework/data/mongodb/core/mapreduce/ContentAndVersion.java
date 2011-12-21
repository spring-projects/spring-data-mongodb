package org.springframework.data.mongodb.core.mapreduce;

public class ContentAndVersion {

	private String id;
	
	private String document_id;
	
	private String content;
	
	private String author;

	private Long version;
	
	private Long value;
	
	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getDocumentId() {
		return document_id;
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

	public void setDocumentId(String documentId) {
		this.document_id = documentId;
	}


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "ContentAndVersion [id=" + id + ", document_id=" + document_id + ", content=" + content + ", author="
				+ author + ", version=" + version + ", value=" + value + "]";
	}
	

}
