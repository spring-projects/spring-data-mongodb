package org.springframework.data.mongodb.core.mapreduce;

public class NumberAndVersion {

	private String id;
	private Long number;
	private Long version;
	private Long value;

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getNumber() {
		return number;
	}

	public void setNumber(Long number) {
		this.number = number;
	}

	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "NumberAndVersion [id=" + id + ", number=" + number + ", version=" + version + ", value=" + value + "]";
	}

}
