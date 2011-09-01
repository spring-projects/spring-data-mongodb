package org.springframework.data.mongodb.core.mapreduce;

public class ValueObject {

	private String id;
	
	public String getId() {
		return id;
	}

	private float value;

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "ValueObject [id=" + id + ", value=" + value + "]";
	}


	
	
}
