package org.springframework.data.mongodb.core.mapreduce;

public class XObject {

	private float x;
	
	private float count;


	public float getX() {
		return x;
	}


	public void setX(float x) {
		this.x = x;
	}


	public float getCount() {
		return count;
	}


	public void setCount(float count) {
		this.count = count;
	}


	@Override
	public String toString() {
		return "XObject [x=" + x + " count = " + count + "]";
	}
	
}
