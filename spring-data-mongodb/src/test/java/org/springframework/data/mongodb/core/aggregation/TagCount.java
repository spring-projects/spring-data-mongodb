package org.springframework.data.mongodb.core.aggregation;

/**
 * Simple value object holding the aggregation result.
 * 
 * @author Tobias Trelle
 */
public class TagCount {

	private String tag;

	private int n;

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

}
