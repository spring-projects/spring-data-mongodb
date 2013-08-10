package org.springframework.data.mongodb.core.aggregation;

class ZipInfoStats {

	String id;
	String state;
	City biggestCity;
	City smallestCity;

	public String toString() {
		return "ZipInfoStats [id=" + id + ", state=" + state + ", biggestCity=" + biggestCity + ", smallestCity="
				+ smallestCity + "]";
	}
}
