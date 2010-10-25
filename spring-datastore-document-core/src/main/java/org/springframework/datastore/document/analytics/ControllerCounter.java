package org.springframework.datastore.document.analytics;

import java.util.Date;
import java.util.Map;

public class ControllerCounter {
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getCount() {
		return count;
	}

	public void setCount(double count) {
		this.count = count;
	}

	public Map<String, Double> getMethods() {
		return methods;
	}
	
	

	public void setMethods(Map<String, Double> methods) {
		this.methods = methods;
	}

	private String name;
	
	private double count;
	
	private Map<String, Double> methods;
	
	@Override
	public String toString() {
		return "ControllerCounter [name=" + name + ", count=" + count
				+ ", methods=" + methods + "]";
	}	
}
