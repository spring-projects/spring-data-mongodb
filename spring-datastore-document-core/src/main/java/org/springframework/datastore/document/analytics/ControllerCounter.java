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

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public Map<String, Integer> getMethods() {
		return methods;
	}
	
	

	public void setMethods(Map<String, Integer> methods) {
		this.methods = methods;
	}

	private String name;
	
	private int count;
	
	private Map<String, Integer> methods;
	
	
}
