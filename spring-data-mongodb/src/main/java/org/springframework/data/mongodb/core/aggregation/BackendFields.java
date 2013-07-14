package org.springframework.data.mongodb.core.aggregation;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link Fields}
 * 
 * @author Thomas Darimont
 */
class BackendFields implements Fields {

	private Map<String, Object> values = new HashMap<String, Object>();

	/**
	 * @param names
	 */
	public BackendFields(String... names) {
		for (String name : names) {
			and(name);
		}
	}

	/**
	 * @param name
	 * @return
	 */
	public Fields and(String name) {
		return and(name, ReferenceUtil.safeReference(name));
	}

	/**
	 * @param name
	 * @param value
	 * @return
	 */
	public Fields and(String name, Object value) {
		this.values.put(name, value);
		return this;
	}

	/**
	 * @return
	 */
	public Map<String, Object> getValues() {
		return new HashMap<String, Object>(this.values);
	}
}
