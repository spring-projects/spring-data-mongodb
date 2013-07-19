/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Map based implementation of {@link AggregateOperationContext}.
 * 
 * @author Thomas Darimont
 */
public class BasicAggregateOperationContext implements AggregateOperationContext {

	private Map<String, String> availableFields = new LinkedHashMap<String, String>();

	@Override
	public Map<String, String> getAvailableFields() {
		return new HashMap<String, String>(getAvailableFieldsInternal());
	}

	protected Map<String, String> getAvailableFieldsInternal() {
		return this.availableFields;
	}

	@Override
	public void registerAvailableField(String fieldName) {
		registerAvailableField(fieldName, fieldName);
	}

	@Override
	public void registerAvailableField(String fieldName, String availableFieldName) {
		getAvailableFieldsInternal().put(fieldName, availableFieldName);
	}

	public String returnFieldNameAliasIfAvailableOr(String fieldName) {
		return isFieldAvailable(fieldName) ? getAvailableFieldsInternal().get(fieldName) : fieldName;
	}

	public boolean isFieldAvailable(String fieldName) {
		return getAvailableFieldsInternal().containsKey(ReferenceUtil.safeNonReference(fieldName));
	}

	@Override
	public void unregisterAvailableField(String fieldName) {
		getAvailableFieldsInternal().remove(fieldName);
	}
}
