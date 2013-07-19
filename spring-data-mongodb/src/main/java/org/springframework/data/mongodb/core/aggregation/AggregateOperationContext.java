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

import java.util.Map;

/**
 * A {@code AggregateOperationContext} holds information about available fields for the aggregation steps.
 * 
 * @author Thomas Darimont
 */
interface AggregateOperationContext {

	Map<String, String> getAvailableFields();

	/**
	 * @param fieldName
	 * @return the alias for the given fieldName if present in available fields. If the given field is not available the
	 *         given fieldName is return instead.
	 */
	String returnFieldNameAliasIfAvailableOr(String fieldName);

	/**
	 * @param fieldName
	 * @return true if the a field with the given field name is available.
	 */
	boolean isFieldAvailable(String fieldName);

	/**
	 * Registers a field with the given {@code fieldName} as available field.
	 * 
	 * @param fieldName
	 */
	void registerAvailableField(String fieldName);

	/**
	 * Registers a field with the given {@code fieldName} as field available with the given {@code availableFieldName} as
	 * an alias.
	 * 
	 * @param fieldName
	 */
	void registerAvailableField(String fieldName, String availableFieldName);

	/**
	 * Removes the field with the given fieldName from the available fields.
	 * 
	 * @param fieldName
	 */
	void unregisterAvailableField(String fieldName);
}
