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
package org.springframework.data.mongodb.core.spel;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.ast.MethodReference;

/**
 * An {@link ExpressionNode} representing a method reference.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class MethodReferenceNode extends ExpressionNode {

	private static final Map<String, String> FUNCTIONS;

	static {

		Map<String, String> map = new HashMap<String, String>();

		map.put("concat", "$concat"); // Concatenates two strings.
		map.put("strcasecmp", "$strcasecmp"); // Compares two strings and returns an integer that reflects the comparison.
		map.put("substr", "$substr"); // Takes a string and returns portion of that string.
		map.put("toLower", "$toLower"); // Converts a string to lowercase.
		map.put("toUpper", "$toUpper"); // Converts a string to uppercase.

		map.put("dayOfYear", "$dayOfYear"); // Converts a date to a number between 1 and 366.
		map.put("dayOfMonth", "$dayOfMonth"); // Converts a date to a number between 1 and 31.
		map.put("dayOfWeek", "$dayOfWeek"); // Converts a date to a number between 1 and 7.
		map.put("year", "$year"); // Converts a date to the full year.
		map.put("month", "$month"); // Converts a date into a number between 1 and 12.
		map.put("week", "$week"); // Converts a date into a number between 0 and 53
		map.put("hour", "$hour"); // Converts a date into a number between 0 and 23.
		map.put("minute", "$minute"); // Converts a date into a number between 0 and 59.
		map.put("second", "$second"); // Converts a date into a number between 0 and 59. May be 60 to account for leap
		// seconds.
		map.put("millisecond", "$millisecond"); // Returns the millisecond portion of a date as an integer between 0 and

		FUNCTIONS = Collections.unmodifiableMap(map);
	}

	MethodReferenceNode(MethodReference reference, ExpressionState state) {
		super(reference, state);
	}

	/**
	 * Returns the name of the method.
	 * 
	 * @return
	 */
	public String getMethodName() {

		String name = getName();
		String methodName = name.substring(0, name.indexOf('('));
		return FUNCTIONS.get(methodName);
	}
}
