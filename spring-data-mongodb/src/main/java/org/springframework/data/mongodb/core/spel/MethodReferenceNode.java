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

import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.ast.MethodReference;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An {@link ExpressionNode} representing a method reference.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Sebastien Gerard
 */
public class MethodReferenceNode extends ExpressionNode {

	private static final Map<String, String> FUNCTIONS;

	static {
		Map<String, String> map = new HashMap<String, String>();

		map.put("and", "$and"); // Returns true only when all its expressions evaluate to true.
		map.put("or", "$or"); // Returns true when any of its expressions evaluates to true.
		map.put("not", "$not"); // Returns the boolean value that is the opposite of its argument expression.

		map.put("setEquals", "$setEquals"); // Returns true if the input sets have the same distinct elements.
		map.put("setIntersection", "$setIntersection"); // Returns a set with elements that appear in all of the input sets.
		map.put("setUnion", "$setUnion"); // Returns a set with elements that appear in any of the input sets.
		map.put("setDifference", "$setDifference"); // Returns a set with elements that appear in the 1st set but not in the
		// 2nd.
		map.put("setIsSubset", "$setIsSubset"); // Returns true if all elements of the 1st set appear in the 2nd set.
		map.put("anyElementTrue", "$anyElementTrue"); // Returns whether any elements of a set evaluate to true.
		map.put("allElementsTrue", "$allElementsTrue"); // Returns whether no element of a set evaluates to false.

		map.put("cmp", "$cmp"); // Returns: 0 if the two values are equivalent, 1 if the first value is greater than the
		// second, and -1 if the first value is less than the second.
		map.put("eq", "$eq"); // Returns true if the values are equivalent.
		map.put("gt", "$gt"); // Returns true if the first value is greater than the second.
		map.put("gte", "$gte"); // Returns true if the first value is greater than or equal to the second.
		map.put("lt", "$lt"); // Returns true if the first value is less than the second.
		map.put("lte", "$lte"); // Returns true if the first value is less than or equal to the second.
		map.put("ne", "$ne"); // Returns true if the values are not equivalent.

		map.put("abs", "$abs"); // Returns the absolute value of a number.;
		map.put("add", "$add"); // Adds numbers to return the sum, or adds numbers and a date to return a new date.
		map.put("ceil", "$ceil"); // Returns the smallest integer greater than or equal to the specified number.
		map.put("divide", "$divide"); // Returns the result of dividing the first number by the second.
		map.put("exp", "$exp"); // Raises e to the specified exponent.
		map.put("floor", "$floor"); // Returns the largest integer less than or equal to the specified number.
		map.put("ln", "$ln"); // Calculates the natural log of a number.
		map.put("log", "$log"); // Calculates the log of a number in the specified base.
		map.put("log10", "$log10"); // Calculates the log base 10 of a number.
		map.put("mod", "$mod"); // Returns the remainder of the first number divided by the second.
		map.put("multiply", "$multiply"); // Multiplies numbers to return the product.
		map.put("pow", "$pow"); // Raises a number to the specified exponent.
		map.put("sqrt", "$sqrt"); // Calculates the square root.
		map.put("subtract", "$subtract"); // Returns the result of subtracting the second value from the first. If the
		// two values are numbers, return the difference. If the two values are dates, return the difference in
		// milliseconds.
		map.put("trunc", "$trunc"); // Truncates a number to its integer.

		map.put("concat", "$concat"); // Concatenates two strings.
		map.put("substr", "$substr"); // Takes a string and returns portion of that string.
		map.put("toLower", "$toLower"); // Converts a string to lowercase.
		map.put("toUpper", "$toUpper"); // Converts a string to uppercase.
		map.put("strcasecmp", "$strcasecmp"); // Compares two strings and returns an integer that reflects the comparison.

		map.put("meta", "$meta"); // Access text search metadata.

		map.put("arrayElemAt", "$arrayElemAt"); // Returns the element at the specified array index.
		map.put("concatArrays", "$concatArrays"); // Concatenates arrays to return the concatenated array.
		map.put("filter", "$filter"); // Selects a subset of the array to return an array with only the elements that
		// match the filter condition.
		map.put("isArray", "$isArray"); // Determines if the operand is an array. Returns a boolean.
		map.put("size", "$size"); // Returns the number of elements in the array.
		map.put("slice", "$slice"); // Returns a subset of an array.

		map.put("map", "$map"); // Applies a subexpression to each element of an array and returns the array of
		// resulting values in order.
		map.put("let", "$let"); // Defines variables for use within the scope of a subexpression and returns the result
		// of the subexpression.

		map.put("literal", "$literal"); // Return a value without parsing.

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
		// 999.
		map.put("dateToString", "$dateToString"); // Returns the date as a formatted string.

		map.put("cond", "$cond"); // A ternary operator that evaluates one expression, and depending on the result,
		// returns the value of one of the other two expressions.
		map.put("ifNull", "$ifNull"); // Returns either the non-null result of the first expression or the result of the
		// second expression if the first expression results in a null result.

		FUNCTIONS = Collections.unmodifiableMap(map);
	}

	MethodReferenceNode(MethodReference reference, ExpressionState state) {
		super(reference, state);
	}

	/**
	 * Returns the name of the method.
	 */
	public String getMethodName() {
		String name = getName();
		String methodName = name.substring(0, name.indexOf('('));
		return FUNCTIONS.get(methodName);
	}
}
