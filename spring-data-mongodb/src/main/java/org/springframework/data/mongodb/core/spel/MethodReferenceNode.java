/*
 * Copyright 2013-2016 the original author or authors.
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

import static org.springframework.data.mongodb.core.spel.MethodReferenceNode.AggregationMethodReference.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.ast.MethodReference;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * An {@link ExpressionNode} representing a method reference.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Sebastien Gerard
 * @author Christoph Strobl
 */
public class MethodReferenceNode extends ExpressionNode {

	private static final Map<String, AggregationMethodReference> FUNCTIONS;

	static {

		Map<String, AggregationMethodReference> map = new HashMap<String, AggregationMethodReference>();

		// BOOLEAN OPERATORS
		map.put("and", arrayArgumentAggregationMethodReference().forOperator("$and"));
		map.put("or", arrayArgumentAggregationMethodReference().forOperator("$or"));
		map.put("not", arrayArgumentAggregationMethodReference().forOperator("$not"));

		// SET OPERATORS
		map.put("setEquals", arrayArgumentAggregationMethodReference().forOperator("$setEquals"));
		map.put("setIntersection", arrayArgumentAggregationMethodReference().forOperator("$setIntersection"));
		map.put("setUnion", arrayArgumentAggregationMethodReference().forOperator("$setUnion"));
		map.put("setDifference", arrayArgumentAggregationMethodReference().forOperator("$setDifference"));
		// 2nd.
		map.put("setIsSubset", arrayArgumentAggregationMethodReference().forOperator("$setIsSubset"));
		map.put("anyElementTrue", arrayArgumentAggregationMethodReference().forOperator("$anyElementTrue"));
		map.put("allElementsTrue", arrayArgumentAggregationMethodReference().forOperator("$allElementsTrue"));

		// COMPARISON OPERATORS
		map.put("cmp", arrayArgumentAggregationMethodReference().forOperator("$cmp"));
		map.put("eq", arrayArgumentAggregationMethodReference().forOperator("$eq"));
		map.put("gt", arrayArgumentAggregationMethodReference().forOperator("$gt"));
		map.put("gte", arrayArgumentAggregationMethodReference().forOperator("$gte"));
		map.put("lt", arrayArgumentAggregationMethodReference().forOperator("$lt"));
		map.put("lte", arrayArgumentAggregationMethodReference().forOperator("$lte"));
		map.put("ne", arrayArgumentAggregationMethodReference().forOperator("$ne"));

		// ARITHMETIC OPERATORS
		map.put("abs", singleArgumentAggregationMethodReference().forOperator("$abs"));
		map.put("add", arrayArgumentAggregationMethodReference().forOperator("$add"));
		map.put("ceil", singleArgumentAggregationMethodReference().forOperator("$ceil"));
		map.put("divide", arrayArgumentAggregationMethodReference().forOperator("$divide"));
		map.put("exp", singleArgumentAggregationMethodReference().forOperator("$exp"));
		map.put("floor", singleArgumentAggregationMethodReference().forOperator("$floor"));
		map.put("ln", singleArgumentAggregationMethodReference().forOperator("$ln"));
		map.put("log", arrayArgumentAggregationMethodReference().forOperator("$log"));
		map.put("log10", singleArgumentAggregationMethodReference().forOperator("$log10"));
		map.put("mod", arrayArgumentAggregationMethodReference().forOperator("$mod"));
		map.put("multiply", arrayArgumentAggregationMethodReference().forOperator("$multiply"));
		map.put("pow", arrayArgumentAggregationMethodReference().forOperator("$pow"));
		map.put("sqrt", singleArgumentAggregationMethodReference().forOperator("$sqrt"));
		map.put("subtract", arrayArgumentAggregationMethodReference().forOperator("$subtract"));
		map.put("trunc", singleArgumentAggregationMethodReference().forOperator("$trunc"));

		// STRING OPERATORS
		map.put("concat", arrayArgumentAggregationMethodReference().forOperator("$concat"));
		map.put("strcasecmp", arrayArgumentAggregationMethodReference().forOperator("$strcasecmp"));
		map.put("substr", arrayArgumentAggregationMethodReference().forOperator("$substr"));
		map.put("toLower", singleArgumentAggregationMethodReference().forOperator("$toLower"));
		map.put("toUpper", singleArgumentAggregationMethodReference().forOperator("$toUpper"));
		map.put("strcasecmp", arrayArgumentAggregationMethodReference().forOperator("$strcasecmp"));
		map.put("indexOfBytes", arrayArgumentAggregationMethodReference().forOperator("$indexOfBytes"));
		map.put("indexOfCP", arrayArgumentAggregationMethodReference().forOperator("$indexOfCP"));
		map.put("split", arrayArgumentAggregationMethodReference().forOperator("$split"));
		map.put("strLenBytes", singleArgumentAggregationMethodReference().forOperator("$strLenBytes"));
		map.put("strLenCP", singleArgumentAggregationMethodReference().forOperator("$strLenCP"));
		map.put("substrCP", arrayArgumentAggregationMethodReference().forOperator("$substrCP"));

		// TEXT SEARCH OPERATORS
		map.put("meta", singleArgumentAggregationMethodReference().forOperator("$meta"));

		// ARRAY OPERATORS
		map.put("arrayElemAt", arrayArgumentAggregationMethodReference().forOperator("$arrayElemAt"));
		map.put("concatArrays", arrayArgumentAggregationMethodReference().forOperator("$concatArrays"));
		map.put("filter", mapArgumentAggregationMethodReference().forOperator("$filter") //
				.mappingParametersTo("input", "as", "cond"));
		map.put("isArray", singleArgumentAggregationMethodReference().forOperator("$isArray"));
		map.put("size", singleArgumentAggregationMethodReference().forOperator("$size"));
		map.put("slice", arrayArgumentAggregationMethodReference().forOperator("$slice"));
		map.put("reverseArray", singleArgumentAggregationMethodReference().forOperator("$reverseArray"));
		map.put("reduce", mapArgumentAggregationMethodReference().forOperator("$reduce").mappingParametersTo("input",
				"initialValue", "in"));
		map.put("zip", mapArgumentAggregationMethodReference().forOperator("$zip").mappingParametersTo("inputs",
				"useLongestLength", "defaults"));
		map.put("in", arrayArgumentAggregationMethodReference().forOperator("$in"));

		// VARIABLE OPERATORS
		map.put("map", mapArgumentAggregationMethodReference().forOperator("$map") //
				.mappingParametersTo("input", "as", "in"));
		map.put("let", mapArgumentAggregationMethodReference().forOperator("$let").mappingParametersTo("vars", "in"));

		// LITERAL OPERATORS
		map.put("literal", singleArgumentAggregationMethodReference().forOperator("$literal"));

		// DATE OPERATORS
		map.put("dayOfYear", singleArgumentAggregationMethodReference().forOperator("$dayOfYear"));
		map.put("dayOfMonth", singleArgumentAggregationMethodReference().forOperator("$dayOfMonth"));
		map.put("dayOfWeek", singleArgumentAggregationMethodReference().forOperator("$dayOfWeek"));
		map.put("year", singleArgumentAggregationMethodReference().forOperator("$year"));
		map.put("month", singleArgumentAggregationMethodReference().forOperator("$month"));
		map.put("week", singleArgumentAggregationMethodReference().forOperator("$week"));
		map.put("hour", singleArgumentAggregationMethodReference().forOperator("$hour"));
		map.put("minute", singleArgumentAggregationMethodReference().forOperator("$minute"));
		map.put("second", singleArgumentAggregationMethodReference().forOperator("$second"));
		map.put("millisecond", singleArgumentAggregationMethodReference().forOperator("$millisecond"));
		map.put("dateToString", mapArgumentAggregationMethodReference().forOperator("$dateToString") //
				.mappingParametersTo("format", "date"));
		map.put("isoDayOfWeek", singleArgumentAggregationMethodReference().forOperator("$isoDayOfWeek"));
		map.put("isoWeek", singleArgumentAggregationMethodReference().forOperator("$isoWeek"));
		map.put("isoWeekYear", singleArgumentAggregationMethodReference().forOperator("$isoWeekYear"));

		// CONDITIONAL OPERATORS
		map.put("cond", mapArgumentAggregationMethodReference().forOperator("$cond") //
				.mappingParametersTo("if", "then", "else"));
		map.put("ifNull", arrayArgumentAggregationMethodReference().forOperator("$ifNull"));

		// GROUP OPERATORS
		map.put("sum", arrayArgumentAggregationMethodReference().forOperator("$sum"));
		map.put("avg", arrayArgumentAggregationMethodReference().forOperator("$avg"));
		map.put("first", singleArgumentAggregationMethodReference().forOperator("$first"));
		map.put("last", singleArgumentAggregationMethodReference().forOperator("$last"));
		map.put("max", arrayArgumentAggregationMethodReference().forOperator("$max"));
		map.put("min", arrayArgumentAggregationMethodReference().forOperator("$min"));
		map.put("push", singleArgumentAggregationMethodReference().forOperator("$push"));
		map.put("addToSet", singleArgumentAggregationMethodReference().forOperator("$addToSet"));
		map.put("stdDevPop", arrayArgumentAggregationMethodReference().forOperator("$stdDevPop"));
		map.put("stdDevSamp", arrayArgumentAggregationMethodReference().forOperator("$stdDevSamp"));

		// TYPE OPERATORS
		map.put("type", singleArgumentAggregationMethodReference().forOperator("$type"));

		FUNCTIONS = Collections.unmodifiableMap(map);
	}

	MethodReferenceNode(MethodReference reference, ExpressionState state) {
		super(reference, state);
	}

	/**
	 * Returns the name of the method.
	 *
	 * @Deprecated since 1.10. Please use {@link #getMethodReference()}.
	 */
	@Deprecated
	public String getMethodName() {

		AggregationMethodReference methodReference = getMethodReference();
		return methodReference != null ? methodReference.getMongoOperator() : null;
	}

	/**
	 * Return the {@link AggregationMethodReference}.
	 *
	 * @return can be {@literal null}.
	 * @since 1.10
	 */
	public AggregationMethodReference getMethodReference() {

		String name = getName();
		String methodName = name.substring(0, name.indexOf('('));
		return FUNCTIONS.get(methodName);
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	public static final class AggregationMethodReference {

		private final String mongoOperator;
		private final ArgumentType argumentType;
		private final String[] argumentMap;

		/**
		 * Creates new {@link AggregationMethodReference}.
		 *
		 * @param mongoOperator can be {@literal null}.
		 * @param argumentType can be {@literal null}.
		 * @param argumentMap can be {@literal null}.
		 */
		private AggregationMethodReference(String mongoOperator, ArgumentType argumentType, String[] argumentMap) {

			this.mongoOperator = mongoOperator;
			this.argumentType = argumentType;
			this.argumentMap = argumentMap;
		}

		/**
		 * Get the MongoDB specific operator.
		 *
		 * @return can be {@literal null}.
		 */
		public String getMongoOperator() {
			return this.mongoOperator;
		}

		/**
		 * Get the {@link ArgumentType} used by the MongoDB.
		 *
		 * @return never {@literal null}.
		 */
		public ArgumentType getArgumentType() {
			return this.argumentType;
		}

		/**
		 * Get the property names in order order of appearance in resulting operation.
		 *
		 * @return never {@literal null}.
		 */
		public String[] getArgumentMap() {
			return argumentMap != null ? argumentMap : new String[] {};
		}

		/**
		 * Create a new {@link AggregationMethodReference} for a {@link ArgumentType#SINGLE} argument.
		 *
		 * @return never {@literal null}.
		 */
		static AggregationMethodReference singleArgumentAggregationMethodReference() {
			return new AggregationMethodReference(null, ArgumentType.SINGLE, null);
		}

		/**
		 * Create a new {@link AggregationMethodReference} for an {@link ArgumentType#ARRAY} argument.
		 *
		 * @return never {@literal null}.
		 */
		static AggregationMethodReference arrayArgumentAggregationMethodReference() {
			return new AggregationMethodReference(null, ArgumentType.ARRAY, null);
		}

		/**
		 * Create a new {@link AggregationMethodReference} for a {@link ArgumentType#MAP} argument.
		 *
		 * @return never {@literal null}.
		 */
		static AggregationMethodReference mapArgumentAggregationMethodReference() {
			return new AggregationMethodReference(null, ArgumentType.MAP, null);
		}

		/**
		 * Create a new {@link AggregationMethodReference} for a given {@literal aggregationExpressionOperator} reusing
		 * previously set arguments.
		 *
		 * @param aggregationExpressionOperator should not be {@literal null}.
		 * @return never {@literal null}.
		 */
		AggregationMethodReference forOperator(String aggregationExpressionOperator) {
			return new AggregationMethodReference(aggregationExpressionOperator, argumentType, argumentMap);
		}

		/**
		 * Create a new {@link AggregationMethodReference} for mapping actual parameters within the AST to the given
		 * {@literal aggregationExpressionProperties} reusing previously set arguments. <br />
		 * <strong>NOTE:</strong> Can only be applied to {@link AggregationMethodReference} of type
		 * {@link ArgumentType#MAP}.
		 *
		 * @param aggregationExpressionProperties should not be {@literal null}.
		 * @return never {@literal null}.
		 * @throws IllegalArgumentException
		 */
		AggregationMethodReference mappingParametersTo(String... aggregationExpressionProperties) {

			Assert.isTrue(ObjectUtils.nullSafeEquals(argumentType, ArgumentType.MAP),
					"Parameter mapping can only be applied to AggregationMethodReference with MAPPED ArgumentType.");
			return new AggregationMethodReference(mongoOperator, argumentType, aggregationExpressionProperties);
		}

		/**
		 * The actual argument type to use when mapping parameters to MongoDB specific format.
		 *
		 * @author Christoph Strobl
		 * @since 1.10
		 */
		public enum ArgumentType {
			SINGLE, ARRAY, MAP
		}
	}

}
