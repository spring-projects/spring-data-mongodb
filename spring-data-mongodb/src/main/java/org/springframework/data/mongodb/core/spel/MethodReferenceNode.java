/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * An {@link ExpressionNode} representing a method reference.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Sebastien Gerard
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MethodReferenceNode extends ExpressionNode {

	private static final Map<String, AggregationMethodReference> FUNCTIONS;

	static {

		Map<String, AggregationMethodReference> map = new HashMap<String, AggregationMethodReference>();

		// BOOLEAN OPERATORS
		map.put("and", arrayArgRef().forOperator("$and"));
		map.put("or", arrayArgRef().forOperator("$or"));
		map.put("not", arrayArgRef().forOperator("$not"));

		// SET OPERATORS
		map.put("setEquals", arrayArgRef().forOperator("$setEquals"));
		map.put("setIntersection", arrayArgRef().forOperator("$setIntersection"));
		map.put("setUnion", arrayArgRef().forOperator("$setUnion"));
		map.put("setDifference", arrayArgRef().forOperator("$setDifference"));
		// 2nd.
		map.put("setIsSubset", arrayArgRef().forOperator("$setIsSubset"));
		map.put("anyElementTrue", arrayArgRef().forOperator("$anyElementTrue"));
		map.put("allElementsTrue", arrayArgRef().forOperator("$allElementsTrue"));

		// COMPARISON OPERATORS
		map.put("cmp", arrayArgRef().forOperator("$cmp"));
		map.put("eq", arrayArgRef().forOperator("$eq"));
		map.put("gt", arrayArgRef().forOperator("$gt"));
		map.put("gte", arrayArgRef().forOperator("$gte"));
		map.put("lt", arrayArgRef().forOperator("$lt"));
		map.put("lte", arrayArgRef().forOperator("$lte"));
		map.put("ne", arrayArgRef().forOperator("$ne"));

		// ARITHMETIC OPERATORS
		map.put("abs", singleArgRef().forOperator("$abs"));
		map.put("add", arrayArgRef().forOperator("$add"));
		map.put("ceil", singleArgRef().forOperator("$ceil"));
		map.put("divide", arrayArgRef().forOperator("$divide"));
		map.put("exp", singleArgRef().forOperator("$exp"));
		map.put("floor", singleArgRef().forOperator("$floor"));
		map.put("ln", singleArgRef().forOperator("$ln"));
		map.put("log", arrayArgRef().forOperator("$log"));
		map.put("log10", singleArgRef().forOperator("$log10"));
		map.put("mod", arrayArgRef().forOperator("$mod"));
		map.put("multiply", arrayArgRef().forOperator("$multiply"));
		map.put("pow", arrayArgRef().forOperator("$pow"));
		map.put("sqrt", singleArgRef().forOperator("$sqrt"));
		map.put("subtract", arrayArgRef().forOperator("$subtract"));
		map.put("trunc", singleArgRef().forOperator("$trunc"));

		// STRING OPERATORS
		map.put("concat", arrayArgRef().forOperator("$concat"));
		map.put("strcasecmp", arrayArgRef().forOperator("$strcasecmp"));
		map.put("substr", arrayArgRef().forOperator("$substr"));
		map.put("toLower", singleArgRef().forOperator("$toLower"));
		map.put("toUpper", singleArgRef().forOperator("$toUpper"));
		map.put("indexOfBytes", arrayArgRef().forOperator("$indexOfBytes"));
		map.put("indexOfCP", arrayArgRef().forOperator("$indexOfCP"));
		map.put("split", arrayArgRef().forOperator("$split"));
		map.put("strLenBytes", singleArgRef().forOperator("$strLenBytes"));
		map.put("strLenCP", singleArgRef().forOperator("$strLenCP"));
		map.put("substrCP", arrayArgRef().forOperator("$substrCP"));
		map.put("trim", mapArgRef().forOperator("$trim").mappingParametersTo("input", "chars"));
		map.put("ltrim", mapArgRef().forOperator("$ltrim").mappingParametersTo("input", "chars"));
		map.put("rtrim", mapArgRef().forOperator("$rtrim").mappingParametersTo("input", "chars"));

		// TEXT SEARCH OPERATORS
		map.put("meta", singleArgRef().forOperator("$meta"));

		// ARRAY OPERATORS
		map.put("arrayElemAt", arrayArgRef().forOperator("$arrayElemAt"));
		map.put("concatArrays", arrayArgRef().forOperator("$concatArrays"));
		map.put("filter", mapArgRef().forOperator("$filter") //
				.mappingParametersTo("input", "as", "cond"));
		map.put("isArray", singleArgRef().forOperator("$isArray"));
		map.put("size", singleArgRef().forOperator("$size"));
		map.put("slice", arrayArgRef().forOperator("$slice"));
		map.put("reverseArray", singleArgRef().forOperator("$reverseArray"));
		map.put("reduce", mapArgRef().forOperator("$reduce").mappingParametersTo("input", "initialValue", "in"));
		map.put("zip", mapArgRef().forOperator("$zip").mappingParametersTo("inputs", "useLongestLength", "defaults"));
		map.put("in", arrayArgRef().forOperator("$in"));
		map.put("arrayToObject", singleArgRef().forOperator("$arrayToObject"));
		map.put("indexOfArray", arrayArgRef().forOperator("$indexOfArray"));
		map.put("range", arrayArgRef().forOperator("$range"));

		// VARIABLE OPERATORS
		map.put("map", mapArgRef().forOperator("$map") //
				.mappingParametersTo("input", "as", "in"));
		map.put("let", mapArgRef().forOperator("$let").mappingParametersTo("vars", "in"));

		// LITERAL OPERATORS
		map.put("literal", singleArgRef().forOperator("$literal"));

		// DATE OPERATORS
		map.put("dayOfYear", singleArgRef().forOperator("$dayOfYear"));
		map.put("dayOfMonth", singleArgRef().forOperator("$dayOfMonth"));
		map.put("dayOfWeek", singleArgRef().forOperator("$dayOfWeek"));
		map.put("year", singleArgRef().forOperator("$year"));
		map.put("month", singleArgRef().forOperator("$month"));
		map.put("week", singleArgRef().forOperator("$week"));
		map.put("hour", singleArgRef().forOperator("$hour"));
		map.put("minute", singleArgRef().forOperator("$minute"));
		map.put("second", singleArgRef().forOperator("$second"));
		map.put("millisecond", singleArgRef().forOperator("$millisecond"));
		map.put("dateToString", mapArgRef().forOperator("$dateToString") //
				.mappingParametersTo("format", "date"));
		map.put("dateFromString", mapArgRef().forOperator("$dateFromString") //
				.mappingParametersTo("dateString", "format", "timezone", "onError", "onNull"));
		map.put("dateFromParts", mapArgRef().forOperator("$dateFromParts").mappingParametersTo("year", "month", "day",
				"hour", "minute", "second", "milliseconds", "timezone"));
		map.put("isoDateFromParts", mapArgRef().forOperator("$dateFromParts").mappingParametersTo("isoWeekYear", "isoWeek",
				"isoDayOfWeek", "hour", "minute", "second", "milliseconds", "timezone"));
		map.put("dateToParts", mapArgRef().forOperator("$dateToParts") //
				.mappingParametersTo("date", "timezone", "iso8601"));
		map.put("isoDayOfWeek", singleArgRef().forOperator("$isoDayOfWeek"));
		map.put("isoWeek", singleArgRef().forOperator("$isoWeek"));
		map.put("isoWeekYear", singleArgRef().forOperator("$isoWeekYear"));

		// CONDITIONAL OPERATORS
		map.put("cond", mapArgRef().forOperator("$cond") //
				.mappingParametersTo("if", "then", "else"));
		map.put("ifNull", arrayArgRef().forOperator("$ifNull"));

		// GROUP OPERATORS
		map.put("sum", arrayArgRef().forOperator("$sum"));
		map.put("avg", arrayArgRef().forOperator("$avg"));
		map.put("first", singleArgRef().forOperator("$first"));
		map.put("last", singleArgRef().forOperator("$last"));
		map.put("max", arrayArgRef().forOperator("$max"));
		map.put("min", arrayArgRef().forOperator("$min"));
		map.put("push", singleArgRef().forOperator("$push"));
		map.put("addToSet", singleArgRef().forOperator("$addToSet"));
		map.put("stdDevPop", arrayArgRef().forOperator("$stdDevPop"));
		map.put("stdDevSamp", arrayArgRef().forOperator("$stdDevSamp"));

		// TYPE OPERATORS
		map.put("type", singleArgRef().forOperator("$type"));

		// OBJECT OPERATORS
		map.put("objectToArray", singleArgRef().forOperator("$objectToArray"));
		map.put("mergeObjects", arrayArgRef().forOperator("$mergeObjects"));

		// CONVERT OPERATORS
		map.put("convert", mapArgRef().forOperator("$convert") //
				.mappingParametersTo("input", "to", "onError", "onNull"));
		map.put("toBool", singleArgRef().forOperator("$toBool"));
		map.put("toDate", singleArgRef().forOperator("$toDate"));
		map.put("toDecimal", singleArgRef().forOperator("$toDecimal"));
		map.put("toDouble", singleArgRef().forOperator("$toDouble"));
		map.put("toInt", singleArgRef().forOperator("$toInt"));
		map.put("toLong", singleArgRef().forOperator("$toLong"));
		map.put("toObjectId", singleArgRef().forOperator("$toObjectId"));
		map.put("toString", singleArgRef().forOperator("$toString"));

		FUNCTIONS = Collections.unmodifiableMap(map);
	}

	MethodReferenceNode(MethodReference reference, ExpressionState state) {
		super(reference, state);
	}

	/**
	 * Returns the name of the method.
	 *
	 * @deprecated since 1.10. Please use {@link #getMethodReference()}.
	 */
	@Nullable
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
	@Nullable
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

		private final @Nullable String mongoOperator;
		private final @Nullable ArgumentType argumentType;
		private final @Nullable String[] argumentMap;

		/**
		 * Creates new {@link AggregationMethodReference}.
		 *
		 * @param mongoOperator can be {@literal null}.
		 * @param argumentType can be {@literal null}.
		 * @param argumentMap can be {@literal null}.
		 */
		private AggregationMethodReference(@Nullable String mongoOperator, @Nullable ArgumentType argumentType,
				@Nullable String[] argumentMap) {

			this.mongoOperator = mongoOperator;
			this.argumentType = argumentType;
			this.argumentMap = argumentMap;
		}

		/**
		 * Get the MongoDB specific operator.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public String getMongoOperator() {
			return this.mongoOperator;
		}

		/**
		 * Get the {@link ArgumentType} used by the MongoDB.
		 *
		 * @return never {@literal null}.
		 */
		@Nullable
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
		static AggregationMethodReference singleArgRef() {
			return new AggregationMethodReference(null, ArgumentType.SINGLE, null);
		}

		/**
		 * Create a new {@link AggregationMethodReference} for an {@link ArgumentType#ARRAY} argument.
		 *
		 * @return never {@literal null}.
		 */
		static AggregationMethodReference arrayArgRef() {
			return new AggregationMethodReference(null, ArgumentType.ARRAY, null);
		}

		/**
		 * Create a new {@link AggregationMethodReference} for a {@link ArgumentType#MAP} argument.
		 *
		 * @return never {@literal null}.
		 */
		static AggregationMethodReference mapArgRef() {
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
