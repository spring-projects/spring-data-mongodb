/*
 * Copyright 2016. the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.AsBuilder;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 * @since 1.10
 */
public interface AggregationExpressions {

	/**
	 * @author Christoph Strobl
	 */
	class SetOperators {

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static SetOperatorFactory arrayAsSet(String fieldRef) {
			return new SetOperatorFactory(fieldRef);
		}

		public static class SetOperatorFactory {

			private final String fieldRef;

			/**
			 * Creates new {@link SetOperatorFactory} for given {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 */
			public SetOperatorFactory(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				this.fieldRef = fieldRef;
			}

			/**
			 * Compares the previously mentioned field to one or more arrays and returns {@literal true} if they have the same
			 * distinct elements and {@literal false} otherwise.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetEquals isEqualTo(String... arrayReferences) {
				return SetEquals.arrayAsSet(fieldRef).isEqualTo(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and one or more arrays and returns an array that contains the
			 * elements that appear in every of those.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetIntersection intersects(String... arrayReferences) {
				return SetIntersection.arrayAsSet(fieldRef).intersects(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and one or more arrays and returns an array that contains the
			 * elements that appear in any of those.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetUnion union(String... arrayReferences) {
				return SetUnion.arrayAsSet(fieldRef).union(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and returns an array containing the elements that do not exist in
			 * the given {@literal arrayReference}.
			 *
			 * @param arrayReference must not be {@literal null}.
			 * @return
			 */
			public SetDifference differenceTo(String arrayReference) {
				return SetDifference.arrayAsSet(fieldRef).differenceTo(arrayReference);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if it is a subset of the given
			 * {@literal arrayReference}.
			 *
			 * @param arrayReference must not be {@literal null}.
			 * @return
			 */
			public SetIsSubset isSubsetOf(String arrayReference) {
				return SetIsSubset.arrayAsSet(fieldRef).isSubsetOf(arrayReference);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if any of the elements are
			 * {@literal true} and {@literal false} otherwise.
			 *
			 * @return
			 */
			public AnyElementTrue anyElementTrue() {
				return AnyElementTrue.arrayAsSet(fieldRef);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if no elements is {@literal false}.
			 *
			 * @return
			 */
			public AllElementsTrue allElementsTrue() {
				return AllElementsTrue.arrayAsSet(fieldRef);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class ArithmeticOperators {

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static ArithmeticOperatorFactory valueOf(String fieldRef) {
			return new ArithmeticOperatorFactory(fieldRef);
		}

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static ArithmeticOperatorFactory valueOf(AggregationExpression fieldRef) {
			return new ArithmeticOperatorFactory(fieldRef);
		}

		public static class ArithmeticOperatorFactory {

			private final String fieldRef;
			private final AggregationExpression expression;

			/**
			 * Creates new {@link ArithmeticOperatorFactory} for given {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 */
			public ArithmeticOperatorFactory(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				this.fieldRef = fieldRef;
				this.expression = null;
			}

			/**
			 * Creats new {@link ArithmeticOperatorFactory} for given {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 */
			public ArithmeticOperatorFactory(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				this.fieldRef = null;
				this.expression = expression;
			}

			/**
			 * Returns the absolute value of the associated number.
			 *
			 * @return
			 */
			public Abs abs() {
				return fieldRef != null ? Abs.absoluteValueOf(fieldRef) : Abs.absoluteValueOf(expression);
			}

			/**
			 * Adds value of {@literal fieldRef} to the associated number.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Add add(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createAdd().add(fieldRef);
			}

			/**
			 * Adds value of {@link AggregationExpression} to the associated number.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Add add(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createAdd().add(expression);
			}

			/**
			 * Adds given {@literal value} to the associated number.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Add add(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createAdd().add(value);
			}

			private Add createAdd() {
				return fieldRef != null ? Add.valueOf(fieldRef) : Add.valueOf(expression);
			}

			/**
			 * Returns the smallest integer greater than or equal to the assoicated number.
			 *
			 * @return
			 */
			public Ceil ceil() {
				return fieldRef != null ? Ceil.ceilValueOf(fieldRef) : Ceil.ceilValueOf(expression);
			}

			/**
			 * Divide associated number by number referenced via {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Divide divideBy(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createDivide().divideBy(fieldRef);
			}

			/**
			 * Divide associated number by number extracted via {@literal expression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Divide divideBy(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createDivide().divideBy(expression);
			}

			/**
			 * Divide associated number by given {@literal value}.
			 *
			 * @param value
			 * @return
			 */
			public Divide divideBy(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createDivide().divideBy(value);
			}

			private Divide createDivide() {
				return fieldRef != null ? Divide.valueOf(fieldRef) : Divide.valueOf(expression);
			}

			/**
			 * Raises Euler’s number (i.e. e ) to the associated number.
			 *
			 * @return
			 */
			public Exp exp() {
				return fieldRef != null ? Exp.expValueOf(fieldRef) : Exp.expValueOf(expression);
			}

			/**
			 * Returns the largest integer less than or equal to the associated number.
			 *
			 * @return
			 */
			public Floor floor() {
				return fieldRef != null ? Floor.floorValueOf(fieldRef) : Floor.floorValueOf(expression);
			}

			/**
			 * Calculates the natural logarithm ln (i.e loge) of the assoicated number.
			 *
			 * @return
			 */
			public Ln ln() {
				return fieldRef != null ? Ln.lnValueOf(fieldRef) : Ln.lnValueOf(expression);
			}

			/**
			 * Calculates the log of the associated number in the specified base referenced via {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Log log(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createLog().log(fieldRef);
			}

			/**
			 * Calculates the log of the associated number in the specified base extracted by given
			 * {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Log log(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createLog().log(fieldRef);
			}

			/**
			 * Calculates the log of a the associated number in the specified {@literal base}.
			 *
			 * @param base must not be {@literal null}.
			 * @return
			 */
			public Log log(Number base) {

				Assert.notNull(base, "Base must not be null!");
				return createLog().log(base);
			}

			private Log createLog() {
				return fieldRef != null ? Log.valueOf(fieldRef) : Log.valueOf(expression);
			}

			/**
			 * Calculates the log base 10 for the associated number.
			 *
			 * @return
			 */
			public Log10 log10() {
				return fieldRef != null ? Log10.log10ValueOf(fieldRef) : Log10.log10ValueOf(expression);
			}

			/**
			 * Divides the associated number by another and returns the remainder.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Mod mod(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createMod().mod(fieldRef);
			}

			/**
			 * Divides the associated number by another and returns the remainder.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Mod mod(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createMod().mod(expression);
			}

			/**
			 * Divides the associated number by another and returns the remainder.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Mod mod(Number value) {

				Assert.notNull(value, "Base must not be null!");
				return createMod().mod(value);
			}

			private Mod createMod() {
				return fieldRef != null ? Mod.valueOf(fieldRef) : Mod.valueOf(expression);
			}

			/**
			 * Multiplies the associated number with another.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createMultiply().multiplyBy(fieldRef);
			}

			/**
			 * Multiplies the associated number with another.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createMultiply().multiplyBy(expression);
			}

			/**
			 * Multiplies the associated number with another.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createMultiply().multiplyBy(value);
			}

			private Multiply createMultiply() {
				return fieldRef != null ? Multiply.valueOf(fieldRef) : Multiply.valueOf(expression);
			}

			/**
			 * Raises the associated number to the specified exponent.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Pow pow(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createPow().pow(fieldRef);
			}

			/**
			 * Raises the associated number to the specified exponent.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Pow pow(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createPow().pow(expression);
			}

			/**
			 * Raises the associated number to the specified exponent.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Pow pow(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createPow().pow(value);
			}

			private Pow createPow() {
				return fieldRef != null ? Pow.valueOf(fieldRef) : Pow.valueOf(expression);
			}

			/**
			 * Calculates the square root of the associated number.
			 *
			 * @return
			 */
			public Sqrt sqrt() {
				return fieldRef != null ? Sqrt.sqrtOf(fieldRef) : Sqrt.sqrtOf(expression);
			}

			/**
			 * Subtracts value of given from the associated number.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Subtract subtract(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createSubtract().subtract(fieldRef);
			}

			/**
			 * Subtracts value of given from the associated number.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Subtract subtract(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createSubtract().subtract(expression);
			}

			/**
			 * Subtracts value from the associated number.
			 *
			 * @param value
			 * @return
			 */
			public Subtract subtract(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createSubtract().subtract(value);
			}

			private Subtract createSubtract() {
				return fieldRef != null ? Subtract.valueOf(fieldRef) : Subtract.valueOf(expression);
			}

			/**
			 * Truncates a number to its integer.
			 *
			 * @return
			 */
			public Trunc trunc() {
				return fieldRef != null ? Trunc.truncValueOf(fieldRef) : Trunc.truncValueOf(expression);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class StringOperators {

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static StringOperatorFactory valueOf(String fieldRef) {
			return new StringOperatorFactory(fieldRef);
		}

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static StringOperatorFactory valueOf(AggregationExpression fieldRef) {
			return new StringOperatorFactory(fieldRef);
		}

		public static class StringOperatorFactory {

			private final String fieldRef;
			private final AggregationExpression expression;

			public StringOperatorFactory(String fieldRef) {
				this.fieldRef = fieldRef;
				this.expression = null;
			}

			public StringOperatorFactory(AggregationExpression expression) {
				this.fieldRef = null;
				this.expression = expression;
			}

			/**
			 * Takes the associated string representation and concats the value of the referenced field to it.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public Concat concatValueOf(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createConcat().concatValueOf(fieldRef);
			}

			/**
			 * Takes the associated string representation and concats the result of the given {@link AggregationExpression} to
			 * it.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Concat concatValueOf(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createConcat().concatValueOf(expression);
			}

			/**
			 * Takes the associated string representation and concats given {@literal value} to it.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Concat concat(String value) {

				Assert.notNull(value, "Value must not be null!");
				return createConcat().concat(value);
			}

			private Concat createConcat() {
				return fieldRef != null ? Concat.valueOf(fieldRef) : Concat.valueOf(expression);
			}

			/**
			 * Takes the associated string representation and returns a substring starting at a specified index position.
			 *
			 * @param start
			 * @return
			 */
			public Substr substring(int start) {
				return substring(start, -1);
			}

			/**
			 * Takes the associated string representation and returns a substring starting at a specified index position
			 * including the specified number of characters.
			 *
			 * @param start
			 * @param nrOfChars
			 * @return
			 */
			public Substr substring(int start, int nrOfChars) {
				return createSubstr().substring(start, nrOfChars);
			}

			private Substr createSubstr() {
				return fieldRef != null ? Substr.valueOf(fieldRef) : Substr.valueOf(expression);
			}

			/**
			 * Takes the associated string representation and lowers it.
			 *
			 * @return
			 */
			public ToLower toLower() {
				return fieldRef != null ? ToLower.lowerValueOf(fieldRef) : ToLower.lowerValueOf(expression);
			}

			/**
			 * Takes the associated string representation and uppers it.
			 *
			 * @return
			 */
			public ToUpper toUpper() {
				return fieldRef != null ? ToUpper.upperValueOf(fieldRef) : ToUpper.upperValueOf(expression);
			}

			/**
			 * Takes the associated string representation and performs case-insensitive comparison to the given
			 * {@literal value}.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public StrCaseCmp strCaseCmp(String value) {

				Assert.notNull(value, "Value must not be null!");
				return createStrCaseCmp().strcasecmp(value);
			}

			/**
			 * Takes the associated string representation and performs case-insensitive comparison to the referenced
			 * {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public StrCaseCmp strCaseCmpValueOf(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createStrCaseCmp().strcasecmpValueOf(fieldRef);
			}

			/**
			 * Takes the associated string representation and performs case-insensitive comparison to the result of the given
			 * {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public StrCaseCmp strCaseCmpValueOf(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createStrCaseCmp().strcasecmpValueOf(expression);
			}

			private StrCaseCmp createStrCaseCmp() {
				return fieldRef != null ? StrCaseCmp.valueOf(fieldRef) : StrCaseCmp.valueOf(expression);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class ArrayOperators {

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static ArrayOperatorFactory arrayOf(String fieldRef) {
			return new ArrayOperatorFactory(fieldRef);
		}

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static ArrayOperatorFactory arrayOf(AggregationExpression expression) {
			return new ArrayOperatorFactory(expression);
		}

		public static class ArrayOperatorFactory {

			private final String fieldRef;
			private final AggregationExpression expression;

			public ArrayOperatorFactory(String fieldRef) {
				this.fieldRef = fieldRef;
				this.expression = null;
			}

			public ArrayOperatorFactory(AggregationExpression expression) {
				this.fieldRef = null;
				this.expression = expression;
			}

			/**
			 * Takes the associated array and returns the element at the specified array {@literal position}.
			 *
			 * @param position
			 * @return
			 */
			public ArrayElemtAt elementAt(int position) {
				return createArrayElemAt().elementAt(position);
			}

			/**
			 * Takes the associated array and returns the element at the position resulting form the given
			 * {@literal expression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public ArrayElemtAt elementAt(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createArrayElemAt().elementAt(expression);
			}

			/**
			 * Takes the associated array and returns the element at the position defined by the referenced {@literal field}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 * @return
			 */
			public ArrayElemtAt elementAt(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				return createArrayElemAt().elementAt(fieldRef);
			}

			private ArrayElemtAt createArrayElemAt() {
				return usesFieldRef() ? ArrayElemtAt.arrayOf(fieldRef) : ArrayElemtAt.arrayOf(expression);
			}

			/**
			 * Takes the associated array and concats the given {@literal arrayFieldReference} to it.
			 *
			 * @param arrayFieldReference must not be {@literal null}.
			 * @return
			 */
			public ConcatArrays concat(String arrayFieldReference) {

				Assert.notNull(arrayFieldReference, "ArrayFieldReference must not be null!");
				return createConcatArrays().concat(arrayFieldReference);
			}

			/**
			 * Takes the associated array and concats the array resulting form the given {@literal expression} to it.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public ConcatArrays concat(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createConcatArrays().concat(expression);
			}

			private ConcatArrays createConcatArrays() {
				return usesFieldRef() ? ConcatArrays.arrayOf(fieldRef) : ConcatArrays.arrayOf(expression);
			}

			/**
			 * Takes the associated array and selects a subset of the array to return based on the specified condition.
			 *
			 * @return
			 */
			public AsBuilder filter() {
				return Filter.filter(fieldRef);
			}

			/**
			 * Takes the associated array and an check if its an array.
			 *
			 * @return
			 */
			public IsArray isArray() {
				return usesFieldRef() ? IsArray.isArray(fieldRef) : IsArray.isArray(expression);
			}

			/**
			 * Takes the associated array and retrieves its length.
			 *
			 * @return
			 */
			public Size length() {
				return usesFieldRef() ? Size.lengthOfArray(fieldRef) : Size.lengthOfArray(expression);
			}

			/**
			 * Takes the associated array and selects a subset from it.
			 *
			 * @return
			 */
			public Slice slice() {
				return usesFieldRef() ? Slice.sliceArrayOf(fieldRef) : Slice.sliceArrayOf(expression);
			}

			private boolean usesFieldRef() {
				return fieldRef != null;
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class LiteralOperators {

		/**
		 * Take the value referenced by given {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static LiteralOperatorFactory valueOf(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new LiteralOperatorFactory(value);
		}

		public static class LiteralOperatorFactory {

			private final Object value;

			public LiteralOperatorFactory(Object value) {
				this.value = value;
			}

			/**
			 * Returns the associated value without parsing.
			 *
			 * @return
			 */
			public Literal asLiteral() {
				return Literal.asLiteral(value);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class DateOperators {

		/**
		 * Take the date referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static DateOperatorFactory dateOf(String fieldRef) {

			Assert.notNull(fieldRef, "FieldRef must not be null!");
			return new DateOperatorFactory(fieldRef);
		}

		/**
		 * Take the date resulting from the given {@literal expression}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static DateOperatorFactory dateOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new DateOperatorFactory(expression);
		}

		public static class DateOperatorFactory {

			private final String fieldRef;
			private final AggregationExpression expression;

			public DateOperatorFactory(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				this.fieldRef = fieldRef;
				this.expression = null;
			}

			public DateOperatorFactory(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				this.fieldRef = null;
				this.expression = expression;
			}

			/**
			 * Returns the day of the year for a date as a number between 1 and 366.
			 *
			 * @return
			 */
			public DayOfYear dayOfYear() {
				return usesFieldRef() ? DayOfYear.dayOfYear(fieldRef) : DayOfYear.dayOfYear(expression);
			}

			/**
			 * Returns the day of the month for a date as a number between 1 and 31.
			 *
			 * @return
			 */
			public DayOfMonth dayOfMonth() {
				return usesFieldRef() ? DayOfMonth.dayOfMonth(fieldRef) : DayOfMonth.dayOfMonth(expression);
			}

			/**
			 * Returns the day of the week for a date as a number between 1 (Sunday) and 7 (Saturday).
			 *
			 * @return
			 */
			public DayOfWeek dayOfWeek() {
				return usesFieldRef() ? DayOfWeek.dayOfWeek(fieldRef) : DayOfWeek.dayOfWeek(expression);
			}

			/**
			 * Returns the year portion of a date.
			 *
			 * @return
			 */
			public Year year() {
				return usesFieldRef() ? Year.yearOf(fieldRef) : Year.yearOf(expression);
			}

			/**
			 * Returns the month of a date as a number between 1 and 12.
			 *
			 * @return
			 */
			public Month month() {
				return usesFieldRef() ? Month.monthOf(fieldRef) : Month.monthOf(expression);
			}

			/**
			 * Returns the week of the year for a date as a number between 0 and 53.
			 *
			 * @return
			 */
			public Week week() {
				return usesFieldRef() ? Week.weekOf(fieldRef) : Week.weekOf(expression);
			}

			/**
			 * Returns the hour portion of a date as a number between 0 and 23.
			 *
			 * @return
			 */
			public Hour hour() {
				return usesFieldRef() ? Hour.hourOf(fieldRef) : Hour.hourOf(expression);
			}

			/**
			 * Returns the minute portion of a date as a number between 0 and 59.
			 *
			 * @return
			 */
			public Minute minute() {
				return usesFieldRef() ? Minute.minuteOf(fieldRef) : Minute.minuteOf(expression);
			}

			/**
			 * Returns the second portion of a date as a number between 0 and 59, but can be 60 to account for leap seconds.
			 *
			 * @return
			 */
			public Second second() {
				return usesFieldRef() ? Second.secondOf(fieldRef) : Second.secondOf(expression);
			}

			/**
			 * Returns the millisecond portion of a date as an integer between 0 and 999.
			 *
			 * @return
			 */
			public Millisecond millisecond() {
				return usesFieldRef() ? Millisecond.millisecondOf(fieldRef) : Millisecond.millisecondOf(expression);
			}

			/**
			 * Converts a date object to a string according to a user-specified {@literal format}.
			 *
			 * @param format must not be {@literal null}.
			 * @return
			 */
			public DateToString toString(String format) {
				return (usesFieldRef() ? DateToString.dateOf(fieldRef) : DateToString.dateOf(expression)).toString(format);
			}

			private boolean usesFieldRef() {
				return fieldRef != null;
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	abstract class AbstractAggregationExpression implements AggregationExpression {

		private final Object value;

		protected AbstractAggregationExpression(Object value) {
			this.value = value;
		}

		@Override
		public DBObject toDbObject(AggregationOperationContext context) {
			return toDbObject(this.value, context);
		}

		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			Object valueToUse;
			if (value instanceof List) {

				List<Object> arguments = (List<Object>) value;
				List<Object> args = new ArrayList<Object>(arguments.size());

				for (Object val : arguments) {
					args.add(unpack(val, context));
				}
				valueToUse = args;
			} else if (value instanceof Map) {

				DBObject dbo = new BasicDBObject();
				for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
					dbo.put(entry.getKey(), unpack(entry.getValue(), context));
				}
				valueToUse = dbo;
			} else {
				valueToUse = unpack(value, context);
			}

			return new BasicDBObject(getMongoMethod(), valueToUse);
		}

		protected static List<Field> asFields(String... fieldRefs) {

			if (ObjectUtils.isEmpty(fieldRefs)) {
				return Collections.emptyList();
			}

			return Fields.fields(fieldRefs).asList();
		}

		private Object unpack(Object value, AggregationOperationContext context) {

			if (value instanceof AggregationExpression) {
				return ((AggregationExpression) value).toDbObject(context);
			}

			if (value instanceof Field) {
				return context.getReference((Field) value).toString();
			}

			return value;
		}

		protected List<Object> append(Object value) {

			if (this.value instanceof List) {

				List<Object> clone = new ArrayList<Object>((List) this.value);

				if (value instanceof List) {
					for (Object val : (List) value) {
						clone.add(val);
					}
				} else {
					clone.add(value);
				}
				return clone;
			}

			return Arrays.asList(this.value, value);
		}

		protected Object append(String key, Object value) {

			if (!(value instanceof Map)) {
				throw new IllegalArgumentException("o_O");
			}
			Map<String, Object> clone = new LinkedHashMap<String, Object>((Map<String, Object>) value);
			clone.put(key, value);
			return clone;

		}

		public abstract String getMongoMethod();
	}

	/**
	 * {@link AggregationExpression} for {@code $setEquals}.
	 *
	 * @author Christoph Strobl
	 */
	class SetEquals extends AbstractAggregationExpression {

		private SetEquals(List<?> arrays) {
			super(arrays);
		}

		@Override
		public String getMongoMethod() {
			return "$setEquals";
		}

		public static SetEquals arrayAsSet(String arrayReference) {
			return new SetEquals(asFields(arrayReference));
		}

		public SetEquals isEqualTo(String... arrayReferences) {
			return new SetEquals(append(Fields.fields(arrayReferences).asList()));
		}

		public SetEquals isEqualTo(Object[] compareValue) {
			return new SetEquals(append(compareValue));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setIntersection}.
	 *
	 * @author Christoph Strobl
	 */
	class SetIntersection extends AbstractAggregationExpression {

		private SetIntersection(List<?> arrays) {
			super(arrays);
		}

		@Override
		public String getMongoMethod() {
			return "$setIntersection";
		}

		public static SetIntersection arrayAsSet(String arrayReference) {
			return new SetIntersection(asFields(arrayReference));
		}

		public SetIntersection intersects(String... arrayReferences) {
			return new SetIntersection(append(asFields(arrayReferences)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setUnion}.
	 *
	 * @author Christoph Strobl
	 */
	class SetUnion extends AbstractAggregationExpression {

		private SetUnion(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setUnion";
		}

		public static SetUnion arrayAsSet(String arrayReference) {
			return new SetUnion(asFields(arrayReference));
		}

		public SetUnion union(String... arrayReferences) {
			return new SetUnion(append(asFields(arrayReferences)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setDifference}.
	 *
	 * @author Christoph Strobl
	 */
	class SetDifference extends AbstractAggregationExpression {

		private SetDifference(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setDifference";
		}

		public static SetDifference arrayAsSet(String arrayReference) {
			return new SetDifference(asFields(arrayReference));
		}

		public SetDifference differenceTo(String arrayReference) {
			return new SetDifference(append(Fields.field(arrayReference)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setIsSubset}.
	 *
	 * @author Christoph Strobl
	 */
	class SetIsSubset extends AbstractAggregationExpression {

		private SetIsSubset(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setIsSubset";
		}

		public static SetIsSubset arrayAsSet(String arrayReference) {
			return new SetIsSubset(asFields(arrayReference));
		}

		public SetIsSubset isSubsetOf(String arrayReference) {
			return new SetIsSubset(append(Fields.field(arrayReference)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $anyElementTrue}.
	 *
	 * @author Christoph Strobl
	 */
	class AnyElementTrue extends AbstractAggregationExpression {

		private AnyElementTrue(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$anyElementTrue";
		}

		public static AnyElementTrue arrayAsSet(String arrayReference) {
			return new AnyElementTrue(asFields(arrayReference));
		}

		public AnyElementTrue anyElementTrue() {
			return this;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $allElementsTrue}.
	 *
	 * @author Christoph Strobl
	 */
	class AllElementsTrue extends AbstractAggregationExpression {

		private AllElementsTrue(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$allElementsTrue";
		}

		public static AllElementsTrue arrayAsSet(String arrayReference) {
			return new AllElementsTrue(asFields(arrayReference));
		}

		public AllElementsTrue allElementsTrue() {
			return this;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $abs}.
	 *
	 * @author Christoph Strobl
	 */
	class Abs extends AbstractAggregationExpression {

		private Abs(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$abs";
		}

		public static Abs absoluteValueOf(String fieldRef) {
			return new Abs(Fields.field(fieldRef));
		}

		public static Abs absoluteValueOf(AggregationExpression expression) {
			return new Abs(expression);
		}

		public static Abs absoluteValueOf(Number value) {
			return new Abs(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $add}.
	 *
	 * @author Christoph Strobl
	 */
	class Add extends AbstractAggregationExpression {

		protected Add(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$add";
		}

		public static Add valueOf(String fieldRef) {
			return new Add(asFields(fieldRef));
		}

		public static Add valueOf(AggregationExpression expression) {
			return new Add(Collections.singletonList(expression));
		}

		public static Add valueOf(Number value) {
			return new Add(Collections.singletonList(value));
		}

		public Add add(String fieldRef) {
			return new Add(append(Fields.field(fieldRef)));
		}

		public Add add(AggregationExpression expression) {
			return new Add(append(expression));
		}

		public Add add(Number value) {
			return new Add(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $ceil}.
	 *
	 * @author Christoph Strobl
	 */
	class Ceil extends AbstractAggregationExpression {

		private Ceil(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$ceil";
		}

		public static Ceil ceilValueOf(String fieldRef) {
			return new Ceil(Fields.field(fieldRef));
		}

		public static Ceil ceilValueOf(AggregationExpression expression) {
			return new Ceil(expression);
		}

		public static Ceil ceilValueOf(Number value) {
			return new Ceil(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $divide}.
	 *
	 * @author Christoph Strobl
	 */
	class Divide extends AbstractAggregationExpression {

		private Divide(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$divide";
		}

		public static Divide valueOf(String fieldRef) {
			return new Divide(asFields(fieldRef));
		}

		public static Divide valueOf(AggregationExpression expression) {
			return new Divide(Collections.singletonList(expression));
		}

		public static Divide valueOf(Number value) {
			return new Divide(Collections.singletonList(value));
		}

		public Divide divideBy(String fieldRef) {
			return new Divide(append(Fields.field(fieldRef)));
		}

		public Divide divideBy(AggregationExpression expression) {
			return new Divide(append(expression));
		}

		public Divide divideBy(Number value) {
			return new Divide(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $exp}.
	 *
	 * @author Christoph Strobl
	 */
	class Exp extends AbstractAggregationExpression {

		protected Exp(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$exp";
		}

		public static Exp expValueOf(String fieldRef) {
			return new Exp(Fields.field(fieldRef));
		}

		public static Exp expValueOf(AggregationExpression expression) {
			return new Exp(expression);
		}

		public static Exp expValueOf(Number value) {
			return new Exp(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $floor}.
	 *
	 * @author Christoph Strobl
	 */
	class Floor extends AbstractAggregationExpression {

		protected Floor(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$floor";
		}

		public static Floor floorValueOf(String fieldRef) {
			return new Floor(Fields.field(fieldRef));
		}

		public static Floor floorValueOf(AggregationExpression expression) {
			return new Floor(expression);
		}

		public static Floor floorValueOf(Number value) {
			return new Floor(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $ln}.
	 *
	 * @author Christoph Strobl
	 */
	class Ln extends AbstractAggregationExpression {

		private Ln(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$ln";
		}

		public static Ln lnValueOf(String fieldRef) {
			return new Ln(Fields.field(fieldRef));
		}

		public static Ln lnValueOf(AggregationExpression expression) {
			return new Ln(expression);
		}

		public static Ln lnValueOf(Number value) {
			return new Ln(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $log}.
	 *
	 * @author Christoph Strobl
	 */
	class Log extends AbstractAggregationExpression {

		private Log(List<?> values) {
			super(values);
		}

		@Override
		public String getMongoMethod() {
			return "$log";
		}

		public static Log valueOf(String fieldRef) {
			return new Log(asFields(fieldRef));
		}

		public static Log valueOf(AggregationExpression expression) {
			return new Log(Collections.singletonList(expression));
		}

		public static Log valueOf(Number value) {
			return new Log(Collections.singletonList(value));
		}

		public Log log(String fieldRef) {
			return new Log(append(Fields.field(fieldRef)));
		}

		public Log log(AggregationExpression expression) {
			return new Log(append(expression));
		}

		public Log log(Number base) {
			return new Log(append(base));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $log10}.
	 *
	 * @author Christoph Strobl
	 */
	class Log10 extends AbstractAggregationExpression {

		private Log10(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$log10";
		}

		public static Log10 log10ValueOf(String fieldRef) {
			return new Log10(Fields.field(fieldRef));
		}

		public static Log10 log10ValueOf(AggregationExpression expression) {
			return new Log10(expression);
		}

		public static Log10 log10ValueOf(Number value) {
			return new Log10(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $mod}.
	 *
	 * @author Christoph Strobl
	 */
	class Mod extends AbstractAggregationExpression {

		private Mod(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$mod";
		}

		public static Mod valueOf(String fieldRef) {
			return new Mod(asFields(fieldRef));
		}

		public static Mod valueOf(AggregationExpression expression) {
			return new Mod(Collections.singletonList(expression));
		}

		public static Mod valueOf(Number value) {
			return new Mod(Collections.singletonList(value));
		}

		public Mod mod(String fieldRef) {
			return new Mod(append(Fields.field(fieldRef)));
		}

		public Mod mod(AggregationExpression expression) {
			return new Mod(append(expression));
		}

		public Mod mod(Number base) {
			return new Mod(append(base));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $multiply}.
	 *
	 * @author Christoph Strobl
	 */
	class Multiply extends AbstractAggregationExpression {

		private Multiply(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$multiply";
		}

		public static Multiply valueOf(String fieldRef) {
			return new Multiply(asFields(fieldRef));
		}

		public static Multiply valueOf(AggregationExpression expression) {
			return new Multiply(Collections.singletonList(expression));
		}

		public static Multiply valueOf(Number value) {
			return new Multiply(Collections.singletonList(value));
		}

		public Multiply multiplyBy(String fieldRef) {
			return new Multiply(append(Fields.field(fieldRef)));
		}

		public Multiply multiplyBy(AggregationExpression expression) {
			return new Multiply(append(expression));
		}

		public Multiply multiplyBy(Number value) {
			return new Multiply(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $pow}.
	 *
	 * @author Christoph Strobl
	 */
	class Pow extends AbstractAggregationExpression {

		private Pow(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$pow";
		}

		public static Pow valueOf(String fieldRef) {
			return new Pow(asFields(fieldRef));
		}

		public static Pow valueOf(AggregationExpression expression) {
			return new Pow(Collections.singletonList(expression));
		}

		public static Pow valueOf(Number value) {
			return new Pow(Collections.singletonList(value));
		}

		public Pow pow(String fieldRef) {
			return new Pow(append(Fields.field(fieldRef)));
		}

		public Pow pow(AggregationExpression expression) {
			return new Pow(append(expression));
		}

		public Pow pow(Number value) {
			return new Pow(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $sqrt}.
	 *
	 * @author Christoph Strobl
	 */
	class Sqrt extends AbstractAggregationExpression {

		private Sqrt(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$sqrt";
		}

		public static Sqrt sqrtOf(String fieldRef) {
			return new Sqrt(Fields.field(fieldRef));
		}

		public static Sqrt sqrtOf(AggregationExpression expression) {
			return new Sqrt(expression);
		}

		public static Sqrt sqrtOf(Number value) {
			return new Sqrt(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $subtract}.
	 *
	 * @author Christoph Strobl
	 */
	class Subtract extends AbstractAggregationExpression {

		protected Subtract(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$subtract";
		}

		public static Subtract valueOf(String fieldRef) {
			return new Subtract(asFields(fieldRef));
		}

		public static Subtract valueOf(AggregationExpression expression) {
			return new Subtract(Collections.singletonList(expression));
		}

		public static Subtract valueOf(Number value) {
			return new Subtract(Collections.singletonList(value));
		}

		public Subtract subtract(String fieldRef) {
			return new Subtract(append(Fields.field(fieldRef)));
		}

		public Subtract subtract(AggregationExpression expression) {
			return new Subtract(append(expression));
		}

		public Subtract subtract(Number value) {
			return new Subtract(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $trunc}.
	 *
	 * @author Christoph Strobl
	 */
	class Trunc extends AbstractAggregationExpression {

		private Trunc(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$trunc";
		}

		public static Trunc truncValueOf(String fieldRef) {
			return new Trunc(Fields.field(fieldRef));
		}

		public static Trunc truncValueOf(AggregationExpression expression) {
			return new Trunc(expression);
		}

		public static Trunc truncValueOf(Number value) {
			return new Trunc(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $concat}.
	 *
	 * @author Christoph Strobl
	 */
	class Concat extends AbstractAggregationExpression {

		private Concat(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$concat";
		}

		public static Concat valueOf(String fieldRef) {
			return new Concat(asFields(fieldRef));
		}

		public static Concat valueOf(AggregationExpression expression) {
			return new Concat(Collections.singletonList(expression));
		}

		public static Concat stringValue(String value) {
			return new Concat(Collections.singletonList(value));
		}

		public Concat concatValueOf(String fieldRef) {
			return new Concat(append(Fields.field(fieldRef)));
		}

		public Concat concatValueOf(AggregationExpression expression) {
			return new Concat(append(expression));
		}

		public Concat concat(String value) {
			return new Concat(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $substr}.
	 *
	 * @author Christoph Strobl
	 */
	class Substr extends AbstractAggregationExpression {

		private Substr(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$substr";
		}

		public static Substr valueOf(String fieldRef) {
			return new Substr(asFields(fieldRef));
		}

		public static Substr valueOf(AggregationExpression expression) {
			return new Substr(Collections.singletonList(expression));
		}

		public Substr substring(int start) {
			return substring(start, -1);
		}

		public Substr substring(int start, int nrOfChars) {
			return new Substr(append(Arrays.asList(start, nrOfChars)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toLower}.
	 *
	 * @author Christoph Strobl
	 */
	class ToLower extends AbstractAggregationExpression {

		private ToLower(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$toLower";
		}

		public static ToLower lower(String value) {
			return new ToLower(value);
		}

		public static ToLower lowerValueOf(String fieldRef) {
			return new ToLower(Fields.field(fieldRef));
		}

		public static ToLower lowerValueOf(AggregationExpression expression) {
			return new ToLower(Collections.singletonList(expression));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toUpper}.
	 *
	 * @author Christoph Strobl
	 */
	class ToUpper extends AbstractAggregationExpression {

		private ToUpper(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$toUpper";
		}

		public static ToUpper upper(String value) {
			return new ToUpper(value);
		}

		public static ToUpper upperValueOf(String fieldRef) {
			return new ToUpper(Fields.field(fieldRef));
		}

		public static ToUpper upperValueOf(AggregationExpression expression) {
			return new ToUpper(Collections.singletonList(expression));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $strcasecmp}.
	 *
	 * @author Christoph Strobl
	 */
	class StrCaseCmp extends AbstractAggregationExpression {

		private StrCaseCmp(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$strcasecmp";
		}

		public static StrCaseCmp valueOf(String fieldRef) {
			return new StrCaseCmp(asFields(fieldRef));
		}

		public static StrCaseCmp valueOf(AggregationExpression expression) {
			return new StrCaseCmp(Collections.singletonList(expression));
		}

		public static StrCaseCmp stringValue(String value) {
			return new StrCaseCmp(Collections.singletonList(value));
		}

		public StrCaseCmp strcasecmp(String value) {
			return new StrCaseCmp(append(value));
		}

		public StrCaseCmp strcasecmpValueOf(String fieldRef) {
			return new StrCaseCmp(append(Fields.field(fieldRef)));
		}

		public StrCaseCmp strcasecmpValueOf(AggregationExpression expression) {
			return new StrCaseCmp(append(expression));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $arrayElementAt}.
	 *
	 * @author Christoph Strobl
	 */
	class ArrayElemtAt extends AbstractAggregationExpression {

		private ArrayElemtAt(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$arrayElemAt";
		}

		public static ArrayElemtAt arrayOf(String fieldRef) {
			return new ArrayElemtAt(asFields(fieldRef));
		}

		public static ArrayElemtAt arrayOf(AggregationExpression expression) {
			return new ArrayElemtAt(Collections.singletonList(expression));
		}

		public ArrayElemtAt elementAt(int index) {
			return new ArrayElemtAt(append(index));
		}

		public ArrayElemtAt elementAt(AggregationExpression expression) {
			return new ArrayElemtAt(append(expression));
		}

		public ArrayElemtAt elementAt(String numericFieldRef) {
			return new ArrayElemtAt(append(Fields.field(numericFieldRef)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $concatArrays}.
	 *
	 * @author Christoph Strobl
	 */
	class ConcatArrays extends AbstractAggregationExpression {

		private ConcatArrays(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$concatArrays";
		}

		public static ConcatArrays arrayOf(String fieldRef) {
			return new ConcatArrays(asFields(fieldRef));
		}

		public static ConcatArrays arrayOf(AggregationExpression expression) {
			return new ConcatArrays(Collections.singletonList(expression));
		}

		public ConcatArrays concat(String arrayFieldReference) {
			return new ConcatArrays(append(Fields.field(arrayFieldReference)));
		}

		public ConcatArrays concat(AggregationExpression expression) {
			return new ConcatArrays(append(expression));
		}
	}

	/**
	 * {@code $filter} {@link AggregationExpression} allows to select a subset of the array to return based on the
	 * specified condition.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	class Filter implements AggregationExpression {

		private Object input;
		private ExposedField as;
		private Object condition;

		private Filter() {
			// used by builder
		}

		/**
		 * Set the {@literal field} to apply the {@code $filter} to.
		 *
		 * @param field must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder filter(String field) {

			Assert.notNull(field, "Field must not be null!");
			return filter(Fields.field(field));
		}

		/**
		 * Set the {@literal field} to apply the {@code $filter} to.
		 *
		 * @param field must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder filter(Field field) {

			Assert.notNull(field, "Field must not be null!");
			return new FilterExpressionBuilder().filter(field);
		}

		/**
		 * Set the {@literal values} to apply the {@code $filter} to.
		 *
		 * @param values must not be {@literal null}.
		 * @return
		 */
		public static AsBuilder filter(List<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new FilterExpressionBuilder().filter(values);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDbObject(final AggregationOperationContext context) {

			return toFilter(new ExposedFieldsAggregationOperationContext(ExposedFields.from(as), context) {

				@Override
				public FieldReference getReference(Field field) {

					FieldReference ref = null;
					try {
						ref = context.getReference(field);
					} catch (Exception e) {
						// just ignore that one.
					}
					return ref != null ? ref : super.getReference(field);
				}
			});
		}

		private DBObject toFilter(AggregationOperationContext context) {

			DBObject filterExpression = new BasicDBObject();

			filterExpression.putAll(context.getMappedObject(new BasicDBObject("input", getMappedInput(context))));
			filterExpression.put("as", as.getTarget());

			filterExpression.putAll(context.getMappedObject(new BasicDBObject("cond", getMappedCondition(context))));

			return new BasicDBObject("$filter", filterExpression);
		}

		private Object getMappedInput(AggregationOperationContext context) {
			return input instanceof Field ? context.getReference((Field) input).toString() : input;
		}

		private Object getMappedCondition(AggregationOperationContext context) {

			if (!(condition instanceof AggregationExpression)) {
				return condition;
			}

			NestedDelegatingExpressionAggregationOperationContext nea = new NestedDelegatingExpressionAggregationOperationContext(
					context);
			return ((AggregationExpression) condition).toDbObject(nea);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface InputBuilder {

			/**
			 * Set the {@literal values} to apply the {@code $filter} to.
			 *
			 * @param array must not be {@literal null}.
			 * @return
			 */
			AsBuilder filter(List<?> array);

			/**
			 * Set the {@literal field} holding an array to apply the {@code $filter} to.
			 *
			 * @param field must not be {@literal null}.
			 * @return
			 */
			AsBuilder filter(Field field);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface AsBuilder {

			/**
			 * Set the {@literal variableName} for the elements in the input array.
			 *
			 * @param variableName must not be {@literal null}.
			 * @return
			 */
			ConditionBuilder as(String variableName);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface ConditionBuilder {

			/**
			 * Set the {@link AggregationExpression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(AggregationExpression expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(String expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(DBObject expression);
		}

		/**
		 * @author Christoph Strobl
		 */
		static final class FilterExpressionBuilder implements InputBuilder, AsBuilder, ConditionBuilder {

			private final Filter filter;

			FilterExpressionBuilder() {
				this.filter = new Filter();
			}

			public static InputBuilder newBuilder() {
				return new FilterExpressionBuilder();
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.InputBuilder#filter(java.util.List)
			 */
			@Override
			public AsBuilder filter(List<?> array) {

				Assert.notNull(array, "Array must not be null!");
				filter.input = new ArrayList<Object>(array);
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.InputBuilder#filter(org.springframework.data.mongodb.core.aggregation.Field)
			 */
			@Override
			public AsBuilder filter(Field field) {

				Assert.notNull(field, "Field must not be null!");
				filter.input = field;
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.AsBuilder#as(java.lang.String)
			 */
			@Override
			public ConditionBuilder as(String variableName) {

				Assert.notNull(variableName, "Variable name  must not be null!");
				filter.as = new ExposedField(variableName, true);
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(org.springframework.data.mongodb.core.aggregation.AggregationExpression)
			 */
			@Override
			public Filter by(AggregationExpression condition) {

				Assert.notNull(condition, "Condition must not be null!");
				filter.condition = condition;
				return filter;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(java.lang.String)
			 */
			@Override
			public Filter by(String expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(com.mongodb.DBObject)
			 */
			@Override
			public Filter by(DBObject expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isArray}.
	 *
	 * @author Christoph Strobl
	 */
	class IsArray extends AbstractAggregationExpression {

		private IsArray(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$isArray";
		}

		public static IsArray isArray(String fieldRef) {
			return new IsArray(Fields.field(fieldRef));
		}

		public static IsArray isArray(AggregationExpression expression) {
			return new IsArray(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $size}.
	 *
	 * @author Christoph Strobl
	 */
	class Size extends AbstractAggregationExpression {

		private Size(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$size";
		}

		public static Size lengthOfArray(String fieldRef) {
			return new Size(Fields.field(fieldRef));
		}

		public static Size lengthOfArray(AggregationExpression expression) {
			return new Size(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $slice}.
	 *
	 * @author Christoph Strobl
	 */
	class Slice extends AbstractAggregationExpression {

		private Slice(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$slice";
		}

		public static Slice sliceArrayOf(String fieldRef) {
			return new Slice(asFields(fieldRef));
		}

		public static Slice sliceArrayOf(AggregationExpression expression) {
			return new Slice(Collections.singletonList(expression));
		}

		public Slice itemCount(int nrElements) {
			return new Slice(append(nrElements));
		}

		public SliceElementsBuilder offset(final int position) {

			return new SliceElementsBuilder() {

				@Override
				public Slice itemCount(int nrElements) {
					return new Slice(append(position)).itemCount(nrElements);
				}
			};
		}

		public interface SliceElementsBuilder {
			Slice itemCount(int nrElements);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $literal}.
	 *
	 * @author Christoph Strobl
	 */
	class Literal extends AbstractAggregationExpression {

		private Literal(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$literal";
		}

		public static Literal asLiteral(Object value) {
			return new Literal(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfYear}.
	 *
	 * @author Christoph Strobl
	 */
	class DayOfYear extends AbstractAggregationExpression {

		private DayOfYear(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$dayOfYear";
		}

		public static DayOfYear dayOfYear(String fieldRef) {
			return new DayOfYear(Fields.field(fieldRef));
		}

		public static DayOfYear dayOfYear(AggregationExpression expression) {
			return new DayOfYear(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfMonth}.
	 *
	 * @author Christoph Strobl
	 */
	class DayOfMonth extends AbstractAggregationExpression {

		private DayOfMonth(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$dayOfMonth";
		}

		public static DayOfMonth dayOfMonth(String fieldRef) {
			return new DayOfMonth(Fields.field(fieldRef));
		}

		public static DayOfMonth dayOfMonth(AggregationExpression expression) {
			return new DayOfMonth(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfWeek}.
	 *
	 * @author Christoph Strobl
	 */
	class DayOfWeek extends AbstractAggregationExpression {

		private DayOfWeek(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$dayOfWeek";
		}

		public static DayOfWeek dayOfWeek(String fieldRef) {
			return new DayOfWeek(Fields.field(fieldRef));
		}

		public static DayOfWeek dayOfWeek(AggregationExpression expression) {
			return new DayOfWeek(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $year}.
	 *
	 * @author Christoph Strobl
	 */
	class Year extends AbstractAggregationExpression {

		private Year(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$year";
		}

		public static Year yearOf(String fieldRef) {
			return new Year(Fields.field(fieldRef));
		}

		public static Year yearOf(AggregationExpression expression) {
			return new Year(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $month}.
	 *
	 * @author Christoph Strobl
	 */
	class Month extends AbstractAggregationExpression {

		private Month(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$month";
		}

		public static Month monthOf(String fieldRef) {
			return new Month(Fields.field(fieldRef));
		}

		public static Month monthOf(AggregationExpression expression) {
			return new Month(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $week}.
	 *
	 * @author Christoph Strobl
	 */
	class Week extends AbstractAggregationExpression {

		private Week(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$week";
		}

		public static Week weekOf(String fieldRef) {
			return new Week(Fields.field(fieldRef));
		}

		public static Week weekOf(AggregationExpression expression) {
			return new Week(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $hour}.
	 *
	 * @author Christoph Strobl
	 */
	class Hour extends AbstractAggregationExpression {

		private Hour(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$hour";
		}

		public static Hour hourOf(String fieldRef) {
			return new Hour(Fields.field(fieldRef));
		}

		public static Hour hourOf(AggregationExpression expression) {
			return new Hour(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $minute}.
	 *
	 * @author Christoph Strobl
	 */
	class Minute extends AbstractAggregationExpression {

		private Minute(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$minute";
		}

		public static Minute minuteOf(String fieldRef) {
			return new Minute(Fields.field(fieldRef));
		}

		public static Minute minuteOf(AggregationExpression expression) {
			return new Minute(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $second}.
	 *
	 * @author Christoph Strobl
	 */
	class Second extends AbstractAggregationExpression {

		private Second(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$second";
		}

		public static Second secondOf(String fieldRef) {
			return new Second(Fields.field(fieldRef));
		}

		public static Second secondOf(AggregationExpression expression) {
			return new Second(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $millisecond}.
	 *
	 * @author Christoph Strobl
	 */
	class Millisecond extends AbstractAggregationExpression {

		private Millisecond(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$millisecond";
		}

		public static Millisecond millisecondOf(String fieldRef) {
			return new Millisecond(Fields.field(fieldRef));
		}

		public static Millisecond millisecondOf(AggregationExpression expression) {
			return new Millisecond(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateToString}.
	 *
	 * @author Christoph Strobl
	 */
	class DateToString extends AbstractAggregationExpression {

		private DateToString(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$dateToString";
		}

		public static FormatBuilder dateOf(final String fieldRef) {

			return new FormatBuilder() {
				@Override
				public DateToString toString(String format) {

					Map<String, Object> args = new LinkedHashMap<String, Object>(2);
					args.put("format", format);
					args.put("date", Fields.field(fieldRef));
					return new DateToString(args);
				}
			};
		}

		public static FormatBuilder dateOf(final AggregationExpression expression) {

			return new FormatBuilder() {
				@Override
				public DateToString toString(String format) {

					Map<String, Object> args = new LinkedHashMap<String, Object>(2);
					args.put("format", format);
					args.put("date", expression);
					return new DateToString(args);
				}
			};
		}

		public interface FormatBuilder {
			DateToString toString(String format);
		}
	}

}
