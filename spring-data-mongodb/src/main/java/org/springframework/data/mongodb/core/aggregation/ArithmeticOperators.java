/*
 * Copyright 2016. the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.Collections;
import java.util.List;

import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Avg;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Max;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Min;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.StdDevPop;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.StdDevSamp;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Sum;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal Arithmetic} aggregation operations that perform math operations on numbers.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
public class ArithmeticOperators {

	/**
	 * Take the field referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static ArithmeticOperatorFactory valueOf(String fieldReference) {
		return new ArithmeticOperatorFactory(fieldReference);
	}

	/**
	 * Take the value resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static ArithmeticOperatorFactory valueOf(AggregationExpression expression) {
		return new ArithmeticOperatorFactory(expression);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class ArithmeticOperatorFactory {

		private final String fieldReference;
		private final AggregationExpression expression;

		/**
		 * Creates new {@link ArithmeticOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public ArithmeticOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link ArithmeticOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public ArithmeticOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the absolute value of the associated number.
		 *
		 * @return
		 */
		public Abs abs() {
			return usesFieldRef() ? Abs.absoluteValueOf(fieldReference) : Abs.absoluteValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the value of {@literal fieldReference} to the associated
		 * number.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Add add(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createAdd().add(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the resulting value of the given
		 * {@link AggregationExpression} to the associated number.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Add add(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createAdd().add(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the given {@literal value} to the associated number.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Add add(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return createAdd().add(value);
		}

		private Add createAdd() {
			return usesFieldRef() ? Add.valueOf(fieldReference) : Add.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the smallest integer greater than or equal to the
		 * assoicated number.
		 *
		 * @return
		 */
		public Ceil ceil() {
			return usesFieldRef() ? Ceil.ceilValueOf(fieldReference) : Ceil.ceilValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that ivides the associated number by number referenced via
		 * {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Divide divideBy(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createDivide().divideBy(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by number extracted via
		 * {@literal expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Divide divideBy(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createDivide().divideBy(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by given {@literal value}.
		 *
		 * @param value
		 * @return
		 */
		public Divide divideBy(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return createDivide().divideBy(value);
		}

		private Divide createDivide() {
			return usesFieldRef() ? Divide.valueOf(fieldReference) : Divide.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that raises Euler’s number (i.e. e ) on the associated number.
		 *
		 * @return
		 */
		public Exp exp() {
			return usesFieldRef() ? Exp.expValueOf(fieldReference) : Exp.expValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the largest integer less than or equal to the associated
		 * number.
		 *
		 * @return
		 */
		public Floor floor() {
			return usesFieldRef() ? Floor.floorValueOf(fieldReference) : Floor.floorValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the natural logarithm ln (i.e loge) of the assoicated
		 * number.
		 *
		 * @return
		 */
		public Ln ln() {
			return usesFieldRef() ? Ln.lnValueOf(fieldReference) : Ln.lnValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the log of the associated number in the specified base
		 * referenced via {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Log log(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createLog().log(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the log of the associated number in the specified base
		 * extracted by given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Log log(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createLog().log(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the log of a the associated number in the specified
		 * {@literal base}.
		 *
		 * @param base must not be {@literal null}.
		 * @return
		 */
		public Log log(Number base) {

			Assert.notNull(base, "Base must not be null!");
			return createLog().log(base);
		}

		private Log createLog() {
			return usesFieldRef() ? Log.valueOf(fieldReference) : Log.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the log base 10 for the associated number.
		 *
		 * @return
		 */
		public Log10 log10() {
			return usesFieldRef() ? Log10.log10ValueOf(fieldReference) : Log10.log10ValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by another and returns the
		 * remainder.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Mod mod(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createMod().mod(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by another and returns the
		 * remainder.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Mod mod(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createMod().mod(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by another and returns the
		 * remainder.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Mod mod(Number value) {

			Assert.notNull(value, "Base must not be null!");
			return createMod().mod(value);
		}

		private Mod createMod() {
			return usesFieldRef() ? Mod.valueOf(fieldReference) : Mod.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that multiplies the associated number with another.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Multiply multiplyBy(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createMultiply().multiplyBy(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that multiplies the associated number with another.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Multiply multiplyBy(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createMultiply().multiplyBy(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that multiplies the associated number with another.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Multiply multiplyBy(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return createMultiply().multiplyBy(value);
		}

		private Multiply createMultiply() {
			return usesFieldRef() ? Multiply.valueOf(fieldReference) : Multiply.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that raises the associated number to the specified exponent.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Pow pow(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createPow().pow(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that raises the associated number to the specified exponent.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Pow pow(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createPow().pow(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that raises the associated number to the specified exponent.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Pow pow(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return createPow().pow(value);
		}

		private Pow createPow() {
			return usesFieldRef() ? Pow.valueOf(fieldReference) : Pow.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the square root of the associated number.
		 *
		 * @return
		 */
		public Sqrt sqrt() {
			return usesFieldRef() ? Sqrt.sqrtOf(fieldReference) : Sqrt.sqrtOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts value of given from the associated number.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Subtract subtract(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createSubtract().subtract(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts value of given from the associated number.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Subtract subtract(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createSubtract().subtract(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts value from the associated number.
		 *
		 * @param value
		 * @return
		 */
		public Subtract subtract(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return createSubtract().subtract(value);
		}

		private Subtract createSubtract() {
			return usesFieldRef() ? Subtract.valueOf(fieldReference) : Subtract.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that truncates a number to its integer.
		 *
		 * @return
		 */
		public Trunc trunc() {
			return usesFieldRef() ? Trunc.truncValueOf(fieldReference) : Trunc.truncValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates and returns the sum of numeric values.
		 *
		 * @return
		 */
		public Sum sum() {
			return usesFieldRef() ? AccumulatorOperators.Sum.sumOf(fieldReference)
					: AccumulatorOperators.Sum.sumOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the average value of the numeric values.
		 *
		 * @return
		 */
		public Avg avg() {
			return usesFieldRef() ? AccumulatorOperators.Avg.avgOf(fieldReference)
					: AccumulatorOperators.Avg.avgOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the maximum value.
		 *
		 * @return
		 */
		public Max max() {
			return usesFieldRef() ? AccumulatorOperators.Max.maxOf(fieldReference)
					: AccumulatorOperators.Max.maxOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the minimum value.
		 *
		 * @return
		 */
		public Min min() {
			return usesFieldRef() ? AccumulatorOperators.Min.minOf(fieldReference)
					: AccumulatorOperators.Min.minOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the population standard deviation of the input values.
		 *
		 * @return
		 */
		public StdDevPop stdDevPop() {
			return usesFieldRef() ? AccumulatorOperators.StdDevPop.stdDevPopOf(fieldReference)
					: AccumulatorOperators.StdDevPop.stdDevPopOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the sample standard deviation of the input values.
		 *
		 * @return
		 */
		public StdDevSamp stdDevSamp() {
			return usesFieldRef() ? AccumulatorOperators.StdDevSamp.stdDevSampOf(fieldReference)
					: AccumulatorOperators.StdDevSamp.stdDevSampOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that rounds a number to a whole integer or to a specified decimal
		 * place.
		 *
		 * @return new instance of {@link Round}.
		 * @since 3.0
		 */
		public Round round() {
			return usesFieldRef() ? Round.roundValueOf(fieldReference) : Round.roundValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that rounds a number to a specified decimal place.
		 *
		 * @return new instance of {@link Round}.
		 * @since 3.0
		 */
		public Round roundToPlace(int place) {
			return round().place(place);
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $abs}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Abs extends AbstractAggregationExpression {

		private Abs(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$abs";
		}

		/**
		 * Creates new {@link Abs}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Abs absoluteValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Abs(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Abs}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Abs absoluteValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Abs(expression);
		}

		/**
		 * Creates new {@link Abs}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Abs absoluteValueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Abs(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $add}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Add extends AbstractAggregationExpression {

		protected Add(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$add";
		}

		/**
		 * Creates new {@link Add}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Add valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Add(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Add}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Add valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Add(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Add}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Add valueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Add(Collections.singletonList(value));
		}

		public Add add(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Add(append(Fields.field(fieldReference)));
		}

		public Add add(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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
	public static class Ceil extends AbstractAggregationExpression {

		private Ceil(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$ceil";
		}

		/**
		 * Creates new {@link Ceil}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Ceil ceilValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Ceil(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Ceil}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Ceil ceilValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Ceil(expression);
		}

		/**
		 * Creates new {@link Ceil}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Ceil ceilValueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Ceil(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $divide}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Divide extends AbstractAggregationExpression {

		private Divide(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$divide";
		}

		/**
		 * Creates new {@link Divide}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Divide valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Divide(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Divide}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Divide valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Divide(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Divide}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Divide valueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Divide(Collections.singletonList(value));
		}

		public Divide divideBy(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Divide(append(Fields.field(fieldReference)));
		}

		public Divide divideBy(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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
	public static class Exp extends AbstractAggregationExpression {

		private Exp(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$exp";
		}

		/**
		 * Creates new {@link Exp}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Exp expValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Exp(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Exp}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Exp expValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Exp(expression);
		}

		/**
		 * Creates new {@link Exp}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Exp expValueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Exp(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $floor}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Floor extends AbstractAggregationExpression {

		private Floor(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$floor";
		}

		/**
		 * Creates new {@link Floor}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Floor floorValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Floor(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Floor}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Floor floorValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Floor(expression);
		}

		/**
		 * Creates new {@link Floor}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Floor floorValueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Floor(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $ln}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Ln extends AbstractAggregationExpression {

		private Ln(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$ln";
		}

		/**
		 * Creates new {@link Ln}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Ln lnValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Ln(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Ln}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Ln lnValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Ln(expression);
		}

		/**
		 * Creates new {@link Ln}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Ln lnValueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Ln(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $log}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Log extends AbstractAggregationExpression {

		private Log(List<?> values) {
			super(values);
		}

		@Override
		protected String getMongoMethod() {
			return "$log";
		}

		/**
		 * Creates new {@link Min}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Log valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Log(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Log}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Log valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Log(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Log}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Log valueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Log(Collections.singletonList(value));
		}

		public Log log(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Log(append(Fields.field(fieldReference)));
		}

		public Log log(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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
	public static class Log10 extends AbstractAggregationExpression {

		private Log10(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$log10";
		}

		/**
		 * Creates new {@link Log10}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Log10 log10ValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Log10(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Log10}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Log10 log10ValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Log10(expression);
		}

		/**
		 * Creates new {@link Log10}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Log10 log10ValueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Log10(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $mod}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Mod extends AbstractAggregationExpression {

		private Mod(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$mod";
		}

		/**
		 * Creates new {@link Mod}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Mod valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Mod(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Mod}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Mod valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Mod(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Mod}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Mod valueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Mod(Collections.singletonList(value));
		}

		public Mod mod(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Mod(append(Fields.field(fieldReference)));
		}

		public Mod mod(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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
	public static class Multiply extends AbstractAggregationExpression {

		private Multiply(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$multiply";
		}

		/**
		 * Creates new {@link Multiply}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Multiply valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Multiply(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Multiply}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Multiply valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Multiply(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Multiply}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Multiply valueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Multiply(Collections.singletonList(value));
		}

		public Multiply multiplyBy(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Multiply(append(Fields.field(fieldReference)));
		}

		public Multiply multiplyBy(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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
	public static class Pow extends AbstractAggregationExpression {

		private Pow(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$pow";
		}

		/**
		 * Creates new {@link Pow}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Pow valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Pow(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Pow}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Pow valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Pow(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Pow}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Pow valueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Pow(Collections.singletonList(value));
		}

		public Pow pow(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Pow(append(Fields.field(fieldReference)));
		}

		public Pow pow(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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
	public static class Sqrt extends AbstractAggregationExpression {

		private Sqrt(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$sqrt";
		}

		/**
		 * Creates new {@link Sqrt}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Sqrt sqrtOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Sqrt(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Sqrt}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Sqrt sqrtOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Sqrt(expression);
		}

		/**
		 * Creates new {@link Sqrt}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Sqrt sqrtOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Sqrt(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $subtract}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Subtract extends AbstractAggregationExpression {

		private Subtract(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$subtract";
		}

		/**
		 * Creates new {@link Subtract}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Subtract valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Subtract(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Subtract}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Subtract valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Subtract(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Subtract}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Subtract valueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Subtract(Collections.singletonList(value));
		}

		public Subtract subtract(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Subtract(append(Fields.field(fieldReference)));
		}

		public Subtract subtract(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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
	public static class Trunc extends AbstractAggregationExpression {

		private Trunc(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$trunc";
		}

		/**
		 * Creates new {@link Trunc}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Trunc truncValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Trunc(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Trunc}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Trunc truncValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Trunc(expression);
		}

		/**
		 * Creates new {@link Trunc}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Trunc truncValueOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Trunc(value);
		}
	}

	/**
	 * {@link Round} rounds a number to a whole integer or to a specified decimal place.
	 * <ul>
	 * <li>If {@link Round#place(int)} resolves to a positive integer, {@code $round} rounds to the given decimal
	 * places.</li>
	 * <li>If {@link Round#place(int)} resolves to a negative integer, {@code $round} rounds to the left of the
	 * decimal.</li>
	 * <li>If {@link Round#place(int)} resolves to a zero, {@code $round} rounds using the first digit to the right of the
	 * decimal.</li>
	 * </ul>
	 *
	 * @since 3.0
	 */
	public static class Round extends AbstractAggregationExpression {

		private Round(Object value) {
			super(value);
		}

		/**
		 * Round the value of the field that resolves to an integer, double, decimal, or long.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Round}.
		 */
		public static Round roundValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Round(Collections.singletonList(Fields.field(fieldReference)));
		}

		/**
		 * Round the outcome of the given expression hat resolves to an integer, double, decimal, or long.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Round}.
		 */
		public static Round roundValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Round(Collections.singletonList(expression));
		}

		/**
		 * Round the given numeric (integer, double, decimal, or long) value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Round}.
		 */
		public static Round round(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Round(Collections.singletonList(value));
		}

		/**
		 * The place to round to. Can be between -20 and 100, exclusive.
		 *
		 * @param place
		 * @return new instance of {@link Round}.
		 */
		public Round place(int place) {
			return new Round(append(place));
		}

		/**
		 * The place to round to defined by an expression that resolves to an integer between -20 and 100, exclusive.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Round}.
		 */
		public Round placeOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Round(append(expression));
		}

		/**
		 * The place to round to defined by via a field reference that resolves to an integer between -20 and 100,
		 * exclusive.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Round}.
		 */
		public Round placeOf(String fieldReference) {

			Assert.notNull(fieldReference, "fieldReference must not be null!");
			return new Round(append(Fields.field(fieldReference)));
		}

		@Override
		protected String getMongoMethod() {
			return "$round";
		}
	}
}
