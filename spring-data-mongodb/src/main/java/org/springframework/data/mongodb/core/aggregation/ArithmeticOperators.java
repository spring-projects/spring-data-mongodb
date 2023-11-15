/*
 * Copyright 2016-2023 the original author or authors.
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
import java.util.Locale;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Avg;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.CovariancePop;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.CovarianceSamp;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Max;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Median;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Min;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Percentile;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.StdDevPop;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.StdDevSamp;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Sum;
import org.springframework.data.mongodb.core.aggregation.SetWindowFieldsOperation.WindowUnit;
import org.springframework.data.mongodb.core.aggregation.SetWindowFieldsOperation.WindowUnits;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Gateway to {@literal Arithmetic} aggregation operations that perform math operations on numbers.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mushtaq Ahmed
 * @author Julia Lee
 * @since 1.10
 */
public class ArithmeticOperators {

	/**
	 * Take the field referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link ArithmeticOperatorFactory}.
	 */
	public static ArithmeticOperatorFactory valueOf(String fieldReference) {
		return new ArithmeticOperatorFactory(fieldReference);
	}

	/**
	 * Take the value resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link ArithmeticOperatorFactory}.
	 */
	public static ArithmeticOperatorFactory valueOf(AggregationExpression expression) {
		return new ArithmeticOperatorFactory(expression);
	}

	/**
	 * Creates new {@link AggregationExpression} that returns a random float between {@code 0} and {@code 1} each time it
	 * is called.
	 *
	 * @return new instance of {@link Rand}.
	 * @since 3.3
	 */
	public static Rand rand() {
		return new Rand();
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

			Assert.notNull(fieldReference, "FieldReference must not be null");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link ArithmeticOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public ArithmeticOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the absolute value of the associated number.
		 *
		 * @return new instance of {@link Abs}.
		 */
		public Abs abs() {
			return usesFieldRef() ? Abs.absoluteValueOf(fieldReference) : Abs.absoluteValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the value of {@literal fieldReference} to the associated
		 * number.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Add}.
		 */
		public Add add(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createAdd().add(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the resulting value of the given
		 * {@link AggregationExpression} to the associated number.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Add}.
		 */
		public Add add(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createAdd().add(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the given {@literal value} to the associated number.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Add}.
		 */
		public Add add(Number value) {

			Assert.notNull(value, "Value must not be null");
			return createAdd().add(value);
		}

		private Add createAdd() {
			return usesFieldRef() ? Add.valueOf(fieldReference) : Add.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the smallest integer greater than or equal to the
		 * associated number.
		 *
		 * @return new instance of {@link Ceil}.
		 */
		public Ceil ceil() {
			return usesFieldRef() ? Ceil.ceilValueOf(fieldReference) : Ceil.ceilValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the mathematical derivative value.
		 *
		 * @return new instance of {@link Derivative}.
		 * @since 3.3
		 */
		public Derivative derivative() {
			return derivative((String) null);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the mathematical derivative value.
		 *
		 * @param unit The time unit ({@link WindowUnits#WEEK}, {@link WindowUnits#DAY}, {@link WindowUnits#HOUR},
		 *          {@link WindowUnits#MINUTE}, {@link WindowUnits#SECOND}, {@link WindowUnits#MILLISECOND}) to apply.
		 * @return new instance of {@link Derivative}.
		 * @since 3.3
		 */
		public Derivative derivative(WindowUnit unit) {

			Assert.notNull(unit, "Window unit must not be null");

			return derivative(unit.name().toLowerCase(Locale.ROOT));
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the mathematical derivative value.
		 *
		 * @param unit The time unit ({@literal week, day, hour, minute, second, millisecond}) to apply can be
		 *          {@literal null}.
		 * @return new instance of {@link Derivative}.
		 * @since 3.3
		 */
		public Derivative derivative(@Nullable String unit) {

			Derivative derivative = usesFieldRef() ? Derivative.derivativeOf(fieldReference)
					: Derivative.derivativeOf(expression);
			return StringUtils.hasText(unit) ? derivative.unit(unit) : derivative;
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by number referenced via
		 * {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Divide}.
		 */
		public Divide divideBy(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createDivide().divideBy(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by number extracted via
		 * {@literal expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Divide}.
		 */
		public Divide divideBy(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createDivide().divideBy(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by given {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Divide}.
		 */
		public Divide divideBy(Number value) {

			Assert.notNull(value, "Value must not be null");
			return createDivide().divideBy(value);
		}

		private Divide createDivide() {
			return usesFieldRef() ? Divide.valueOf(fieldReference) : Divide.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that raises Euler’s number (i.e. e ) on the associated number.
		 *
		 * @return new instance of {@link Exp}.
		 */
		public Exp exp() {
			return usesFieldRef() ? Exp.expValueOf(fieldReference) : Exp.expValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the largest integer less than or equal to the associated
		 * number.
		 *
		 * @return new instance of {@link Floor}.
		 */
		public Floor floor() {
			return usesFieldRef() ? Floor.floorValueOf(fieldReference) : Floor.floorValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the approximation for the mathematical integral value.
		 *
		 * @return new instance of {@link Integral}.
		 * @since 3.3
		 */
		public Integral integral() {
			return usesFieldRef() ? Integral.integralOf(fieldReference) : Integral.integralOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the approximation for the mathematical integral value.
		 *
		 * @param unit The time unit ({@link WindowUnits#WEEK}, {@link WindowUnits#DAY}, {@link WindowUnits#HOUR},
		 *          {@link WindowUnits#MINUTE}, {@link WindowUnits#SECOND}, {@link WindowUnits#MILLISECOND}) to apply.
		 * @return new instance of {@link Derivative}.
		 * @since 3.3
		 */
		public Integral integral(WindowUnit unit) {

			Assert.notNull(unit, "Window unit must not be null");

			return integral(unit.name().toLowerCase(Locale.ROOT));
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the approximation for the mathematical integral value.
		 *
		 * @param unit the unit of measure.
		 * @return new instance of {@link Integral}.
		 * @since 3.3
		 */
		public Integral integral(String unit) {

			Assert.hasText(unit, "Unit must not be empty");

			return integral().unit(unit);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the natural logarithm ln (i.e loge) of the associated
		 * number.
		 *
		 * @return new instance of {@link Ln}.
		 */
		public Ln ln() {
			return usesFieldRef() ? Ln.lnValueOf(fieldReference) : Ln.lnValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the log of the associated number in the specified base
		 * referenced via {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Log}.
		 */
		public Log log(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createLog().log(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the log of the associated number in the specified base
		 * extracted by given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Log}.
		 */
		public Log log(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createLog().log(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the log of a the associated number in the specified
		 * {@literal base}.
		 *
		 * @param base must not be {@literal null}.
		 * @return new instance of {@link Log}.
		 */
		public Log log(Number base) {

			Assert.notNull(base, "Base must not be null");
			return createLog().log(base);
		}

		private Log createLog() {
			return usesFieldRef() ? Log.valueOf(fieldReference) : Log.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the log base 10 for the associated number.
		 *
		 * @return new instance of {@link Log10}.
		 */
		public Log10 log10() {
			return usesFieldRef() ? Log10.log10ValueOf(fieldReference) : Log10.log10ValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by another and returns the
		 * remainder.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Mod}.
		 */
		public Mod mod(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createMod().mod(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by another and returns the
		 * remainder.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Mod}.
		 */
		public Mod mod(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createMod().mod(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated number by another and returns the
		 * remainder.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Mod}.
		 */
		public Mod mod(Number value) {

			Assert.notNull(value, "Base must not be null");
			return createMod().mod(value);
		}

		private Mod createMod() {
			return usesFieldRef() ? Mod.valueOf(fieldReference) : Mod.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that multiplies the associated number with another.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Multiply}.
		 */
		public Multiply multiplyBy(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createMultiply().multiplyBy(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that multiplies the associated number with another.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Multiply}.
		 */
		public Multiply multiplyBy(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createMultiply().multiplyBy(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that multiplies the associated number with another.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Multiply}.
		 */
		public Multiply multiplyBy(Number value) {

			Assert.notNull(value, "Value must not be null");
			return createMultiply().multiplyBy(value);
		}

		private Multiply createMultiply() {
			return usesFieldRef() ? Multiply.valueOf(fieldReference) : Multiply.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that raises the associated number to the specified exponent.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
		public Pow pow(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createPow().pow(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that raises the associated number to the specified exponent.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
		public Pow pow(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createPow().pow(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that raises the associated number to the specified exponent.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
		public Pow pow(Number value) {

			Assert.notNull(value, "Value must not be null");
			return createPow().pow(value);
		}

		private Pow createPow() {
			return usesFieldRef() ? Pow.valueOf(fieldReference) : Pow.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the square root of the associated number.
		 *
		 * @return new instance of {@link Sqrt}.
		 */
		public Sqrt sqrt() {
			return usesFieldRef() ? Sqrt.sqrtOf(fieldReference) : Sqrt.sqrtOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts value of given from the associated number.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Subtract}.
		 */
		public Subtract subtract(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createSubtract().subtract(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts value of given from the associated number.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Subtract}.
		 */
		public Subtract subtract(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createSubtract().subtract(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts value from the associated number.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Subtract}.
		 */
		public Subtract subtract(Number value) {

			Assert.notNull(value, "Value must not be null");
			return createSubtract().subtract(value);
		}

		private Subtract createSubtract() {
			return usesFieldRef() ? Subtract.valueOf(fieldReference) : Subtract.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that truncates a number to its integer.
		 *
		 * @return new instance of {@link Trunc}.
		 */
		public Trunc trunc() {
			return usesFieldRef() ? Trunc.truncValueOf(fieldReference) : Trunc.truncValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates and returns the sum of numeric values.
		 *
		 * @return new instance of {@link Sum}.
		 */
		public Sum sum() {
			return usesFieldRef() ? AccumulatorOperators.Sum.sumOf(fieldReference)
					: AccumulatorOperators.Sum.sumOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the average value of the numeric values.
		 *
		 * @return new instance of {@link Avg}.
		 */
		public Avg avg() {
			return usesFieldRef() ? AccumulatorOperators.Avg.avgOf(fieldReference)
					: AccumulatorOperators.Avg.avgOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the maximum value.
		 *
		 * @return new instance of {@link Max}.
		 */
		public Max max() {
			return usesFieldRef() ? AccumulatorOperators.Max.maxOf(fieldReference)
					: AccumulatorOperators.Max.maxOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the minimum value.
		 *
		 * @return new instance of {@link Min}.
		 */
		public Min min() {
			return usesFieldRef() ? AccumulatorOperators.Min.minOf(fieldReference)
					: AccumulatorOperators.Min.minOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the population standard deviation of the input values.
		 *
		 * @return new instance of {@link StdDevPop}.
		 */
		public StdDevPop stdDevPop() {
			return usesFieldRef() ? AccumulatorOperators.StdDevPop.stdDevPopOf(fieldReference)
					: AccumulatorOperators.StdDevPop.stdDevPopOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the sample standard deviation of the input values.
		 *
		 * @return new instance of {@link StdDevSamp}.
		 */
		public StdDevSamp stdDevSamp() {
			return usesFieldRef() ? AccumulatorOperators.StdDevSamp.stdDevSampOf(fieldReference)
					: AccumulatorOperators.StdDevSamp.stdDevSampOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that uses the previous input (field/expression) and the value of the
		 * given field to calculate the population covariance of the two.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link CovariancePop}.
		 * @since 3.3
		 */
		public CovariancePop covariancePop(String fieldReference) {
			return covariancePop().and(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that uses the previous input (field/expression) and the result of the
		 * given {@link AggregationExpression expression} to calculate the population covariance of the two.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link CovariancePop}.
		 * @since 3.3
		 */
		public CovariancePop covariancePop(AggregationExpression expression) {
			return covariancePop().and(expression);
		}

		private CovariancePop covariancePop() {
			return usesFieldRef() ? CovariancePop.covariancePopOf(fieldReference) : CovariancePop.covariancePopOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that uses the previous input (field/expression) and the value of the
		 * given field to calculate the sample covariance of the two.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link CovariancePop}.
		 * @since 3.3
		 */
		public CovarianceSamp covarianceSamp(String fieldReference) {
			return covarianceSamp().and(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that uses the previous input (field/expression) and the result of the
		 * given {@link AggregationExpression expression} to calculate the sample covariance of the two.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link CovariancePop}.
		 * @since 3.3
		 */
		public CovarianceSamp covarianceSamp(AggregationExpression expression) {
			return covarianceSamp().and(expression);
		}

		private CovarianceSamp covarianceSamp() {
			return usesFieldRef() ? CovarianceSamp.covarianceSampOf(fieldReference)
					: CovarianceSamp.covarianceSampOf(expression);
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

		/**
		 * Creates new {@link AggregationExpression} that calculates the sine of a numeric value given in
		 * {@link AngularUnit#RADIANS radians}.
		 *
		 * @return new instance of {@link Sin}.
		 * @since 3.3
		 */
		public Sin sin() {
			return sin(AngularUnit.RADIANS);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the sine of a numeric value in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param unit the unit of measure.
		 * @return new instance of {@link Sin}.
		 * @since 3.3
		 */
		public Sin sin(AngularUnit unit) {
			return usesFieldRef() ? Sin.sinOf(fieldReference, unit) : Sin.sinOf(expression, unit);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the sine of a numeric value given in
		 * {@link AngularUnit#RADIANS radians}.
		 *
		 * @return new instance of {@link Sinh}.
		 * @since 3.3
		 */
		public Sinh sinh() {
			return sinh(AngularUnit.RADIANS);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the sine of a numeric value.
		 *
		 * @param unit the unit of measure.
		 * @return new instance of {@link Sinh}.
		 * @since 3.3
		 */
		public Sinh sinh(AngularUnit unit) {
			return usesFieldRef() ? Sinh.sinhOf(fieldReference, unit) : Sinh.sinhOf(expression, unit);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse sine of a numeric value.
		 *
		 * @return new instance of {@link ASin}.
		 * @since 3.3
		 */
		public ASin asin() {
			return usesFieldRef() ? ASin.asinOf(fieldReference) : ASin.asinOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse hyperbolic sine of a numeric value.
		 *
		 * @return new instance of {@link ASinh}.
		 * @since 3.3
		 */
		public ASinh asinh() {
			return usesFieldRef() ? ASinh.asinhOf(fieldReference) : ASinh.asinhOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the cosine of a numeric value given in
		 * {@link AngularUnit#RADIANS radians}.
		 *
		 * @return new instance of {@link Cos}.
		 * @since 3.3
		 */
		public Cos cos() {
			return cos(AngularUnit.RADIANS);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the cosine of a numeric value in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param unit the unit of measure.
		 * @return new instance of {@link Cos}.
		 * @since 3.3
		 */
		public Cos cos(AngularUnit unit) {
			return usesFieldRef() ? Cos.cosOf(fieldReference, unit) : Cos.cosOf(expression, unit);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the hyperbolic cosine of a numeric value given in
		 * {@link AngularUnit#RADIANS radians}.
		 *
		 * @return new instance of {@link Cosh}.
		 * @since 3.3
		 */
		public Cosh cosh() {
			return cosh(AngularUnit.RADIANS);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the hyperbolic cosine of a numeric value.
		 *
		 * @param unit the unit of measure.
		 * @return new instance of {@link Cosh}.
		 * @since 3.3
		 */
		public Cosh cosh(AngularUnit unit) {
			return usesFieldRef() ? Cosh.coshOf(fieldReference, unit) : Cosh.coshOf(expression, unit);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse cosine of a numeric value.
		 *
		 * @return new instance of {@link ACos}.
		 * @since 3.4
		 */
		public ACos acos() {
			return usesFieldRef() ? ACos.acosOf(fieldReference) : ACos.acosOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse hyperbolic cosine of a numeric value.
		 *
		 * @return new instance of {@link ACosh}.
		 * @since 3.4
		 */
		public ACosh acosh() {
			return usesFieldRef() ? ACosh.acoshOf(fieldReference) : ACosh.acoshOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the tangent of a numeric value given in
		 * {@link AngularUnit#RADIANS radians}.
		 *
		 * @return new instance of {@link Tan}.
		 * @since 3.3
		 */
		public Tan tan() {
			return tan(AngularUnit.RADIANS);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse tangent of a numeric value.
		 *
		 * @return new instance of {@link ATan}.
		 * @since 3.3
		 */
		public ATan atan() {
			return usesFieldRef() ? ATan.atanOf(fieldReference) : ATan.atanOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse tangent of the the numeric value divided by
		 * the given numeric value in the argument.
		 *
		 * @param value the numeric value
		 * @return new instance of {@link ATan2}.
		 * @since 3.3
		 */
		public ATan2 atan2(Number value) {

			Assert.notNull(value, "Value must not be null");
			return createATan2().atan2of(value);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse tangent of the the numeric value divided by
		 * the given field reference in the argument.
		 *
		 * @param fieldReference the numeric value
		 * @return new instance of {@link ATan2}.
		 * @since 3.3
		 */
		public ATan2 atan2(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createATan2().atan2of(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse tangent of the the numeric value divided by
		 * the given {@link AggregationExpression} in the argument.
		 *
		 * @param expression the expression evaluating to a numeric value
		 * @return new instance of {@link ATan2}.
		 * @since 3.3
		 */
		public ATan2 atan2(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createATan2().atan2of(expression);
		}

		private ATan2 createATan2() {

			return usesFieldRef() ? ATan2.valueOf(fieldReference) : ATan2.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the inverse hyperbolic tangent of a numeric value.
		 *
		 * @return new instance of {@link ATanh}.
		 * @since 3.3
		 */
		public ATanh atanh() {
			return usesFieldRef() ? ATanh.atanhOf(fieldReference) : ATanh.atanhOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the tangent of a numeric value in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param unit the unit of measure.
		 * @return new instance of {@link Tan}.
		 * @since 3.3
		 */
		public Tan tan(AngularUnit unit) {
			return usesFieldRef() ? Tan.tanOf(fieldReference, unit) : Tan.tanOf(expression, unit);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the hyperbolic tangent of a numeric value given in
		 * {@link AngularUnit#RADIANS radians}.
		 *
		 * @return new instance of {@link Tan}.
		 * @since 3.3
		 */
		public Tanh tanh() {
			return tanh(AngularUnit.RADIANS);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the hyperbolic tangent of a numeric value.
		 *
		 * @param unit the unit of measure.
		 * @return new instance of {@link Tanh}.
		 * @since 3.3
		 */
		public Tanh tanh(AngularUnit unit) {
			return usesFieldRef() ? Tanh.tanhOf(fieldReference, unit) : Tanh.tanhOf(expression, unit);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the requested percentile(s) of the
		 * numeric value.
		 *
		 * @return new instance of {@link Percentile}.
		 * @param percentages must not be {@literal null}.
		 * @since 4.2
		 */
		public Percentile percentile(Double... percentages) {
			Percentile percentile = usesFieldRef() ? AccumulatorOperators.Percentile.percentileOf(fieldReference)
					: AccumulatorOperators.Percentile.percentileOf(expression);
			return percentile.percentages(percentages);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the requested percentile(s) of the
		 * numeric value.
		 *
		 * @return new instance of {@link Median}.
		 * @since 4.2
		 */
		public Median median() {
			return usesFieldRef() ? AccumulatorOperators.Median.medianOf(fieldReference)
					: AccumulatorOperators.Median.medianOf(expression);
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
		 * @return new instance of {@link Abs}.
		 */
		public static Abs absoluteValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Abs(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Abs}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Abs}.
		 */
		public static Abs absoluteValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Abs(expression);
		}

		/**
		 * Creates new {@link Abs}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Abs}.
		 */
		public static Abs absoluteValueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
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
		 * @return new instance of {@link Add}.
		 */
		public static Add valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Add(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Add}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Add}.
		 */
		public static Add valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Add(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Add}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Add}.
		 */
		public static Add valueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
			return new Add(Collections.singletonList(value));
		}

		/**
		 * Add the value stored at the given field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Add}.
		 */
		public Add add(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Add(append(Fields.field(fieldReference)));
		}

		/**
		 * Add the evaluation result of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Add}.
		 */
		public Add add(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Add(append(expression));
		}

		/**
		 * Add the given value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Add}.
		 */
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
		 * @return new instance of {@link Ceil}.
		 */
		public static Ceil ceilValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Ceil(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Ceil}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Ceil}.
		 */
		public static Ceil ceilValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Ceil(expression);
		}

		/**
		 * Creates new {@link Ceil}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Ceil}.
		 */
		public static Ceil ceilValueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
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
		 * @return new instance of {@link Divide}.
		 */
		public static Divide valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Divide(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Divide}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Divide}.
		 */
		public static Divide valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Divide(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Divide}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Divide}.
		 */
		public static Divide valueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
			return new Divide(Collections.singletonList(value));
		}

		/**
		 * Divide by the value stored at the given field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Divide}.
		 */
		public Divide divideBy(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Divide(append(Fields.field(fieldReference)));
		}

		/**
		 * Divide by the evaluation results of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Divide}.
		 */
		public Divide divideBy(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Divide(append(expression));
		}

		/**
		 * Divide by the given value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Divide}.
		 */
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
		 * @return new instance of {@link Exp}.
		 */
		public static Exp expValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Exp(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Exp}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Exp}.
		 */
		public static Exp expValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Exp(expression);
		}

		/**
		 * Creates new {@link Exp}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Exp}.
		 */
		public static Exp expValueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
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
		 * @return new instance of {@link Floor}.
		 */
		public static Floor floorValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Floor(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Floor}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Floor}.
		 */
		public static Floor floorValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Floor(expression);
		}

		/**
		 * Creates new {@link Floor}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Floor}.
		 */
		public static Floor floorValueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
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
		 * @return new instance of {@link Ln}.
		 */
		public static Ln lnValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Ln(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Ln}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Ln}.
		 */
		public static Ln lnValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Ln(expression);
		}

		/**
		 * Creates new {@link Ln}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Ln}.
		 */
		public static Ln lnValueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
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
		 * @return new instance of {@link Log}.
		 */
		public static Log valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Log(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Log}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Log}.
		 */
		public static Log valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Log(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Log}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Log valueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
			return new Log(Collections.singletonList(value));
		}

		/**
		 * Use the value stored at the given field as log base.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Log}.
		 */
		public Log log(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Log(append(Fields.field(fieldReference)));
		}

		/**
		 * Use the evaluated value of the given {@link AggregationExpression} as log base.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Log}.
		 */
		public Log log(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Log(append(expression));
		}

		/**
		 * Use the given value as log base.
		 *
		 * @param base must not be {@literal null}.
		 * @return new instance of {@link Log}.
		 */
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
		 * @return new instance of {@link Log10}.
		 */
		public static Log10 log10ValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Log10(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Log10}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Log10}.
		 */
		public static Log10 log10ValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Log10(expression);
		}

		/**
		 * Creates new {@link Log10}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Log10}.
		 */
		public static Log10 log10ValueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
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
		 * @return new instance of {@link Mod}.
		 */
		public static Mod valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Mod(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Mod}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Mod}.
		 */
		public static Mod valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Mod(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Mod}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Mod}.
		 */
		public static Mod valueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
			return new Mod(Collections.singletonList(value));
		}

		/**
		 * Use the value stored at the given field as mod base.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Mod}.
		 */
		public Mod mod(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Mod(append(Fields.field(fieldReference)));
		}

		/**
		 * Use evaluated value of the given {@link AggregationExpression} as mod base.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Mod}.
		 */
		public Mod mod(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Mod(append(expression));
		}

		/**
		 * Use the given value as mod base.
		 *
		 * @param base must not be {@literal null}.
		 * @return new instance of {@link Mod}.
		 */
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
		 * @return new instance of {@link Multiply}.
		 */
		public static Multiply valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Multiply(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Multiply}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Multiply}.
		 */
		public static Multiply valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Multiply(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Multiply}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Multiply}.
		 */
		public static Multiply valueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
			return new Multiply(Collections.singletonList(value));
		}

		/**
		 * Multiply by the value stored at the given field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Multiply}.
		 */
		public Multiply multiplyBy(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Multiply(append(Fields.field(fieldReference)));
		}

		/**
		 * Multiply by the evaluated value of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Multiply}.
		 */
		public Multiply multiplyBy(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Multiply(append(expression));
		}

		/**
		 * Multiply by the given value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Multiply}.
		 */
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

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Pow(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Pow}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Pow valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Pow(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Pow}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Pow valueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
			return new Pow(Collections.singletonList(value));
		}

		/**
		 * Pow by the value stored at the given field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
		public Pow pow(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Pow(append(Fields.field(fieldReference)));
		}

		/**
		 * Pow by the evaluated value of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
		public Pow pow(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Pow(append(expression));
		}

		/**
		 * Pow by the given value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
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
		 * @return new instance of {@link Sqrt}.
		 */
		public static Sqrt sqrtOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Sqrt(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Sqrt}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Sqrt}.
		 */
		public static Sqrt sqrtOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Sqrt(expression);
		}

		/**
		 * Creates new {@link Sqrt}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Sqrt}.
		 */
		public static Sqrt sqrtOf(Number value) {

			Assert.notNull(value, "Value must not be null");
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
		 * @return new instance of {@link Subtract}.
		 */
		public static Subtract valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Subtract(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Subtract}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Subtract}.
		 */
		public static Subtract valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Subtract(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Subtract}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Subtract}.
		 */
		public static Subtract valueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
			return new Subtract(Collections.singletonList(value));
		}

		/**
		 * Subtract the value stored at the given field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
		public Subtract subtract(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Subtract(append(Fields.field(fieldReference)));
		}

		/**
		 * Subtract the evaluated value of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
		public Subtract subtract(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Subtract(append(expression));
		}

		/**
		 * Subtract the given value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Pow}.
		 */
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
		 * @return new instance of {@link Trunc}.
		 */
		public static Trunc truncValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Trunc(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Trunc}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Trunc}.
		 */
		public static Trunc truncValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Trunc(expression);
		}

		/**
		 * Creates new {@link Trunc}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Trunc}.
		 */
		public static Trunc truncValueOf(Number value) {

			Assert.notNull(value, "Value must not be null");
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

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Round(Collections.singletonList(Fields.field(fieldReference)));
		}

		/**
		 * Round the outcome of the given expression hat resolves to an integer, double, decimal, or long.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Round}.
		 */
		public static Round roundValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Round(Collections.singletonList(expression));
		}

		/**
		 * Round the given numeric (integer, double, decimal, or long) value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Round}.
		 */
		public static Round round(Number value) {

			Assert.notNull(value, "Value must not be null");
			return new Round(Collections.singletonList(value));
		}

		/**
		 * The place to round to. Can be between -20 and 100, exclusive.
		 *
		 * @param place value between -20 and 100, exclusive.
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

			Assert.notNull(expression, "Expression must not be null");
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

			Assert.notNull(fieldReference, "fieldReference must not be null");
			return new Round(append(Fields.field(fieldReference)));
		}

		@Override
		protected String getMongoMethod() {
			return "$round";
		}
	}

	/**
	 * Value object to represent an {@link AggregationExpression expression} that calculates the average rate of change
	 * within the specified window.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Derivative extends AbstractAggregationExpression {

		private Derivative(Object value) {
			super(value);
		}

		/**
		 * Create a new instance of {@link Derivative} for the value stored at the given field holding a numeric value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Derivative}.
		 */
		public static Derivative derivativeOf(String fieldReference) {
			return new Derivative(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Create a new instance of {@link Derivative} for the value provided by the given expression that resolves to a
		 * numeric value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Derivative}.
		 */
		public static Derivative derivativeOf(AggregationExpression expression) {
			return new Derivative(Collections.singletonMap("input", expression));
		}

		public static Derivative derivativeOfValue(Number value) {
			return new Derivative(Collections.singletonMap("input", value));
		}

		public Derivative unit(String unit) {
			return new Derivative(append("unit", unit));
		}

		@Override
		protected String getMongoMethod() {
			return "$derivative";
		}
	}

	/**
	 * Value object to represent an {@link AggregationExpression expression} that calculates the approximation for the
	 * mathematical integral value.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Integral extends AbstractAggregationExpression {

		private Integral(Object value) {
			super(value);
		}

		/**
		 * Create a new instance of {@link Integral} for the value stored at the given field holding a numeric value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Integral}.
		 */
		public static Integral integralOf(String fieldReference) {
			return new Integral(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Create a new instance of {@link Integral} for the value provided by the given expression that resolves to a
		 * numeric value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Integral}.
		 */
		public static Integral integralOf(AggregationExpression expression) {
			return new Integral(Collections.singletonMap("input", expression));
		}

		/**
		 * Set the unit of measure.
		 *
		 * @param unit the unit of measure.
		 * @return new instance of {@link Integral}.
		 */
		public Integral unit(String unit) {
			return new Integral(append("unit", unit));
		}

		@Override
		protected String getMongoMethod() {
			return "$integral";
		}
	}

	/**
	 * The unit of measure for computations that operate upon angles.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public enum AngularUnit {
		RADIANS, DEGREES
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the sine of a value that is measured in radians.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Sin extends AbstractAggregationExpression {

		private Sin(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the sine of a value that is measured in
		 * {@link AngularUnit#RADIANS radians}.
		 * <br />
		 * Use {@code sinhOf("angle", DEGREES)} as shortcut for
		 *
		 * <pre>
		 * { $sinh : { $degreesToRadians : "$angle" } }
		 * </pre>
		 *
		 * .
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link Sin}.
		 */
		public static Sin sinOf(String fieldReference) {
			return sinOf(fieldReference, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the sine of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Sin}.
		 */
		public static Sin sinOf(String fieldReference, AngularUnit unit) {
			return sin(Fields.field(fieldReference), unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the sine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link Sin}.
		 */
		public static Sin sinOf(AggregationExpression expression) {
			return sinOf(expression, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the sine of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Sin}.
		 */
		public static Sin sinOf(AggregationExpression expression, AngularUnit unit) {
			return sin(expression, unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the sine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value
		 * @return new instance of {@link Sin}.
		 */
		public static Sin sin(Object value) {
			return sin(value, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the sine of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Sin}.
		 */
		public static Sin sin(Object value, AngularUnit unit) {

			if (ObjectUtils.nullSafeEquals(AngularUnit.DEGREES, unit)) {
				return new Sin(ConvertOperators.DegreesToRadians.degreesToRadians(value));
			}
			return new Sin(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$sin";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the hyperbolic sine of a value that is measured in
	 * {@link AngularUnit#RADIANS}.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Sinh extends AbstractAggregationExpression {

		private Sinh(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic sine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link Sin}.
		 */
		public static Sinh sinhOf(String fieldReference) {
			return sinhOf(fieldReference, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic sine of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 * <br />
		 * Use {@code sinhOf("angle", DEGREES)} as shortcut for
		 *
		 * <pre>
		 * { $sinh : { $degreesToRadians : "$angle" } }
		 * </pre>
		 *
		 * .
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Sin}.
		 */
		public static Sinh sinhOf(String fieldReference, AngularUnit unit) {
			return sinh(Fields.field(fieldReference), unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic sine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 * <br />
		 * Use {@code sinhOf("angle", DEGREES)} as shortcut for eg.
		 * {@code sinhOf(ConvertOperators.valueOf("angle").degreesToRadians())}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link Sin}.
		 */
		public static Sinh sinhOf(AggregationExpression expression) {
			return sinhOf(expression, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic sine of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Sin}.
		 */
		public static Sinh sinhOf(AggregationExpression expression, AngularUnit unit) {
			return sinh(expression, unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic sine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link Sin}.
		 */
		public static Sinh sinh(Object value) {
			return sinh(value, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic sine of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Sin}.
		 */
		public static Sinh sinh(Object value, AngularUnit unit) {

			if (ObjectUtils.nullSafeEquals(AngularUnit.DEGREES, unit)) {
				return new Sinh(ConvertOperators.DegreesToRadians.degreesToRadians(value));
			}
			return new Sinh(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$sinh";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the inverse sine of a value.
	 *
	 * @author Divya Srivastava
	 * @since 3.3
	 */
	public static class ASin extends AbstractAggregationExpression {

		private ASin(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse sine of a value.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link ASin}.
		 */
		public static ASin asinOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new ASin(Fields.field(fieldReference));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse sine of a value.
		 * <br />
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link ASin}.
		 */
		public static ASin asinOf(AggregationExpression expression) {
			return new ASin(expression);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse sine of a value.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link ASin}.
		 */
		public static ASin asinOf(Number value) {
			return new ASin(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$asin";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the inverse hyperbolic sine of a value
	 *
	 * @author Divya Srivastava
	 * @since 3.3
	 */
	public static class ASinh extends AbstractAggregationExpression {

		private ASinh(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse hyperbolic sine of a value.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link ASinh}.
		 */
		public static ASinh asinhOf(String fieldReference) {
			return new ASinh(Fields.field(fieldReference));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse hyperbolic sine of a value.
		 * <br />
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link ASinh}.
		 */
		public static ASinh asinhOf(AggregationExpression expression) {
			return new ASinh(expression);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse hyperbolic sine of a value.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link ASinh}.
		 */
		public static ASinh asinhOf(Object value) {
			return new ASinh(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$asinh";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the cosine of a value that is measured in radians.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Cos extends AbstractAggregationExpression {

		private Cos(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the cosine of a value that is measured in
		 * {@link AngularUnit#RADIANS radians}.
		 * <br />
		 * Use {@code cosOf("angle", DEGREES)} as shortcut for
		 *
		 * <pre>
		 * { $cos : { $degreesToRadians : "$angle" } }
		 * </pre>
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link Cos}.
		 */
		public static Cos cosOf(String fieldReference) {
			return cosOf(fieldReference, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the cosine of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Cos}.
		 */
		public static Cos cosOf(String fieldReference, AngularUnit unit) {
			return cos(Fields.field(fieldReference), unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the cosine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link Cos}.
		 */
		public static Cos cosOf(AggregationExpression expression) {
			return cosOf(expression, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the cosine of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Cos}.
		 */
		public static Cos cosOf(AggregationExpression expression, AngularUnit unit) {
			return cos(expression, unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the cosine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value
		 * @return new instance of {@link Cos}.
		 */
		public static Cos cos(Object value) {
			return cos(value, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the cosine of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Cos}.
		 */
		public static Cos cos(Object value, AngularUnit unit) {

			if (ObjectUtils.nullSafeEquals(AngularUnit.DEGREES, unit)) {
				return new Cos(ConvertOperators.DegreesToRadians.degreesToRadians(value));
			}
			return new Cos(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$cos";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the hyperbolic cosine of a value that is measured in
	 * {@link AngularUnit#RADIANS}.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Cosh extends AbstractAggregationExpression {

		private Cosh(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic cosine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link Cosh}.
		 */
		public static Cosh coshOf(String fieldReference) {
			return coshOf(fieldReference, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic cosine of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 * <br />
		 * Use {@code coshOf("angle", DEGREES)} as shortcut for
		 *
		 * <pre>
		 * { $cosh : { $degreesToRadians : "$angle" } }
		 * </pre>
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Cosh}.
		 */
		public static Cosh coshOf(String fieldReference, AngularUnit unit) {
			return cosh(Fields.field(fieldReference), unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic cosine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 * <br />
		 * Use {@code sinhOf("angle", DEGREES)} as shortcut for eg.
		 * {@code sinhOf(ConvertOperators.valueOf("angle").degreesToRadians())}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link Cosh}.
		 */
		public static Cosh coshOf(AggregationExpression expression) {
			return coshOf(expression, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic cosine of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Cosh}.
		 */
		public static Cosh coshOf(AggregationExpression expression, AngularUnit unit) {
			return cosh(expression, unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic cosine of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link Cosh}.
		 */
		public static Cosh cosh(Object value) {
			return cosh(value, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic cosine of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Cosh}.
		 */
		public static Cosh cosh(Object value, AngularUnit unit) {

			if (ObjectUtils.nullSafeEquals(AngularUnit.DEGREES, unit)) {
				return new Cosh(ConvertOperators.DegreesToRadians.degreesToRadians(value));
			}
			return new Cosh(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$cosh";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the inverse cosine of a value.
	 *
	 * @author Divya Srivastava
	 * @since 3.4
	 */
	public static class ACos extends AbstractAggregationExpression {

		private ACos(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse cosine of a value.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link ACos}.
		 */
		public static ACos acosOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new ACos(Fields.field(fieldReference));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse cosine of a value.
		 * <br />
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link ACos}.
		 */
		public static ACos acosOf(AggregationExpression expression) {
			return new ACos(expression);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse cosine of a value.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link ACos}.
		 */
		public static ACos acosOf(Number value) {
			return new ACos(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$acos";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the inverse hyperbolic cosine of a value.
	 *
	 * @author Divya Srivastava
	 * @since 3.4
	 */
	public static class ACosh extends AbstractAggregationExpression {

		private ACosh(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse hyperbolic cosine of a value.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link ACosh}.
		 */
		public static ACosh acoshOf(String fieldReference) {
			return new ACosh(Fields.field(fieldReference));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse hyperbolic cosine of a value.
		 * <br />
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link ACosh}.
		 */
		public static ACosh acoshOf(AggregationExpression expression) {
			return new ACosh(expression);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse hyperbolic cosine of a value.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link ACosh}.
		 */
		public static ACosh acoshOf(Object value) {
			return new ACosh(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$acosh";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the tangent of a value that is measured in radians.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Tan extends AbstractAggregationExpression {

		private Tan(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the tangent of a value that is measured in
		 * {@link AngularUnit#RADIANS radians}.
		 * <br />
		 * Use {@code tanOf("angle", DEGREES)} as shortcut for
		 *
		 * <pre>
		 * { $tan : { $degreesToRadians : "$angle" } }
		 * </pre>
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link Tan}.
		 */
		public static Tan tanOf(String fieldReference) {
			return tanOf(fieldReference, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the tangent of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Tan}.
		 */
		public static Tan tanOf(String fieldReference, AngularUnit unit) {
			return tan(Fields.field(fieldReference), unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the tangent of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link Tan}.
		 */
		public static Tan tanOf(AggregationExpression expression) {
			return tanOf(expression, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the tangent of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Tan}.
		 */
		public static Tan tanOf(AggregationExpression expression, AngularUnit unit) {
			return tan(expression, unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the tangent of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value
		 * @return new instance of {@link Tan}.
		 */
		public static Tan tan(Object value) {
			return tan(value, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the tangent of a value that is measured in the given
		 * {@link AngularUnit unit}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Tan}.
		 */
		public static Tan tan(Object value, AngularUnit unit) {

			if (ObjectUtils.nullSafeEquals(AngularUnit.DEGREES, unit)) {
				return new Tan(ConvertOperators.DegreesToRadians.degreesToRadians(value));
			}
			return new Tan(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$tan";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the inverse tangent of a value.
	 *
	 * @author Divya Srivastava
	 * @since 3.3
	 */
	public static class ATan extends AbstractAggregationExpression {

		private ATan(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse tangent of a value.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link ATan}.
		 */
		public static ATan atanOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new ATan(Fields.field(fieldReference));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse tangent of a value.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link ATan}.
		 */
		public static ATan atanOf(AggregationExpression expression) {
			return new ATan(expression);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse tangent of a value.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link ATan}.
		 */
		public static ATan atanOf(Number value) {
			return new ATan(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$atan";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the inverse tangent of y / x, where y and x are the
	 * first and second values passed to the expression respectively.
	 *
	 * @author Divya Srivastava
	 * @since 3.3
	 */
	public static class ATan2 extends AbstractAggregationExpression {

		private ATan2(List<?> value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse tangent of of y / x, where y and x are
		 * the first and second values passed to the expression respectively.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link ATan2}.
		 */
		public static ATan2 valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new ATan2(asFields(fieldReference));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse tangent of of y / x, where y and x are
		 * the first and second values passed to the expression respectively.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link ATan2}.
		 */
		public static ATan2 valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new ATan2((Collections.singletonList(expression)));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse tangent of of y / x, where y and x are
		 * the first and second values passed to the expression respectively.
		 *
		 * @param fieldReference anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link ATan2}.
		 */
		public ATan2 atan2of(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new ATan2(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic tangent of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param expression anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link ATan2}.
		 */
		public ATan2 atan2of(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new ATan2(append(expression));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse tangent of of y / x, where y and x are
		 * the first and second values passed to the expression respectively.
		 *
		 * @param value of type {@link Number}
		 * @return new instance of {@link ATan2}.
		 */
		public ATan2 atan2of(Number value) {
			return new ATan2(append(value));
		}

		@Override
		protected String getMongoMethod() {
			return "$atan2";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the hyperbolic tangent of a value that is measured in
	 * {@link AngularUnit#RADIANS}.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Tanh extends AbstractAggregationExpression {

		private Tanh(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic tangent of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @return new instance of {@link Tanh}.
		 */
		public static Tanh tanhOf(String fieldReference) {
			return tanhOf(fieldReference, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic tangent of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 * <br />
		 * Use {@code tanhOf("angle", DEGREES)} as shortcut for
		 *
		 * <pre>
		 * { $tanh : { $degreesToRadians : "$angle" } }
		 * </pre>
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Tanh}.
		 */
		public static Tanh tanhOf(String fieldReference, AngularUnit unit) {
			return tanh(Fields.field(fieldReference), unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic tangent of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 * <br />
		 * Use {@code sinhOf("angle", DEGREES)} as shortcut for eg.
		 * {@code sinhOf(ConvertOperators.valueOf("angle").degreesToRadians())}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link Tanh}.
		 */
		public static Tanh tanhOf(AggregationExpression expression) {
			return tanhOf(expression, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic tangent of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Tanh}.
		 */
		public static Tanh tanhOf(AggregationExpression expression, AngularUnit unit) {
			return tanh(expression, unit);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic tangent of a value that is measured in
		 * {@link AngularUnit#RADIANS}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value.
		 * @return new instance of {@link Tanh}.
		 */
		public static Tanh tanh(Object value) {
			return tanh(value, AngularUnit.RADIANS);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the hyperbolic tangent of a value that is measured in
		 * the given {@link AngularUnit unit}.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression expression}, ...) that resolves to a
		 *          numeric value
		 * @param unit the unit of measure used by the value of the given field.
		 * @return new instance of {@link Tanh}.
		 */
		public static Tanh tanh(Object value, AngularUnit unit) {

			if (ObjectUtils.nullSafeEquals(AngularUnit.DEGREES, unit)) {
				return new Tanh(ConvertOperators.DegreesToRadians.degreesToRadians(value));
			}
			return new Tanh(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$tanh";
		}
	}

	/**
	 * An {@link AggregationExpression expression} that calculates the inverse hyperbolic tangent of a value
	 *
	 * @author Divya Srivastava
	 * @since 3.3
	 */
	public static class ATanh extends AbstractAggregationExpression {

		private ATanh(Object value) {
			super(value);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse
		 * hyperbolic tangent of a value.
		 *
		 * @param fieldReference the name of the {@link Field field} that resolves to a
		 *                       numeric value.
		 * @return new instance of {@link ATanh}.
		 */
		public static ATanh atanhOf(String fieldReference) {
			return new ATanh(Fields.field(fieldReference));
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse hyperbolic tangent of a value.
		 * <br />
		 *
		 * @param expression the {@link AggregationExpression expression} that resolves to a numeric value.
		 * @return new instance of {@link ATanh}.
		 */
		public static ATanh atanhOf(AggregationExpression expression) {
			return new ATanh(expression);
		}

		/**
		 * Creates a new {@link AggregationExpression} that calculates the inverse
		 * hyperbolic tangent of a value.
		 *
		 * @param value anything ({@link Field field}, {@link AggregationExpression
		 *              expression}, ...) that resolves to a numeric value.
		 * @return new instance of {@link ATanh}.
		 */
		public static ATanh atanhOf(Object value) {
			return new ATanh(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$atanh";
		}
	}

	/**
	 * {@link Rand} returns a floating value between 0 and 1.
	 *
	 * @author Mushtaq Ahmed
	 * @since 3.3
	 */
	public static class Rand implements AggregationExpression {

		@Override
		public Document toDocument(AggregationOperationContext context) {
			return new Document("$rand", new Document());
		}
	}
}
