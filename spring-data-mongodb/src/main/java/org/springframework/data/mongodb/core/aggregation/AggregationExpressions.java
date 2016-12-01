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
	 * Gateway to {@literal boolean expressions} that evaluate their argument expressions as booleans and return a boolean
	 * as the result.
	 *
	 * @author Christoph Strobl
	 */
	class BooleanOperators {

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static BooleanOperatorFactory valueOf(String fieldReference) {
			return new BooleanOperatorFactory(fieldReference);
		}

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static BooleanOperatorFactory valueOf(AggregationExpression fieldReference) {
			return new BooleanOperatorFactory(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpressions} that evaluates the boolean value of the referenced field and returns
		 * the opposite boolean value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Not not(String fieldReference) {
			return Not.not(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpressions} that evaluates the boolean value of {@link AggregationExpression}
		 * result and returns the opposite boolean value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Not not(AggregationExpression expression) {
			return Not.not(expression);
		}

		public static class BooleanOperatorFactory {

			private final String fieldReference;
			private final AggregationExpression expression;

			/**
			 * Creates new {@link ComparisonOperatorFactory} for given {@literal fieldReference}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 */
			public BooleanOperatorFactory(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				this.fieldReference = fieldReference;
				this.expression = null;
			}

			/**
			 * Creats new {@link ComparisonOperatorFactory} for given {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 */
			public BooleanOperatorFactory(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				this.fieldReference = null;
				this.expression = expression;
			}

			/**
			 * Creates new {@link AggregationExpressions} that evaluates one or more expressions and returns {@literal true}
			 * if all of the expressions are {@literal true}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public And and(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createAnd().andExpression(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that evaluates one or more expressions and returns {@literal true}
			 * if all of the expressions are {@literal true}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public And and(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createAnd().andField(fieldReference);
			}

			private And createAnd() {
				return usesFieldRef() ? And.and(Fields.field(fieldReference)) : And.and(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that evaluates one or more expressions and returns {@literal true}
			 * if any of the expressions are {@literal true}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Or or(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createOr().orExpression(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that evaluates one or more expressions and returns {@literal true}
			 * if any of the expressions are {@literal true}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Or or(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createOr().orField(fieldReference);
			}

			private Or createOr() {
				return usesFieldRef() ? Or.or(Fields.field(fieldReference)) : Or.or(expression);
			}

			/**
			 * Creates new {@link AggregationExpression} that evaluates a boolean and returns the opposite boolean value.
			 *
			 * @return
			 */
			public Not not() {
				return usesFieldRef() ? Not.not(fieldReference) : Not.not(expression);
			}

			private boolean usesFieldRef() {
				return this.fieldReference != null;
			}
		}
	}

	/**
	 * Gateway to {@literal Set expressions} which perform {@literal set} operation on arrays, treating arrays as sets.
	 *
	 * @author Christoph Strobl
	 */
	class SetOperators {

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static SetOperatorFactory arrayAsSet(String fieldReference) {
			return new SetOperatorFactory(fieldReference);
		}

		public static class SetOperatorFactory {

			private final String fieldReference;

			/**
			 * Creates new {@link SetOperatorFactory} for given {@literal fieldReference}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 */
			public SetOperatorFactory(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				this.fieldReference = fieldReference;
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares the previously mentioned field to one or more arrays
			 * and returns {@literal true} if they have the same distinct elements and {@literal false} otherwise.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetEquals isEqualTo(String... arrayReferences) {
				return SetEquals.arrayAsSet(fieldReference).isEqualTo(arrayReferences);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and one or more
			 * arrays and returns an array that contains the elements that appear in every of those.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetIntersection intersects(String... arrayReferences) {
				return SetIntersection.arrayAsSet(fieldReference).intersects(arrayReferences);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and one or more
			 * arrays and returns an array that contains the elements that appear in any of those.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetUnion union(String... arrayReferences) {
				return SetUnion.arrayAsSet(fieldReference).union(arrayReferences);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and returns an
			 * array containing the elements that do not exist in the given {@literal arrayReference}.
			 *
			 * @param arrayReference must not be {@literal null}.
			 * @return
			 */
			public SetDifference differenceTo(String arrayReference) {
				return SetDifference.arrayAsSet(fieldReference).differenceTo(arrayReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and returns
			 * {@literal true} if it is a subset of the given {@literal arrayReference}.
			 *
			 * @param arrayReference must not be {@literal null}.
			 * @return
			 */
			public SetIsSubset isSubsetOf(String arrayReference) {
				return SetIsSubset.arrayAsSet(fieldReference).isSubsetOf(arrayReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and returns
			 * {@literal true} if any of the elements are {@literal true} and {@literal false} otherwise.
			 *
			 * @return
			 */
			public AnyElementTrue anyElementTrue() {
				return AnyElementTrue.arrayAsSet(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that tkes array of the previously mentioned field and returns
			 * {@literal true} if no elements is {@literal false}.
			 *
			 * @return
			 */
			public AllElementsTrue allElementsTrue() {
				return AllElementsTrue.arrayAsSet(fieldReference);
			}
		}
	}

	/**
	 * Gateway to {@literal comparison expressions}.
	 *
	 * @author Christoph Strobl
	 */
	class ComparisonOperators {

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static ComparisonOperatorFactory valueOf(String fieldReference) {
			return new ComparisonOperatorFactory(fieldReference);
		}

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static ComparisonOperatorFactory valueOf(AggregationExpression fieldReference) {
			return new ComparisonOperatorFactory(fieldReference);
		}

		public static class ComparisonOperatorFactory {

			private final String fieldReference;
			private final AggregationExpression expression;

			/**
			 * Creates new {@link ComparisonOperatorFactory} for given {@literal fieldReference}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 */
			public ComparisonOperatorFactory(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				this.fieldReference = fieldReference;
				this.expression = null;
			}

			/**
			 * Creats new {@link ComparisonOperatorFactory} for given {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 */
			public ComparisonOperatorFactory(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				this.fieldReference = null;
				this.expression = expression;
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Cmp compareTo(String fieldReference) {
				return createCmp().compareTo(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Cmp compareTo(AggregationExpression expression) {
				return createCmp().compareTo(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Cmp compareToValue(Object value) {
				return createCmp().compareToValue(value);
			}

			private Cmp createCmp() {
				return usesFieldRef() ? Cmp.valueOf(fieldReference) : Cmp.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is equal to the value of the referenced field.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Eq equalTo(String fieldReference) {
				return createEq().equalTo(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is equal to the expression result.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Eq equalTo(AggregationExpression expression) {
				return createEq().equalTo(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is equal to the given value.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Eq equalToValue(Object value) {
				return createEq().equalToValue(value);
			}

			private Eq createEq() {
				return usesFieldRef() ? Eq.valueOf(fieldReference) : Eq.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is greater than the value of the referenced field.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Gt greaterThan(String fieldReference) {
				return createGt().greaterThan(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is greater than the expression result.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Gt greaterThan(AggregationExpression expression) {
				return createGt().greaterThan(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is greater than the given value.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Gt greaterThanValue(Object value) {
				return createGt().greaterThanValue(value);
			}

			private Gt createGt() {
				return usesFieldRef() ? Gt.valueOf(fieldReference) : Gt.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is greater than or equivalent to the value of the referenced field.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Gte greaterThanEqualTo(String fieldReference) {
				return createGte().greaterThanEqualTo(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is greater than or equivalent to the expression result.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Gte greaterThanEqualTo(AggregationExpression expression) {
				return createGte().greaterThanEqualTo(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is greater than or equivalent to the given value.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Gte greaterThanEqualToValue(Object value) {
				return createGte().greaterThanEqualToValue(value);
			}

			private Gte createGte() {
				return usesFieldRef() ? Gte.valueOf(fieldReference) : Gte.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is less than the value of the referenced field.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Lt lessThan(String fieldReference) {
				return createLt().lessThan(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is less than the expression result.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Lt lessThan(AggregationExpression expression) {
				return createLt().lessThan(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is less than to the given value.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Lt lessThanValue(Object value) {
				return createLt().lessThanValue(value);
			}

			private Lt createLt() {
				return usesFieldRef() ? Lt.valueOf(fieldReference) : Lt.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is less than or equivalent to the value of the referenced field.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Lte lessThanEqualTo(String fieldReference) {
				return createLte().lessThanEqualTo(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is less than or equivalent to the expression result.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Lte lessThanEqualTo(AggregationExpression expression) {
				return createLte().lessThanEqualTo(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the first
			 * value is less than or equivalent to the given value.
			 *
			 * @param value
			 * @return
			 */
			public Lte lessThanEqualToValue(Object value) {
				return createLte().lessThanEqualToValue(value);
			}

			private Lte createLte() {
				return usesFieldRef() ? Lte.valueOf(fieldReference) : Lte.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the values
			 * are not equivalent.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Ne notEqualTo(String fieldReference) {
				return createNe().notEqualTo(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the values
			 * are not equivalent.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Ne notEqualTo(AggregationExpression expression) {
				return createNe().notEqualTo(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that compares two values and returns {@literal true} when the values
			 * are not equivalent.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Ne notEqualToValue(Object value) {
				return createNe().notEqualToValue(value);
			}

			private Ne createNe() {
				return usesFieldRef() ? Ne.valueOf(fieldReference) : Ne.valueOf(expression);
			}

			private boolean usesFieldRef() {
				return fieldReference != null;
			}
		}

	}

	/**
	 * Gateway to {@literal Arithmetic} aggregation operations that perform mathematic operations on numbers.
	 *
	 * @author Christoph Strobl
	 */
	class ArithmeticOperators {

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static ArithmeticOperatorFactory valueOf(String fieldReference) {
			return new ArithmeticOperatorFactory(fieldReference);
		}

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static ArithmeticOperatorFactory valueOf(AggregationExpression fieldReference) {
			return new ArithmeticOperatorFactory(fieldReference);
		}

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
			 * Creates new {@link AggregationExpressions} that returns the absolute value of the associated number.
			 *
			 * @return
			 */
			public Abs abs() {
				return fieldReference != null ? Abs.absoluteValueOf(fieldReference) : Abs.absoluteValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that adds the value of {@literal fieldReference} to the associated
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
			 * Creates new {@link AggregationExpressions} that adds the resulting value of the given
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
			 * Creates new {@link AggregationExpressions} that adds the given {@literal value} to the associated number.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Add add(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createAdd().add(value);
			}

			private Add createAdd() {
				return fieldReference != null ? Add.valueOf(fieldReference) : Add.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the smallest integer greater than or equal to the
			 * assoicated number.
			 *
			 * @return
			 */
			public Ceil ceil() {
				return fieldReference != null ? Ceil.ceilValueOf(fieldReference) : Ceil.ceilValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that ivides the associated number by number referenced via
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
			 * Creates new {@link AggregationExpressions} that divides the associated number by number extracted via
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
			 * Creates new {@link AggregationExpressions} that divides the associated number by given {@literal value}.
			 *
			 * @param value
			 * @return
			 */
			public Divide divideBy(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createDivide().divideBy(value);
			}

			private Divide createDivide() {
				return fieldReference != null ? Divide.valueOf(fieldReference) : Divide.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that raises Euler’s number (i.e. e ) on the associated number.
			 *
			 * @return
			 */
			public Exp exp() {
				return fieldReference != null ? Exp.expValueOf(fieldReference) : Exp.expValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the largest integer less than or equal to the
			 * associated number.
			 *
			 * @return
			 */
			public Floor floor() {
				return fieldReference != null ? Floor.floorValueOf(fieldReference) : Floor.floorValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates the natural logarithm ln (i.e loge) of the
			 * assoicated number.
			 *
			 * @return
			 */
			public Ln ln() {
				return fieldReference != null ? Ln.lnValueOf(fieldReference) : Ln.lnValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates the log of the associated number in the specified
			 * base referenced via {@literal fieldReference}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Log log(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createLog().log(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates the log of the associated number in the specified
			 * base extracted by given {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Log log(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createLog().log(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates the log of a the associated number in the specified
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
				return fieldReference != null ? Log.valueOf(fieldReference) : Log.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates the log base 10 for the associated number.
			 *
			 * @return
			 */
			public Log10 log10() {
				return fieldReference != null ? Log10.log10ValueOf(fieldReference) : Log10.log10ValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that divides the associated number by another and returns the
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
			 * Creates new {@link AggregationExpressions} that divides the associated number by another and returns the
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
			 * Creates new {@link AggregationExpressions} that divides the associated number by another and returns the
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
				return fieldReference != null ? Mod.valueOf(fieldReference) : Mod.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that multiplies the associated number with another.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createMultiply().multiplyBy(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that multiplies the associated number with another.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createMultiply().multiplyBy(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that multiplies the associated number with another.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Multiply multiplyBy(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createMultiply().multiplyBy(value);
			}

			private Multiply createMultiply() {
				return fieldReference != null ? Multiply.valueOf(fieldReference) : Multiply.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that raises the associated number to the specified exponent.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Pow pow(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createPow().pow(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that raises the associated number to the specified exponent.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Pow pow(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createPow().pow(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that raises the associated number to the specified exponent.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Pow pow(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createPow().pow(value);
			}

			private Pow createPow() {
				return fieldReference != null ? Pow.valueOf(fieldReference) : Pow.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates the square root of the associated number.
			 *
			 * @return
			 */
			public Sqrt sqrt() {
				return fieldReference != null ? Sqrt.sqrtOf(fieldReference) : Sqrt.sqrtOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that subtracts value of given from the associated number.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Subtract subtract(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createSubtract().subtract(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that subtracts value of given from the associated number.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Subtract subtract(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createSubtract().subtract(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that subtracts value from the associated number.
			 *
			 * @param value
			 * @return
			 */
			public Subtract subtract(Number value) {

				Assert.notNull(value, "Value must not be null!");
				return createSubtract().subtract(value);
			}

			private Subtract createSubtract() {
				return fieldReference != null ? Subtract.valueOf(fieldReference) : Subtract.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that truncates a number to its integer.
			 *
			 * @return
			 */
			public Trunc trunc() {
				return fieldReference != null ? Trunc.truncValueOf(fieldReference) : Trunc.truncValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates and returns the sum of numeric values.
			 *
			 * @return
			 */
			public Sum sum() {
				return fieldReference != null ? Sum.sumOf(fieldReference) : Sum.sumOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the average value of the numeric values.
			 *
			 * @return
			 */
			public Avg avg() {
				return fieldReference != null ? Avg.avgOf(fieldReference) : Avg.avgOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the maximum value.
			 *
			 * @return
			 */
			public Max max() {
				return fieldReference != null ? Max.maxOf(fieldReference) : Max.maxOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the minimum value.
			 *
			 * @return
			 */
			public Min min() {
				return fieldReference != null ? Min.minOf(fieldReference) : Min.minOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates the population standard deviation of the input
			 * values.
			 *
			 * @return
			 */
			public StdDevPop stdDevPop() {
				return fieldReference != null ? StdDevPop.stdDevPopOf(fieldReference) : StdDevPop.stdDevPopOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that calculates the sample standard deviation of the input values.
			 *
			 * @return
			 */
			public StdDevSamp stdDevSamp() {
				return fieldReference != null ? StdDevSamp.stdDevSampOf(fieldReference) : StdDevSamp.stdDevSampOf(expression);
			}
		}
	}

	/**
	 * Gateway to {@literal String} aggregation operations.
	 *
	 * @author Christoph Strobl
	 */
	class StringOperators {

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static StringOperatorFactory valueOf(String fieldReference) {
			return new StringOperatorFactory(fieldReference);
		}

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static StringOperatorFactory valueOf(AggregationExpression fieldReference) {
			return new StringOperatorFactory(fieldReference);
		}

		public static class StringOperatorFactory {

			private final String fieldReference;
			private final AggregationExpression expression;

			public StringOperatorFactory(String fieldReference) {
				this.fieldReference = fieldReference;
				this.expression = null;
			}

			public StringOperatorFactory(AggregationExpression expression) {
				this.fieldReference = null;
				this.expression = expression;
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and concats the
			 * value of the referenced field to it.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public Concat concatValueOf(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createConcat().concatValueOf(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and concats the
			 * result of the given {@link AggregationExpression} to it.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public Concat concatValueOf(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createConcat().concatValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and concats given
			 * {@literal value} to it.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public Concat concat(String value) {

				Assert.notNull(value, "Value must not be null!");
				return createConcat().concat(value);
			}

			private Concat createConcat() {
				return fieldReference != null ? Concat.valueOf(fieldReference) : Concat.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and returns a
			 * substring starting at a specified index position.
			 *
			 * @param start
			 * @return
			 */
			public Substr substring(int start) {
				return substring(start, -1);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and returns a
			 * substring starting at a specified index position including the specified number of characters.
			 *
			 * @param start
			 * @param nrOfChars
			 * @return
			 */
			public Substr substring(int start, int nrOfChars) {
				return createSubstr().substring(start, nrOfChars);
			}

			private Substr createSubstr() {
				return fieldReference != null ? Substr.valueOf(fieldReference) : Substr.valueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and lowers it.
			 *
			 * @return
			 */
			public ToLower toLower() {
				return fieldReference != null ? ToLower.lowerValueOf(fieldReference) : ToLower.lowerValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and uppers it.
			 *
			 * @return
			 */
			public ToUpper toUpper() {
				return fieldReference != null ? ToUpper.upperValueOf(fieldReference) : ToUpper.upperValueOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and performs
			 * case-insensitive comparison to the given {@literal value}.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public StrCaseCmp strCaseCmp(String value) {

				Assert.notNull(value, "Value must not be null!");
				return createStrCaseCmp().strcasecmp(value);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and performs
			 * case-insensitive comparison to the referenced {@literal fieldReference}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public StrCaseCmp strCaseCmpValueOf(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createStrCaseCmp().strcasecmpValueOf(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated string representation and performs
			 * case-insensitive comparison to the result of the given {@link AggregationExpression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public StrCaseCmp strCaseCmpValueOf(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createStrCaseCmp().strcasecmpValueOf(expression);
			}

			private StrCaseCmp createStrCaseCmp() {
				return fieldReference != null ? StrCaseCmp.valueOf(fieldReference) : StrCaseCmp.valueOf(expression);
			}
		}
	}

	/**
	 * Gateway to {@litearl array} aggregation operations.
	 *
	 * @author Christoph Strobl
	 */
	class ArrayOperators {

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static ArrayOperatorFactory arrayOf(String fieldReference) {
			return new ArrayOperatorFactory(fieldReference);
		}

		/**
		 * Take the array referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static ArrayOperatorFactory arrayOf(AggregationExpression expression) {
			return new ArrayOperatorFactory(expression);
		}

		public static class ArrayOperatorFactory {

			private final String fieldReference;
			private final AggregationExpression expression;

			public ArrayOperatorFactory(String fieldReference) {
				this.fieldReference = fieldReference;
				this.expression = null;
			}

			public ArrayOperatorFactory(AggregationExpression expression) {
				this.fieldReference = null;
				this.expression = expression;
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and returns the element at the
			 * specified array {@literal position}.
			 *
			 * @param position
			 * @return
			 */
			public ArrayElemtAt elementAt(int position) {
				return createArrayElemAt().elementAt(position);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and returns the element at the
			 * position resulting form the given {@literal expression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public ArrayElemtAt elementAt(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createArrayElemAt().elementAt(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and returns the element at the
			 * position defined by the referenced {@literal field}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public ArrayElemtAt elementAt(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return createArrayElemAt().elementAt(fieldReference);
			}

			private ArrayElemtAt createArrayElemAt() {
				return usesFieldRef() ? ArrayElemtAt.arrayOf(fieldReference) : ArrayElemtAt.arrayOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and concats the given
			 * {@literal arrayFieldReference} to it.
			 *
			 * @param arrayFieldReference must not be {@literal null}.
			 * @return
			 */
			public ConcatArrays concat(String arrayFieldReference) {

				Assert.notNull(arrayFieldReference, "ArrayFieldReference must not be null!");
				return createConcatArrays().concat(arrayFieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and concats the array resulting form
			 * the given {@literal expression} to it.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public ConcatArrays concat(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return createConcatArrays().concat(expression);
			}

			private ConcatArrays createConcatArrays() {
				return usesFieldRef() ? ConcatArrays.arrayOf(fieldReference) : ConcatArrays.arrayOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and selects a subset of the array to
			 * return based on the specified condition.
			 *
			 * @return
			 */
			public AsBuilder filter() {
				return Filter.filter(fieldReference);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and an check if its an array.
			 *
			 * @return
			 */
			public IsArray isArray() {
				return usesFieldRef() ? IsArray.isArray(fieldReference) : IsArray.isArray(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and retrieves its length.
			 *
			 * @return
			 */
			public Size length() {
				return usesFieldRef() ? Size.lengthOfArray(fieldReference) : Size.lengthOfArray(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that takes the associated array and selects a subset from it.
			 *
			 * @return
			 */
			public Slice slice() {
				return usesFieldRef() ? Slice.sliceArrayOf(fieldReference) : Slice.sliceArrayOf(expression);
			}

			private boolean usesFieldRef() {
				return fieldReference != null;
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
			 * Creates new {@link AggregationExpressions} that returns the associated value without parsing.
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
		 * Take the date referenced by given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DateOperatorFactory dateOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new DateOperatorFactory(fieldReference);
		}

		/**
		 * Take the date resulting from the given {@literal expression}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DateOperatorFactory dateOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new DateOperatorFactory(expression);
		}

		public static class DateOperatorFactory {

			private final String fieldReference;
			private final AggregationExpression expression;

			public DateOperatorFactory(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				this.fieldReference = fieldReference;
				this.expression = null;
			}

			public DateOperatorFactory(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				this.fieldReference = null;
				this.expression = expression;
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the day of the year for a date as a number between 1
			 * and 366.
			 *
			 * @return
			 */
			public DayOfYear dayOfYear() {
				return usesFieldRef() ? DayOfYear.dayOfYear(fieldReference) : DayOfYear.dayOfYear(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the day of the month for a date as a number between 1
			 * and 31.
			 *
			 * @return
			 */
			public DayOfMonth dayOfMonth() {
				return usesFieldRef() ? DayOfMonth.dayOfMonth(fieldReference) : DayOfMonth.dayOfMonth(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the day of the week for a date as a number between 1
			 * (Sunday) and 7 (Saturday).
			 *
			 * @return
			 */
			public DayOfWeek dayOfWeek() {
				return usesFieldRef() ? DayOfWeek.dayOfWeek(fieldReference) : DayOfWeek.dayOfWeek(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the year portion of a date.
			 *
			 * @return
			 */
			public Year year() {
				return usesFieldRef() ? Year.yearOf(fieldReference) : Year.yearOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the month of a date as a number between 1 and 12.
			 *
			 * @return
			 */
			public Month month() {
				return usesFieldRef() ? Month.monthOf(fieldReference) : Month.monthOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the week of the year for a date as a number between 0
			 * and 53.
			 *
			 * @return
			 */
			public Week week() {
				return usesFieldRef() ? Week.weekOf(fieldReference) : Week.weekOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the hour portion of a date as a number between 0 and
			 * 23.
			 *
			 * @return
			 */
			public Hour hour() {
				return usesFieldRef() ? Hour.hourOf(fieldReference) : Hour.hourOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the minute portion of a date as a number between 0 and
			 * 59.
			 *
			 * @return
			 */
			public Minute minute() {
				return usesFieldRef() ? Minute.minuteOf(fieldReference) : Minute.minuteOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the second portion of a date as a number between 0 and
			 * 59, but can be 60 to account for leap seconds.
			 *
			 * @return
			 */
			public Second second() {
				return usesFieldRef() ? Second.secondOf(fieldReference) : Second.secondOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that returns the millisecond portion of a date as an integer between
			 * 0 and 999.
			 *
			 * @return
			 */
			public Millisecond millisecond() {
				return usesFieldRef() ? Millisecond.millisecondOf(fieldReference) : Millisecond.millisecondOf(expression);
			}

			/**
			 * Creates new {@link AggregationExpressions} that converts a date object to a string according to a
			 * user-specified {@literal format}.
			 *
			 * @param format must not be {@literal null}.
			 * @return
			 */
			public DateToString toString(String format) {
				return (usesFieldRef() ? DateToString.dateOf(fieldReference) : DateToString.dateOf(expression))
						.toString(format);
			}

			private boolean usesFieldRef() {
				return fieldReference != null;
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

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetEquals(asFields(arrayReference));
		}

		public SetEquals isEqualTo(String... arrayReferences) {

			Assert.notNull(arrayReferences, "ArrayReferences must not be null!");
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

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetIntersection(asFields(arrayReference));
		}

		public SetIntersection intersects(String... arrayReferences) {

			Assert.notNull(arrayReferences, "ArrayReferences must not be null!");
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

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetUnion(asFields(arrayReference));
		}

		public SetUnion union(String... arrayReferences) {

			Assert.notNull(arrayReferences, "ArrayReferences must not be null!");
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

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetDifference(asFields(arrayReference));
		}

		public SetDifference differenceTo(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
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

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetIsSubset(asFields(arrayReference));
		}

		public SetIsSubset isSubsetOf(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
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

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
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

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
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

		public static Abs absoluteValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Abs(Fields.field(fieldReference));
		}

		public static Abs absoluteValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Add valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Add(asFields(fieldReference));
		}

		public static Add valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Add(Collections.singletonList(expression));
		}

		public static Add valueOf(Number value) {
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
	class Ceil extends AbstractAggregationExpression {

		private Ceil(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$ceil";
		}

		public static Ceil ceilValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Ceil(Fields.field(fieldReference));
		}

		public static Ceil ceilValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Divide valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Divide(asFields(fieldReference));
		}

		public static Divide valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Divide(Collections.singletonList(expression));
		}

		public static Divide valueOf(Number value) {
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
	class Exp extends AbstractAggregationExpression {

		protected Exp(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$exp";
		}

		public static Exp expValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Exp(Fields.field(fieldReference));
		}

		public static Exp expValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Floor floorValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Floor(Fields.field(fieldReference));
		}

		public static Floor floorValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Ln lnValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Ln(Fields.field(fieldReference));
		}

		public static Ln lnValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Log valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Log(asFields(fieldReference));
		}

		public static Log valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Log(Collections.singletonList(expression));
		}

		public static Log valueOf(Number value) {
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
	class Log10 extends AbstractAggregationExpression {

		private Log10(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$log10";
		}

		public static Log10 log10ValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Log10(Fields.field(fieldReference));
		}

		public static Log10 log10ValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Mod valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Mod(asFields(fieldReference));
		}

		public static Mod valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Mod(Collections.singletonList(expression));
		}

		public static Mod valueOf(Number value) {
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
	class Multiply extends AbstractAggregationExpression {

		private Multiply(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$multiply";
		}

		public static Multiply valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Multiply(asFields(fieldReference));
		}

		public static Multiply valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Multiply(Collections.singletonList(expression));
		}

		public static Multiply valueOf(Number value) {
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
	class Pow extends AbstractAggregationExpression {

		private Pow(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$pow";
		}

		public static Pow valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Pow(asFields(fieldReference));
		}

		public static Pow valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Pow(Collections.singletonList(expression));
		}

		public static Pow valueOf(Number value) {
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
	class Sqrt extends AbstractAggregationExpression {

		private Sqrt(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$sqrt";
		}

		public static Sqrt sqrtOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Sqrt(Fields.field(fieldReference));
		}

		public static Sqrt sqrtOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Subtract valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Subtract(asFields(fieldReference));
		}

		public static Subtract valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Subtract(Collections.singletonList(expression));
		}

		public static Subtract valueOf(Number value) {
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
	class Trunc extends AbstractAggregationExpression {

		private Trunc(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$trunc";
		}

		public static Trunc truncValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Trunc(Fields.field(fieldReference));
		}

		public static Trunc truncValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Concat valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Concat(asFields(fieldReference));
		}

		public static Concat valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Concat(Collections.singletonList(expression));
		}

		public static Concat stringValue(String value) {
			return new Concat(Collections.singletonList(value));
		}

		public Concat concatValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Concat(append(Fields.field(fieldReference)));
		}

		public Concat concatValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Substr valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Substr(asFields(fieldReference));
		}

		public static Substr valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static ToLower lowerValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ToLower(Fields.field(fieldReference));
		}

		public static ToLower lowerValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static ToUpper upperValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ToUpper(Fields.field(fieldReference));
		}

		public static ToUpper upperValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static StrCaseCmp valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StrCaseCmp(asFields(fieldReference));
		}

		public static StrCaseCmp valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StrCaseCmp(Collections.singletonList(expression));
		}

		public static StrCaseCmp stringValue(String value) {
			return new StrCaseCmp(Collections.singletonList(value));
		}

		public StrCaseCmp strcasecmp(String value) {
			return new StrCaseCmp(append(value));
		}

		public StrCaseCmp strcasecmpValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StrCaseCmp(append(Fields.field(fieldReference)));
		}

		public StrCaseCmp strcasecmpValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static ArrayElemtAt arrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ArrayElemtAt(asFields(fieldReference));
		}

		public static ArrayElemtAt arrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ArrayElemtAt(Collections.singletonList(expression));
		}

		public ArrayElemtAt elementAt(int index) {
			return new ArrayElemtAt(append(index));
		}

		public ArrayElemtAt elementAt(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ArrayElemtAt(append(expression));
		}

		public ArrayElemtAt elementAt(String arrayFieldReference) {

			Assert.notNull(arrayFieldReference, "ArrayReference must not be null!");
			return new ArrayElemtAt(append(Fields.field(arrayFieldReference)));
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

		public static ConcatArrays arrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ConcatArrays(asFields(fieldReference));
		}

		public static ConcatArrays arrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ConcatArrays(Collections.singletonList(expression));
		}

		public ConcatArrays concat(String arrayFieldReference) {

			Assert.notNull(arrayFieldReference, "ArrayFieldReference must not be null!");
			return new ConcatArrays(append(Fields.field(arrayFieldReference)));
		}

		public ConcatArrays concat(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static IsArray isArray(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new IsArray(Fields.field(fieldReference));
		}

		public static IsArray isArray(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Size lengthOfArray(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Size(Fields.field(fieldReference));
		}

		public static Size lengthOfArray(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Slice sliceArrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Slice(asFields(fieldReference));
		}

		public static Slice sliceArrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static DayOfYear dayOfYear(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new DayOfYear(Fields.field(fieldReference));
		}

		public static DayOfYear dayOfYear(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static DayOfMonth dayOfMonth(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new DayOfMonth(Fields.field(fieldReference));
		}

		public static DayOfMonth dayOfMonth(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static DayOfWeek dayOfWeek(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new DayOfWeek(Fields.field(fieldReference));
		}

		public static DayOfWeek dayOfWeek(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Year yearOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Year(Fields.field(fieldReference));
		}

		public static Year yearOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Month monthOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Month(Fields.field(fieldReference));
		}

		public static Month monthOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Week weekOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Week(Fields.field(fieldReference));
		}

		public static Week weekOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Hour hourOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Hour(Fields.field(fieldReference));
		}

		public static Hour hourOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		public static Minute minuteOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Minute(Fields.field(fieldReference));
		}

		public static Minute minuteOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		/**
		 * Creates new {@link Second}.
		 * 
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Second secondOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Second(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Second}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Second secondOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Millisecond millisecondOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Millisecond(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Millisecond millisecondOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static FormatBuilder dateOf(final String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new FormatBuilder() {
				@Override
				public DateToString toString(String format) {

					Assert.notNull(format, "Format must not be null!");
					return new DateToString(argumentMap(Fields.field(fieldReference), format));
				}
			};
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static FormatBuilder dateOf(final AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new FormatBuilder() {
				@Override
				public DateToString toString(String format) {

					Assert.notNull(format, "Format must not be null!");

					return new DateToString(argumentMap(expression, format));
				}
			};
		}

		private static Map<String, Object> argumentMap(Object date, String format) {

			Map<String, Object> args = new LinkedHashMap<String, Object>(2);
			args.put("format", format);
			args.put("date", date);
			return args;
		}

		public interface FormatBuilder {

			/**
			 * Creates new {@link DateToString} with all previously added arguments appending the given one.
			 *
			 * @param format must not be {@literal null}.
			 * @return
			 */
			DateToString toString(String format);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $sum}.
	 *
	 * @author Christoph Strobl
	 */
	class Sum extends AbstractAggregationExpression {

		private Sum(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$sum";
		}

		/**
		 * Creates new {@link Sum}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Sum sumOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Sum(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Sum}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Sum sumOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Sum(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Sum} with all previously added arguments appending the given one. <strong>NOTE:</strong> Only
		 * possible in {@code $project} stage.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Sum and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Sum(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Sum} with all previously added arguments appending the given one. <strong>NOTE:</strong> Only
		 * possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Sum and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Sum(append(expression));
		}

		@Override
		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDbObject(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDbObject(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $avg}.
	 *
	 * @author Christoph Strobl
	 */
	class Avg extends AbstractAggregationExpression {

		private Avg(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$avg";
		}

		/**
		 * Creates new {@link Avg}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Avg avgOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Avg(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Avg}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Avg avgOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Avg(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Avg} with all previously added arguments appending the given one. <strong>NOTE:</strong> Only
		 * possible in {@code $project} stage.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Avg and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Avg(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Avg} with all previously added arguments appending the given one. <strong>NOTE:</strong> Only
		 * possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Avg and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Avg(append(expression));
		}

		@Override
		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDbObject(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDbObject(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $max}.
	 *
	 * @author Christoph Strobl
	 */
	class Max extends AbstractAggregationExpression {

		private Max(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$max";
		}

		/**
		 * Creates new {@link Max}.
		 * 
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Max maxOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Max(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Max}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Max maxOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Max(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Max} with all previously added arguments appending the given one. <strong>NOTE:</strong> Only
		 * possible in {@code $project} stage.
		 * 
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Max and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Max(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Max} with all previously added arguments appending the given one. <strong>NOTE:</strong> Only
		 * possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Max and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Max(append(expression));
		}

		@Override
		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDbObject(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDbObject(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $min}.
	 *
	 * @author Christoph Strobl
	 */
	class Min extends AbstractAggregationExpression {

		private Min(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$min";
		}

		/**
		 * Creates new {@link Min}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Min minOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Min(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Min}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Min minOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Min(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Min} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 * 
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Min and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Min(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Min} with all previously added arguments appending the given one. <strong>NOTE:</strong> Only
		 * possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Min and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Min(append(expression));
		}

		@Override
		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDbObject(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDbObject(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $stdDevPop}.
	 *
	 * @author Christoph Strobl
	 */
	class StdDevPop extends AbstractAggregationExpression {

		private StdDevPop(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$stdDevPop";
		}

		/**
		 * Creates new {@link StdDevPop}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static StdDevPop stdDevPopOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StdDevPop(asFields(fieldReference));
		}

		/**
		 * Creates new {@link StdDevPop} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static StdDevPop stdDevPopOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StdDevPop(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link StdDevPop} with all previously added arguments appending the given one. <br/>
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public StdDevPop and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StdDevPop(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link StdDevSamp} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public StdDevPop and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StdDevPop(append(expression));
		}

		@Override
		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDbObject(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDbObject(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $stdDevSamp}.
	 *
	 * @author Christoph Strobl
	 */
	class StdDevSamp extends AbstractAggregationExpression {

		private StdDevSamp(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$stdDevSamp";
		}

		/**
		 * Creates new {@link StdDevSamp}.
		 * 
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static StdDevSamp stdDevSampOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StdDevSamp(asFields(fieldReference));
		}

		/**
		 * Creates new {@link StdDevSamp}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static StdDevSamp stdDevSampOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StdDevSamp(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link StdDevSamp} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public StdDevSamp and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StdDevSamp(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link StdDevSamp} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public StdDevSamp and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StdDevSamp(append(expression));
		}

		@Override
		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDbObject(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDbObject(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $cmp}.
	 *
	 * @author Christoph Strobl
	 */
	class Cmp extends AbstractAggregationExpression {

		private Cmp(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$cmp";
		}

		/**
		 * Creates new {@link Cmp}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Cmp valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Cmp(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Cmp}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Cmp valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Cmp(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Cmp} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Cmp compareTo(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Cmp(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Cmp} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Cmp compareTo(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Cmp(append(expression));
		}

		/**
		 * Creates new {@link Cmp} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Cmp compareToValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Cmp(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $eq}.
	 *
	 * @author Christoph Strobl
	 */
	class Eq extends AbstractAggregationExpression {

		private Eq(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$eq";
		}

		/**
		 * Creates new {@link Eq}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Eq valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Eq(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Eq}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Eq valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Eq(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Eq} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Eq equalTo(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Eq(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Eq} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Eq equalTo(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Eq(append(expression));
		}

		/**
		 * Creates new {@link Eq} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Eq equalToValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Eq(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $gt}.
	 *
	 * @author Christoph Strobl
	 */
	class Gt extends AbstractAggregationExpression {

		private Gt(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$gt";
		}

		/**
		 * Creates new {@link Gt}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Gt valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Gt(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Gt}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Gt valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Gt(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Gt} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Gt greaterThan(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Gt(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Gt} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Gt greaterThan(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Gt(append(expression));
		}

		/**
		 * Creates new {@link Gt} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Gt greaterThanValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Gt(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $lt}.
	 *
	 * @author Christoph Strobl
	 */
	class Lt extends AbstractAggregationExpression {

		private Lt(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$lt";
		}

		/**
		 * Creates new {@link Lt}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Lt valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Lt(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Lt}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Lt valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Lt(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Lt} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Lt lessThan(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Lt(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Lt} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Lt lessThan(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Lt(append(expression));
		}

		/**
		 * Creates new {@link Lt} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Lt lessThanValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Lt(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $gte}.
	 *
	 * @author Christoph Strobl
	 */
	class Gte extends AbstractAggregationExpression {

		private Gte(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$gte";
		}

		/**
		 * Creates new {@link Gte}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Gte valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Gte(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Gte}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Gte valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Gte(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Gte} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Gte greaterThanEqualTo(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Gte(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Gte} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Gte greaterThanEqualTo(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Gte(append(expression));
		}

		/**
		 * Creates new {@link Gte} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Gte greaterThanEqualToValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Gte(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $lte}.
	 *
	 * @author Christoph Strobl
	 */
	class Lte extends AbstractAggregationExpression {

		private Lte(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$lte";
		}

		/**
		 * Creates new {@link Lte}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Lte valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Lte(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Lte}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Lte valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Lte(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Lte} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Lte lessThanEqualTo(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Lte(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Lte} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Lte lessThanEqualTo(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Lte(append(expression));
		}

		/**
		 * Creates new {@link Lte} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Lte lessThanEqualToValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Lte(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $ne}.
	 *
	 * @author Christoph Strobl
	 */
	class Ne extends AbstractAggregationExpression {

		private Ne(List<?> value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$ne";
		}

		/**
		 * Creates new {@link Ne}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Ne valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Ne(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Ne}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Ne valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Ne(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Ne} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Ne notEqualTo(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Ne(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Ne} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Ne notEqualTo(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Ne(append(expression));
		}

		/**
		 * Creates new {@link Eq} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Ne notEqualToValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Ne(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $and}.
	 *
	 * @author Christoph Strobl
	 */
	class And extends AbstractAggregationExpression {

		private And(List<?> values) {
			super(values);
		}

		@Override
		public String getMongoMethod() {
			return "$and";
		}

		/**
		 * Creates new {@link And} that evaluates one or more expressions and returns {@literal true} if all of the
		 * expressions are {@literal true}.
		 *
		 * @param expressions
		 * @return
		 */
		public static And and(Object... expressions) {
			return new And(Arrays.asList(expressions));
		}

		/**
		 * Creates new {@link And} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public And andExpression(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new And(append(expression));
		}

		/**
		 * Creates new {@link And} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public And andField(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new And(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link And} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public And andValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new And(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $or}.
	 *
	 * @author Christoph Strobl
	 */
	class Or extends AbstractAggregationExpression {

		private Or(List<?> values) {
			super(values);
		}

		@Override
		public String getMongoMethod() {
			return "$or";
		}

		/**
		 * Creates new {@link Or} that evaluates one or more expressions and returns {@literal true} if any of the
		 * expressions are {@literal true}.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 */
		public static Or or(Object... expressions) {

			Assert.notNull(expressions, "Expressions must not be null!");
			return new Or(Arrays.asList(expressions));
		}

		/**
		 * Creates new {@link Or} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Or orExpression(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Or(append(expression));
		}

		/**
		 * Creates new {@link Or} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Or orField(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Or(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Or} with all previously added arguments appending the given one.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Or orValue(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Or(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $not}.
	 *
	 * @author Christoph Strobl
	 */
	class Not extends AbstractAggregationExpression {

		private Not(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$not";
		}

		/**
		 * Creates new {@link Not} that evaluates the boolean value of the referenced field and returns the opposite boolean
		 * value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Not not(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Not(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Not} that evaluates the resulting boolean value of the given {@link AggregationExpression} and
		 * returns the opposite boolean value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Not not(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Not(Collections.singletonList(expression));
		}
	}

}
