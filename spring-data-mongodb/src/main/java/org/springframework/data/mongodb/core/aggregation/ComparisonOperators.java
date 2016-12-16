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

import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;

/**
 * Gateway to {@literal comparison expressions}.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
public class ComparisonOperators {

	/**
	 * Take the field referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static ComparisonOperatorFactory valueOf(String fieldReference) {
		return new ComparisonOperatorFactory(fieldReference);
	}

	/**
	 * Take the value resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static ComparisonOperatorFactory valueOf(AggregationExpression expression) {
		return new ComparisonOperatorFactory(expression);
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
		 * Creates new {@link ComparisonOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public ComparisonOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Cmp compareTo(String fieldReference) {
			return createCmp().compareTo(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Cmp compareTo(AggregationExpression expression) {
			return createCmp().compareTo(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values.
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
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is equal to the value of the referenced field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Eq equalTo(String fieldReference) {
			return createEq().equalTo(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is equal to the expression result.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Eq equalTo(AggregationExpression expression) {
			return createEq().equalTo(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
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
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is greater than the value of the referenced field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Gt greaterThan(String fieldReference) {
			return createGt().greaterThan(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is greater than the expression result.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Gt greaterThan(AggregationExpression expression) {
			return createGt().greaterThan(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
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
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is greater than or equivalent to the value of the referenced field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Gte greaterThanEqualTo(String fieldReference) {
			return createGte().greaterThanEqualTo(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is greater than or equivalent to the expression result.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Gte greaterThanEqualTo(AggregationExpression expression) {
			return createGte().greaterThanEqualTo(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
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
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is less than the value of the referenced field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Lt lessThan(String fieldReference) {
			return createLt().lessThan(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is less than the expression result.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Lt lessThan(AggregationExpression expression) {
			return createLt().lessThan(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
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
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is less than or equivalent to the value of the referenced field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Lte lessThanEqualTo(String fieldReference) {
			return createLte().lessThanEqualTo(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
		 * value is less than or equivalent to the expression result.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Lte lessThanEqualTo(AggregationExpression expression) {
			return createLte().lessThanEqualTo(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the first
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
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the values
		 * are not equivalent.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Ne notEqualTo(String fieldReference) {
			return createNe().notEqualTo(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the values
		 * are not equivalent.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Ne notEqualTo(AggregationExpression expression) {
			return createNe().notEqualTo(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that compares two values and returns {@literal true} when the values
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

	/**
	 * {@link AggregationExpression} for {@code $cmp}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Cmp extends AbstractAggregationExpression {

		private Cmp(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
	public static class Eq extends AbstractAggregationExpression {

		private Eq(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
	public static class Gt extends AbstractAggregationExpression {

		private Gt(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
	public static class Lt extends AbstractAggregationExpression {

		private Lt(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
	public static class Gte extends AbstractAggregationExpression {

		private Gte(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
	public static class Lte extends AbstractAggregationExpression {

		private Lte(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
	public static class Ne extends AbstractAggregationExpression {

		private Ne(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
}
