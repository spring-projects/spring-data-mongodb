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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;

/**
 * Gateway to {@literal boolean expressions} that evaluate their argument expressions as booleans and return a boolean
 * as the result.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
public class BooleanOperators {

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
	 * Take the value resulting of the given {@link AggregationExpression}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static BooleanOperatorFactory valueOf(AggregationExpression fieldReference) {
		return new BooleanOperatorFactory(fieldReference);
	}

	/**
	 * Creates new {@link AggregationExpressions} that evaluates the boolean value of the referenced field and returns the
	 * opposite boolean value.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static Not not(String fieldReference) {
		return Not.not(fieldReference);
	}

	/**
	 * Creates new {@link AggregationExpressions} that evaluates the boolean value of {@link AggregationExpression} result
	 * and returns the opposite boolean value.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static Not not(AggregationExpression expression) {
		return Not.not(expression);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class BooleanOperatorFactory {

		private final String fieldReference;
		private final AggregationExpression expression;

		/**
		 * Creates new {@link BooleanOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public BooleanOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link BooleanOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public BooleanOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpressions} that evaluates one or more expressions and returns {@literal true} if
		 * all of the expressions are {@literal true}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public And and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createAnd().andExpression(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that evaluates one or more expressions and returns {@literal true} if
		 * all of the expressions are {@literal true}.
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
		 * Creates new {@link AggregationExpressions} that evaluates one or more expressions and returns {@literal true} if
		 * any of the expressions are {@literal true}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Or or(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createOr().orExpression(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that evaluates one or more expressions and returns {@literal true} if
		 * any of the expressions are {@literal true}.
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

	/**
	 * {@link AggregationExpression} for {@code $and}.
	 *
	 * @author Christoph Strobl
	 */
	public static class And extends AbstractAggregationExpression {

		private And(List<?> values) {
			super(values);
		}

		@Override
		protected String getMongoMethod() {
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
	public static class Or extends AbstractAggregationExpression {

		private Or(List<?> values) {
			super(values);
		}

		@Override
		protected String getMongoMethod() {
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
	public static class Not extends AbstractAggregationExpression {

		private Not(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
