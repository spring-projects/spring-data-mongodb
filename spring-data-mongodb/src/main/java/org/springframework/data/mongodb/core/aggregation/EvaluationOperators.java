/*
 * Copyright 2021-2023 the original author or authors.
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

import org.bson.Document;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal evaluation operators} such as {@literal $expr}.
 *
 * @author Divya Srivastava
 * @author Christoph Strobl
 * @since 3.3
 */
public class EvaluationOperators {

	/**
	 * Take the value resulting from the given fieldReference.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link EvaluationOperatorFactory}.
	 */
	public static EvaluationOperatorFactory valueOf(String fieldReference) {
		return new EvaluationOperatorFactory(fieldReference);
	}

	/**
	 * Take the value resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link EvaluationOperatorFactory}.
	 */
	public static EvaluationOperatorFactory valueOf(AggregationExpression expression) {
		return new EvaluationOperatorFactory(expression);
	}

	public static class EvaluationOperatorFactory {

		private final String fieldReference;
		private final AggregationExpression expression;

		/**
		 * Creates new {@link EvaluationOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public EvaluationOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link EvaluationOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public EvaluationOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpression} that is a valid aggregation expression.
		 *
		 * @return new instance of {@link Expr}.
		 */
		public Expr expr() {
			return usesFieldRef() ? Expr.valueOf(fieldReference) : Expr.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that is a valid aggregation expression.
		 *
		 * @return new instance of {@link Expr}.
		 */
		public LastObservationCarriedForward locf() {
			return usesFieldRef() ? LastObservationCarriedForward.locfValueOf(fieldReference)
					: LastObservationCarriedForward.locfValueOf(expression);
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

	/**
	 * Allows the use of aggregation expressions within the query language.
	 */
	public static class Expr extends AbstractAggregationExpression {

		private Expr(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$expr";
		}

		/**
		 * Creates new {@link Expr}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Expr}.
		 */
		public static Expr valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Expr(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Expr}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Expr}.
		 */
		public static Expr valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Expr(expression);
		}

		/**
		 * Creates {@code $expr} as {@link CriteriaDefinition}.
		 *
		 * @return the {@link CriteriaDefinition} from this expression.
		 */
		public CriteriaDefinition toCriteriaDefinition(AggregationOperationContext context) {

			Document criteriaObject = toDocument(context);

			return new CriteriaDefinition() {
				@Override
				public Document getCriteriaObject() {
					return criteriaObject;
				}

				@Override
				public String getKey() {
					return getMongoMethod();
				}
			};
		}
	}

	/**
	 * Sets {@literal null} and missing values to the last non-null value.
	 *
	 * @since 4.0
	 */
	public static class LastObservationCarriedForward extends AbstractAggregationExpression {

		private LastObservationCarriedForward(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$locf";
		}

		/**
		 * Creates new {@link LastObservationCarriedForward}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link LastObservationCarriedForward}.
		 */
		public static LastObservationCarriedForward locfValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new LastObservationCarriedForward(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link LastObservationCarriedForward}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link LastObservationCarriedForward}.
		 */
		public static LastObservationCarriedForward locfValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new LastObservationCarriedForward(expression);
		}
	}

}
