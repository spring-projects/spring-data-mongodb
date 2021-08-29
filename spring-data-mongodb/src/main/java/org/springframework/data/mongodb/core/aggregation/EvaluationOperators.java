package org.springframework.data.mongodb.core.aggregation;

import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators.Abs;
import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators.ArithmeticOperatorFactory;
import org.springframework.util.Assert;

public class EvaluationOperators {
	
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

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		
		/**
		 * Creates new {@link EvaluationOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public EvaluationOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

				Assert.notNull(fieldReference, "FieldReference must not be null!");
				return new Expr(Fields.field(fieldReference));
			}

			/**
			 * Creates new {@link Expr}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return new instance of {@link Expr}.
			 */
			public static Expr valueOf(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return new Expr(expression);
			}

		}
		
		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

}
