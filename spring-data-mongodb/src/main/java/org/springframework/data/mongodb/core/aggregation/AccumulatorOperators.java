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

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal accumulator} aggregation operations.
 *
 * @author Christoph Strobl
 * @since 1.10
 * @soundtrack Rage Against The Machine - Killing In The Name
 */
public class AccumulatorOperators {

	/**
	 * Take the numeric value referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static AccumulatorOperatorFactory valueOf(String fieldReference) {
		return new AccumulatorOperatorFactory(fieldReference);
	}

	/**
	 * Take the numeric value referenced resulting from given {@link AggregationExpression}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static AccumulatorOperatorFactory valueOf(AggregationExpression expression) {
		return new AccumulatorOperatorFactory(expression);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class AccumulatorOperatorFactory {

		private final String fieldReference;
		private final AggregationExpression expression;

		/**
		 * Creates new {@link AccumulatorOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public AccumulatorOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link ArrayOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public AccumulatorOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes the associated numeric value expression and calculates and
		 * returns the sum.
		 *
		 * @return
		 */
		public Sum sum() {
			return usesFieldRef() ? Sum.sumOf(fieldReference) : Sum.sumOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes the associated numeric value expression and returns the
		 * average value.
		 *
		 * @return
		 */
		public Avg avg() {
			return usesFieldRef() ? Avg.avgOf(fieldReference) : Avg.avgOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes the associated numeric value expression and returns the
		 * maximum value.
		 *
		 * @return
		 */
		public Max max() {
			return usesFieldRef() ? Max.maxOf(fieldReference) : Max.maxOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes the associated numeric value expression and returns the
		 * minimum value.
		 *
		 * @return
		 */
		public Min min() {
			return usesFieldRef() ? Min.minOf(fieldReference) : Min.minOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes the associated numeric value expression and calculates the
		 * population standard deviation of the input values.
		 *
		 * @return
		 */
		public StdDevPop stdDevPop() {
			return usesFieldRef() ? StdDevPop.stdDevPopOf(fieldReference) : StdDevPop.stdDevPopOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes the associated numeric value expression and calculates the
		 * sample standard deviation of the input values.
		 *
		 * @return
		 */
		public StdDevSamp stdDevSamp() {
			return usesFieldRef() ? StdDevSamp.stdDevSampOf(fieldReference) : StdDevSamp.stdDevSampOf(expression);
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $sum}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Sum extends AbstractAggregationExpression {

		private Sum(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
		 * Creates new {@link Sum} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Sum and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Sum(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Sum} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Sum and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Sum(append(expression));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.AbstractAggregationExpression#toDocument(java.lang.Object, org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDocument(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDocument(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $avg}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Avg extends AbstractAggregationExpression {

		private Avg(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
		 * Creates new {@link Avg} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Avg and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Avg(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Avg} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Avg and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Avg(append(expression));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.AbstractAggregationExpression#toDocument(java.lang.Object, org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDocument(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDocument(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $max}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Max extends AbstractAggregationExpression {

		private Max(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
		 * Creates new {@link Max} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Max and(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Max(append(Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Max} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Max and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Max(append(expression));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.AbstractAggregationExpression#toDocument(java.lang.Object, org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDocument(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDocument(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $min}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Min extends AbstractAggregationExpression {

		private Min(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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
		 * Creates new {@link Min} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Min and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Min(append(expression));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.AbstractAggregationExpression#toDocument(java.lang.Object, org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDocument(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDocument(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $stdDevPop}.
	 *
	 * @author Christoph Strobl
	 */
	public static class StdDevPop extends AbstractAggregationExpression {

		private StdDevPop(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.AbstractAggregationExpression#toDocument(java.lang.Object, org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDocument(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDocument(value, context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $stdDevSamp}.
	 *
	 * @author Christoph Strobl
	 */
	public static class StdDevSamp extends AbstractAggregationExpression {

		private StdDevSamp(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
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

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.AbstractAggregationExpression#toDocument(java.lang.Object, org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(Object value, AggregationOperationContext context) {

			if (value instanceof List) {
				if (((List) value).size() == 1) {
					return super.toDocument(((List<Object>) value).iterator().next(), context);
				}
			}

			return super.toDocument(value, context);
		}
	}
}
