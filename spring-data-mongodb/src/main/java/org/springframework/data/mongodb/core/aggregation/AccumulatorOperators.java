/*
 * Copyright 2016-2021 the original author or authors.
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
	 * @return new instance of {@link AccumulatorOperatorFactory}.
	 */
	public static AccumulatorOperatorFactory valueOf(String fieldReference) {
		return new AccumulatorOperatorFactory(fieldReference);
	}

	/**
	 * Take the numeric value referenced resulting from given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link AccumulatorOperatorFactory}.
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
		 * Creates new {@link AccumulatorOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public AccumulatorOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated numeric value expression and calculates and
		 * returns the sum.
		 *
		 * @return new instance of {@link Sum}.
		 */
		public Sum sum() {
			return usesFieldRef() ? Sum.sumOf(fieldReference) : Sum.sumOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated numeric value expression and returns the
		 * average value.
		 *
		 * @return new instance of {@link Avg}.
		 */
		public Avg avg() {
			return usesFieldRef() ? Avg.avgOf(fieldReference) : Avg.avgOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated numeric value expression and returns the
		 * maximum value.
		 *
		 * @return new instance of {@link Max}.
		 */
		public Max max() {
			return usesFieldRef() ? Max.maxOf(fieldReference) : Max.maxOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated numeric value expression and returns the
		 * minimum value.
		 *
		 * @return new instance of {@link Min}.
		 */
		public Min min() {
			return usesFieldRef() ? Min.minOf(fieldReference) : Min.minOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated numeric value expression and calculates the
		 * population standard deviation of the input values.
		 *
		 * @return new instance of {@link StdDevPop}.
		 */
		public StdDevPop stdDevPop() {
			return usesFieldRef() ? StdDevPop.stdDevPopOf(fieldReference) : StdDevPop.stdDevPopOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated numeric value expression and calculates the
		 * sample standard deviation of the input values.
		 *
		 * @return new instance of {@link StdDevSamp}.
		 */
		public StdDevSamp stdDevSamp() {
			return usesFieldRef() ? StdDevSamp.stdDevSampOf(fieldReference) : StdDevSamp.stdDevSampOf(expression);
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
		 * Creates new {@link ExpMovingAvgBuilder} that to build {@link AggregationExpression expMovingAvg} that calculates
		 * the exponential moving average of numeric values
		 *
		 * @return new instance of {@link ExpMovingAvg}.
		 * @since 3.3
		 */
		public ExpMovingAvgBuilder expMovingAvg() {

			ExpMovingAvg expMovingAvg = usesFieldRef() ? ExpMovingAvg.expMovingAvgOf(fieldReference)
					: ExpMovingAvg.expMovingAvgOf(expression);
			return new ExpMovingAvgBuilder() {

				@Override
				public ExpMovingAvg historicalDocuments(int numberOfHistoricalDocuments) {
					return expMovingAvg.n(numberOfHistoricalDocuments);
				}

				@Override
				public ExpMovingAvg alpha(double exponentialDecayValue) {
					return expMovingAvg.alpha(exponentialDecayValue);
				}
			};
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

	/**
	 * Builder for {@link ExpMovingAvg}.
	 *
	 * @since 3.3
	 */
	public interface ExpMovingAvgBuilder {

		/**
		 * Define the number of historical documents with significant mathematical weight.
		 *
		 * @param numberOfHistoricalDocuments
		 * @return new instance of {@link ExpMovingAvg}.
		 */
		ExpMovingAvg historicalDocuments(int numberOfHistoricalDocuments);

		/**
		 * Define the exponential decay value.
		 *
		 * @param exponentialDecayValue
		 * @return new instance of {@link ExpMovingAvg}.
		 */
		ExpMovingAvg alpha(double exponentialDecayValue);

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
		 * @return new instance of {@link Sum}.
		 */
		public static Sum sumOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Sum(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Sum}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Sum}.
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
		 * @return new instance of {@link Sum}.
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
		 * @return new instance of {@link Sum}.
		 */
		public Sum and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Sum(append(expression));
		}

		/**
		 * Creates new {@link Sum} with all previously added arguments appending the given one. <br />
		 * <strong>NOTE:</strong> Only possible in {@code $project} stage.
		 *
		 * @param value the value to add.
		 * @return new instance of {@link Sum}.
		 * @since 2.2
		 */
		public Sum and(Number value) {

			Assert.notNull(value, "Value must not be null!");
			return new Sum(append(value));
		}

		@Override
		@SuppressWarnings("unchecked")
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
		 * @return new instance of {@link Avg}.
		 */
		public static Avg avgOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Avg(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Avg}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Avg}.
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
		 * @return new instance of {@link Avg}.
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
		 * @return new instance of {@link Avg}.
		 */
		public Avg and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Avg(append(expression));
		}

		@Override
		@SuppressWarnings("unchecked")
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
		 * @return new instance of {@link Max}.
		 */
		public static Max maxOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Max(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Max}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Max}.
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
		 * @return new instance of {@link Max}.
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
		 * @return new instance of {@link Max}.
		 */
		public Max and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Max(append(expression));
		}

		@Override
		@SuppressWarnings("unchecked")
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
		 * @return new instance of {@link Min}.
		 */
		public static Min minOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Min(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Min}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Min}.
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
		 * @return new instance of {@link Min}.
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
		 * @return new instance of {@link Min}.
		 */
		public Min and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Min(append(expression));
		}

		@Override
		@SuppressWarnings("unchecked")
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
		 * @return new instance of {@link StdDevPop}.
		 */
		public static StdDevPop stdDevPopOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StdDevPop(asFields(fieldReference));
		}

		/**
		 * Creates new {@link StdDevPop} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link StdDevPop}.
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
		 * @return new instance of {@link StdDevPop}.
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
		 * @return new instance of {@link StdDevPop}.
		 */
		public StdDevPop and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StdDevPop(append(expression));
		}

		@Override
		@SuppressWarnings("unchecked")
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
		 * @return new instance of {@link StdDevSamp}.
		 */
		public static StdDevSamp stdDevSampOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StdDevSamp(asFields(fieldReference));
		}

		/**
		 * Creates new {@link StdDevSamp}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link StdDevSamp}.
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
		 * @return new instance of {@link StdDevSamp}.
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
		 * @return new instance of {@link StdDevSamp}.
		 */
		public StdDevSamp and(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StdDevSamp(append(expression));
		}

		@Override
		@SuppressWarnings("unchecked")
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
	 * {@link AggregationExpression} for {@code $covariancePop}.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class CovariancePop extends AbstractAggregationExpression {

		private CovariancePop(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link CovariancePop}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link CovariancePop}.
		 */
		public static CovariancePop covariancePopOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new CovariancePop(asFields(fieldReference));
		}

		/**
		 * Creates new {@link CovariancePop}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link CovariancePop}.
		 */
		public static CovariancePop covariancePopOf(AggregationExpression expression) {
			return new CovariancePop(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link CovariancePop} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link CovariancePop}.
		 */
		public CovariancePop and(String fieldReference) {
			return new CovariancePop(append(asFields(fieldReference)));
		}

		/**
		 * Creates new {@link CovariancePop} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link CovariancePop}.
		 */
		public CovariancePop and(AggregationExpression expression) {
			return new CovariancePop(append(expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$covariancePop";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $covarianceSamp}.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class CovarianceSamp extends AbstractAggregationExpression {

		private CovarianceSamp(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link CovarianceSamp}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link CovarianceSamp}.
		 */
		public static CovarianceSamp covarianceSampOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new CovarianceSamp(asFields(fieldReference));
		}

		/**
		 * Creates new {@link CovarianceSamp}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link CovarianceSamp}.
		 */
		public static CovarianceSamp covarianceSampOf(AggregationExpression expression) {
			return new CovarianceSamp(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link CovarianceSamp} with all previously added arguments appending the given one.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link CovarianceSamp}.
		 */
		public CovarianceSamp and(String fieldReference) {
			return new CovarianceSamp(append(asFields(fieldReference)));
		}

		/**
		 * Creates new {@link CovarianceSamp} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link CovarianceSamp}.
		 */
		public CovarianceSamp and(AggregationExpression expression) {
			return new CovarianceSamp(append(expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$covarianceSamp";
		}
	}

	/**
	 * {@link ExpMovingAvg} calculates the exponential moving average of numeric values.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class ExpMovingAvg extends AbstractAggregationExpression {

		private ExpMovingAvg(Object value) {
			super(value);
		}

		/**
		 * Create a new {@link ExpMovingAvg} by defining the field holding the value to be used as input.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ExpMovingAvg}.
		 */
		public static ExpMovingAvg expMovingAvgOf(String fieldReference) {
			return new ExpMovingAvg(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Create a new {@link ExpMovingAvg} by defining the {@link AggregationExpression expression} to compute the value
		 * to be used as input.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ExpMovingAvg}.
		 */
		public static ExpMovingAvg expMovingAvgOf(AggregationExpression expression) {
			return new ExpMovingAvg(Collections.singletonMap("input", expression));
		}

		/**
		 * Define the number of historical documents with significant mathematical weight. <br />
		 * Specify either {@link #n(int) N} or {@link #alpha(double) aplha}. Not both!
		 *
		 * @param numberOfHistoricalDocuments
		 * @return new instance of {@link ExpMovingAvg}.
		 */
		public ExpMovingAvg n/*umber of historical documents*/(int numberOfHistoricalDocuments) {
			return new ExpMovingAvg(append("N", numberOfHistoricalDocuments));
		}

		/**
		 * Define the exponential decay value. <br />
		 * Specify either {@link #alpha(double) aplha} or {@link #n(int) N}. Not both!
		 *
		 * @param exponentialDecayValue
		 * @return new instance of {@link ExpMovingAvg}.
		 */
		public ExpMovingAvg alpha(double exponentialDecayValue) {
			return new ExpMovingAvg(append("alpha", exponentialDecayValue));
		}

		@Override
		protected String getMongoMethod() {
			return "$expMovingAvg";
		}
	}
}
