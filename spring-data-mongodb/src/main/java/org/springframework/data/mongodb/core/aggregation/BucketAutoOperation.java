/*
 * Copyright 2016 the original author or authors.
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

import org.springframework.data.mongodb.core.aggregation.BucketAutoOperation.BucketAutoOperationOutputBuilder;
import org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.OutputBuilder;
import org.springframework.util.Assert;

import org.bson.Document;

/**
 * Encapsulates the aggregation framework {@code $bucketAuto}-operation.
 * <p>
 * Bucket stage is typically used with {@link Aggregation} and {@code $facet}. Categorizes incoming documents into a
 * specific number of groups, called buckets, based on a specified expression. Bucket boundaries are automatically
 * determined in an attempt to evenly distribute the documents into the specified number of buckets.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#bucketAuto(String, int)} instead of creating instances of
 * this class directly.
 *
 * @see http://docs.mongodb.org/manual/reference/aggregation/bucketAuto/
 * @see BucketOperationSupport
 * @author Mark Paluch
 * @since 1.10
 */
public class BucketAutoOperation extends BucketOperationSupport<BucketAutoOperation, BucketAutoOperationOutputBuilder>
		implements FieldsExposingAggregationOperation {

	private final int buckets;
	private final String granularity;

	/**
	 * Creates a new {@link BucketAutoOperation} given a {@link Field group-by field}.
	 *
	 * @param groupByField must not be {@literal null}.
	 * @param buckets number of buckets, must be a positive integer.
	 */
	public BucketAutoOperation(Field groupByField, int buckets) {

		super(groupByField);

		Assert.isTrue(buckets > 0, "Number of buckets must be greater 0!");

		this.buckets = buckets;
		this.granularity = null;
	}

	/**
	 * Creates a new {@link BucketAutoOperation} given a {@link AggregationExpression group-by expression}.
	 *
	 * @param groupByExpression must not be {@literal null}.
	 * @param buckets number of buckets, must be a positive integer.
	 */
	public BucketAutoOperation(AggregationExpression groupByExpression, int buckets) {

		super(groupByExpression);

		Assert.isTrue(buckets > 0, "Number of buckets must be greater 0!");

		this.buckets = buckets;
		this.granularity = null;
	}

	private BucketAutoOperation(BucketAutoOperation bucketOperation, Outputs outputs) {

		super(bucketOperation, outputs);

		this.buckets = bucketOperation.buckets;
		this.granularity = bucketOperation.granularity;
	}

	private BucketAutoOperation(BucketAutoOperation bucketOperation, int buckets, String granularity) {

		super(bucketOperation);

		this.buckets = buckets;
		this.granularity = granularity;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document options = new Document();

		options.put("buckets", buckets);

		if (granularity != null) {
			options.put("granularity", granularity);
		}

		options.putAll(super.toDocument(context));

		return new Document("$bucketAuto", options);
	}

	/**
	 * Configures a number of bucket {@literal buckets} and return a new {@link BucketAutoOperation}.
	 *
	 * @param buckets must be a positive number.
	 * @return
	 */
	public BucketAutoOperation withBuckets(int buckets) {

		Assert.isTrue(buckets > 0, "Number of buckets must be greater 0!");
		return new BucketAutoOperation(this, buckets, granularity);
	}

	/**
	 * Configures {@literal granularity} that specifies the preferred number series to use to ensure that the calculated
	 * boundary edges end on preferred round numbers or their powers of 10 and return a new {@link BucketAutoOperation}.
	 *
	 * @param granularity must not be {@literal null}.
	 * @return
	 */
	public BucketAutoOperation withGranularity(Granularity granularity) {

		Assert.notNull(granularity, "Granularity must not be null!");

		return new BucketAutoOperation(this, buckets, granularity.toMongoGranularity());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport#newBucketOperation(org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.Outputs)
	 */
	@Override
	protected BucketAutoOperation newBucketOperation(Outputs outputs) {
		return new BucketAutoOperation(this, outputs);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport#andOutputExpression(java.lang.String, java.lang.Object[])
	 */
	@Override
	public ExpressionBucketAutoOperationBuilder andOutputExpression(String expression, Object... params) {
		return new ExpressionBucketAutoOperationBuilder(expression, this, params);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport#andOutput(org.springframework.data.mongodb.core.aggregation.AggregationExpression)
	 */
	@Override
	public BucketAutoOperationOutputBuilder andOutput(AggregationExpression expression) {
		return new BucketAutoOperationOutputBuilder(expression, this);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport#andOutput(java.lang.String)
	 */
	@Override
	public BucketAutoOperationOutputBuilder andOutput(String fieldName) {
		return new BucketAutoOperationOutputBuilder(Fields.field(fieldName), this);
	}

	/**
	 * {@link OutputBuilder} implementation for {@link BucketAutoOperation}.
	 */
	public static class BucketAutoOperationOutputBuilder
			extends OutputBuilder<BucketAutoOperationOutputBuilder, BucketAutoOperation> {

		/**
		 * Creates a new {@link BucketAutoOperationOutputBuilder} fot the given value and {@link BucketAutoOperation}.
		 *
		 * @param value must not be {@literal null}.
		 * @param operation must not be {@literal null}.
		 */
		protected BucketAutoOperationOutputBuilder(Object value, BucketAutoOperation operation) {
			super(value, operation);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.OutputBuilder#apply(org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.OperationOutput)
		 */
		@Override
		protected BucketAutoOperationOutputBuilder apply(OperationOutput operationOutput) {
			return new BucketAutoOperationOutputBuilder(operationOutput, this.operation);
		}
	}

	/**
	 * {@link ExpressionBucketOperationBuilderSupport} implementation for {@link BucketAutoOperation} using SpEL
	 * expression based {@link Output}.
	 * 
	 * @author Mark Paluch
	 */
	public static class ExpressionBucketAutoOperationBuilder
			extends ExpressionBucketOperationBuilderSupport<BucketAutoOperationOutputBuilder, BucketAutoOperation> {

		/**
		 * Creates a new {@link ExpressionBucketAutoOperationBuilder} for the given value, {@link BucketAutoOperation} and
		 * parameters.
		 *
		 * @param expression must not be {@literal null}.
		 * @param operation must not be {@literal null}.
		 * @param parameters
		 */
		protected ExpressionBucketAutoOperationBuilder(String expression, BucketAutoOperation operation,
				Object[] parameters) {
			super(expression, operation, parameters);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.OutputBuilder#apply(org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.OperationOutput)
		 */
		@Override
		protected BucketAutoOperationOutputBuilder apply(OperationOutput operationOutput) {
			return new BucketAutoOperationOutputBuilder(operationOutput, this.operation);
		}
	}

	/**
	 * @author Mark Paluch
	 */
	public static interface Granularity {

		/**
		 * @return a String that represents a MongoDB granularity to be used with {@link BucketAutoOperation}.
		 */
		String toMongoGranularity();
	}

	/**
	 * Supported MongoDB granularities.
	 *
	 * @see https://en.wikipedia.org/wiki/Preferred_number
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/#granularity
	 * @author Mark Paluch
	 */
	public enum Granularities implements Granularity {

		R5, R10, R20, R40, R80, //

		SERIES_1_2_5("1-2-5"), //

		E6, E12, E24, E48, E96, E192, //

		POWERSOF2;

		final String granularity;

		Granularities() {
			this.granularity = name();
		}

		Granularities(String granularity) {
			this.granularity = granularity;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.GranularitytoMongoGranularity()
		 */
		@Override
		public String toMongoGranularity() {
			return granularity;
		}
	}
}
