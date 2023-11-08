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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.BucketOperation.BucketOperationOutputBuilder;
import org.springframework.util.Assert;

/**
 * Encapsulates the aggregation framework {@code $bucket}-operation. <br />
 * Bucket stage is typically used with {@link Aggregation} and {@code $facet}. Categorizes incoming documents into
 * groups, called buckets, based on a specified expression and bucket boundaries. <br />
 * We recommend to use the static factory method {@link Aggregation#bucket(String)} instead of creating instances of
 * this class directly.
 *
 * @see <a href="https://docs.mongodb.org/manual/reference/aggregation/bucket/">https://docs.mongodb.org/manual/reference/aggregation/bucket/</a>
 * @see BucketOperationSupport
 * @author Mark Paluch
 * @since 1.10
 */
public class BucketOperation extends BucketOperationSupport<BucketOperation, BucketOperationOutputBuilder>
		implements FieldsExposingAggregationOperation {

	private final List<Object> boundaries;
	private final Object defaultBucket;

	/**
	 * Creates a new {@link BucketOperation} given a {@link Field group-by field}.
	 *
	 * @param groupByField must not be {@literal null}.
	 */
	public BucketOperation(Field groupByField) {

		super(groupByField);

		this.boundaries = Collections.emptyList();
		this.defaultBucket = null;
	}

	/**
	 * Creates a new {@link BucketOperation} given a {@link AggregationExpression group-by expression}.
	 *
	 * @param groupByExpression must not be {@literal null}.
	 */
	public BucketOperation(AggregationExpression groupByExpression) {

		super(groupByExpression);

		this.boundaries = Collections.emptyList();
		this.defaultBucket = null;
	}

	private BucketOperation(BucketOperation bucketOperation, Outputs outputs) {

		super(bucketOperation, outputs);

		this.boundaries = bucketOperation.boundaries;
		this.defaultBucket = bucketOperation.defaultBucket;
	}

	private BucketOperation(BucketOperation bucketOperation, List<Object> boundaries, Object defaultBucket) {

		super(bucketOperation);

		this.boundaries = new ArrayList<>(boundaries);
		this.defaultBucket = defaultBucket;
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document options = new Document();

		options.put("boundaries", context.getMappedObject(new Document("$set", boundaries)).get("$set"));

		if (defaultBucket != null) {
			options.put("default", context.getMappedObject(new Document("$set", defaultBucket)).get("$set"));
		}

		options.putAll(super.toDocument(context));

		return new Document(getOperator(), options);
	}

	@Override
	public String getOperator() {
		return "$bucket";
	}

	/**
	 * Configures a default bucket {@literal literal} and return a new {@link BucketOperation}.
	 *
	 * @param literal must not be {@literal null}.
	 * @return new instance of {@link BucketOperation}.
	 */
	public BucketOperation withDefaultBucket(Object literal) {

		Assert.notNull(literal, "Default bucket literal must not be null");
		return new BucketOperation(this, boundaries, literal);
	}

	/**
	 * Configures {@literal boundaries} and return a new {@link BucketOperation}. Existing {@literal boundaries} are
	 * preserved and the new {@literal boundaries} are appended.
	 *
	 * @param boundaries must not be {@literal null}.
	 * @return new instance of {@link BucketOperation}.
	 */
	public BucketOperation withBoundaries(Object... boundaries) {

		Assert.notNull(boundaries, "Boundaries must not be null");
		Assert.noNullElements(boundaries, "Boundaries must not contain null values");

		List<Object> newBoundaries = new ArrayList<>(this.boundaries.size() + boundaries.length);
		newBoundaries.addAll(this.boundaries);
		newBoundaries.addAll(Arrays.asList(boundaries));

		return new BucketOperation(this, newBoundaries, defaultBucket);
	}

	@Override
	protected BucketOperation newBucketOperation(Outputs outputs) {
		return new BucketOperation(this, outputs);
	}

	@Override
	public ExpressionBucketOperationBuilder andOutputExpression(String expression, Object... params) {
		return new ExpressionBucketOperationBuilder(expression, this, params);
	}

	@Override
	public BucketOperationOutputBuilder andOutput(AggregationExpression expression) {
		return new BucketOperationOutputBuilder(expression, this);
	}

	@Override
	public BucketOperationOutputBuilder andOutput(String fieldName) {
		return new BucketOperationOutputBuilder(Fields.field(fieldName), this);
	}

	/**
	 * {@link OutputBuilder} implementation for {@link BucketOperation}.
	 */
	public static class BucketOperationOutputBuilder
			extends BucketOperationSupport.OutputBuilder<BucketOperationOutputBuilder, BucketOperation> {

		/**
		 * Creates a new {@link BucketOperationOutputBuilder} fot the given value and {@link BucketOperation}.
		 *
		 * @param value must not be {@literal null}.
		 * @param operation must not be {@literal null}.
		 */
		protected BucketOperationOutputBuilder(Object value, BucketOperation operation) {
			super(value, operation);
		}

		@Override
		protected BucketOperationOutputBuilder apply(OperationOutput operationOutput) {
			return new BucketOperationOutputBuilder(operationOutput, this.operation);
		}
	}

	/**
	 * {@link ExpressionBucketOperationBuilderSupport} implementation for {@link BucketOperation} using SpEL expression
	 * based {@link Output}.
	 *
	 * @author Mark Paluch
	 */
	public static class ExpressionBucketOperationBuilder
			extends ExpressionBucketOperationBuilderSupport<BucketOperationOutputBuilder, BucketOperation> {

		/**
		 * Creates a new {@link ExpressionBucketOperationBuilderSupport} for the given value, {@link BucketOperation} and
		 * parameters.
		 *
		 * @param expression must not be {@literal null}.
		 * @param operation must not be {@literal null}.
		 * @param parameters must not be {@literal null}.
		 */
		protected ExpressionBucketOperationBuilder(String expression, BucketOperation operation, Object[] parameters) {
			super(expression, operation, parameters);
		}

		@Override
		protected BucketOperationOutputBuilder apply(OperationOutput operationOutput) {
			return new BucketOperationOutputBuilder(operationOutput, this.operation);
		}
	}
}
