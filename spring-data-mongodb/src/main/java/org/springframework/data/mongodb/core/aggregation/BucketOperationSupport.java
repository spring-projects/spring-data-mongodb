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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.OutputBuilder;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder;
import org.springframework.expression.spel.ast.Projection;
import org.springframework.util.Assert;

import org.bson.Document;

/**
 * Base class for bucket operations that support output expressions the aggregation framework. <br />
 * Bucket stages collect documents into buckets and can contribute output fields. <br />
 * Implementing classes are required to provide an {@link OutputBuilder}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.10
 */
public abstract class BucketOperationSupport<T extends BucketOperationSupport<T, B>, B extends OutputBuilder<B, T>>
		implements FieldsExposingAggregationOperation {

	private final Field groupByField;
	private final AggregationExpression groupByExpression;
	private final Outputs outputs;

	/**
	 * Creates a new {@link BucketOperationSupport} given a {@link Field group-by field}.
	 *
	 * @param groupByField must not be {@literal null}.
	 */
	protected BucketOperationSupport(Field groupByField) {

		Assert.notNull(groupByField, "Group by field must not be null!");

		this.groupByField = groupByField;
		this.groupByExpression = null;
		this.outputs = Outputs.EMPTY;
	}

	/**
	 * Creates a new {@link BucketOperationSupport} given a {@link AggregationExpression group-by expression}.
	 *
	 * @param groupByExpression must not be {@literal null}.
	 */
	protected BucketOperationSupport(AggregationExpression groupByExpression) {

		Assert.notNull(groupByExpression, "Group by AggregationExpression must not be null!");

		this.groupByExpression = groupByExpression;
		this.groupByField = null;
		this.outputs = Outputs.EMPTY;
	}

	/**
	 * Creates a copy of {@link BucketOperationSupport}.
	 *
	 * @param operationSupport must not be {@literal null}.
	 */
	protected BucketOperationSupport(BucketOperationSupport<?, ?> operationSupport) {
		this(operationSupport, operationSupport.outputs);
	}

	/**
	 * Creates a copy of {@link BucketOperationSupport} and applies the new {@link Outputs}.
	 *
	 * @param operationSupport must not be {@literal null}.
	 * @param outputs must not be {@literal null}.
	 */
	protected BucketOperationSupport(BucketOperationSupport<?, ?> operationSupport, Outputs outputs) {

		Assert.notNull(operationSupport, "BucketOperationSupport must not be null!");
		Assert.notNull(outputs, "Outputs must not be null!");

		this.groupByField = operationSupport.groupByField;
		this.groupByExpression = operationSupport.groupByExpression;
		this.outputs = outputs;
	}

	/**
	 * Creates a new {@link ExpressionBucketOperationBuilderSupport} given a SpEL {@literal expression} and optional
	 * {@literal params} to add an output field to the resulting bucket documents.
	 *
	 * @param expression the SpEL expression, must not be {@literal null} or empty.
	 * @param params must not be {@literal null}
	 * @return
	 */
	public abstract ExpressionBucketOperationBuilderSupport<B, T> andOutputExpression(String expression,
			Object... params);

	/**
	 * Creates a new {@link BucketOperationSupport} given an {@link AggregationExpression} to add an output field to the
	 * resulting bucket documents.
	 *
	 * @param expression the SpEL expression, must not be {@literal null} or empty.
	 * @return
	 */
	public abstract B andOutput(AggregationExpression expression);

	/**
	 * Creates a new {@link BucketOperationSupport} given {@literal fieldName} to add an output field to the resulting
	 * bucket documents. {@link BucketOperationSupport} exposes accumulation operations that can be applied to
	 * {@literal fieldName}.
	 *
	 * @param fieldName must not be {@literal null} or empty.
	 * @return
	 */
	public abstract B andOutput(String fieldName);

	/**
	 * Creates a new {@link BucketOperationSupport} given to add a count field to the resulting bucket documents.
	 *
	 * @return
	 */
	public B andOutputCount() {
		return andOutput(new AggregationExpression() {
			@Override
			public Document toDocument(AggregationOperationContext context) {
				return new Document("$sum", 1);
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document document = new Document();

		document.put("groupBy", groupByExpression == null ? context.getReference(groupByField).toString()
				: groupByExpression.toDocument(context));

		if (!outputs.isEmpty()) {
			document.put("output", outputs.toDocument(context));
		}

		return document;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation#getFields()
	 */
	@Override
	public ExposedFields getFields() {
		return outputs.asExposedFields();
	}

	/**
	 * Implementation hook to create a new bucket operation.
	 *
	 * @param outputs the outputs
	 * @return the new bucket operation.
	 */
	protected abstract T newBucketOperation(Outputs outputs);

	protected T andOutput(Output output) {
		return newBucketOperation(outputs.and(output));
	}

	/**
	 * Builder for SpEL expression-based {@link Output}.
	 *
	 * @author Mark Paluch
	 */
	public abstract static class ExpressionBucketOperationBuilderSupport<B extends OutputBuilder<B, T>, T extends BucketOperationSupport<T, B>>
			extends OutputBuilder<B, T> {

		/**
		 * Creates a new {@link ExpressionBucketOperationBuilderSupport} for the given value, {@link BucketOperationSupport}
		 * and parameters.
		 *
		 * @param expression must not be {@literal null}.
		 * @param operation must not be {@literal null}.
		 * @param parameters
		 */
		protected ExpressionBucketOperationBuilderSupport(String expression, T operation, Object[] parameters) {
			super(new SpelExpressionOutput(expression, parameters), operation);
		}
	}

	/**
	 * Base class for {@link Output} builders that result in a {@link BucketOperationSupport} providing the built
	 * {@link Output}.
	 *
	 * @author Mark Paluch
	 */
	public abstract static class OutputBuilder<B extends OutputBuilder<B, T>, T extends BucketOperationSupport<T, B>> {

		protected final Object value;
		protected final T operation;

		/**
		 * Creates a new {@link OutputBuilder} for the given value and {@link BucketOperationSupport}.
		 *
		 * @param value must not be {@literal null}.
		 * @param operation must not be {@literal null}.
		 */
		protected OutputBuilder(Object value, T operation) {

			Assert.notNull(value, "Value must not be null or empty!");
			Assert.notNull(operation, "ProjectionOperation must not be null!");

			this.value = value;
			this.operation = operation;
		}

		/**
		 * Generates a builder for a {@code $sum}-expression. <br />
		 * Count expressions are emulated via {@code $sum: 1}.
		 *
		 * @return
		 */
		public B count() {
			return sum(1);
		}

		/**
		 * Generates a builder for a {@code $sum}-expression for the current value.
		 *
		 * @return
		 */
		public B sum() {
			return apply(Accumulators.SUM);
		}

		/**
		 * Generates a builder for a {@code $sum}-expression for the given {@literal value}.
		 *
		 * @param value
		 * @return
		 */
		public B sum(Number value) {
			return apply(new OperationOutput(Accumulators.SUM.getMongoOperator(), Collections.singleton(value)));
		}

		/**
		 * Generates a builder for an {@code $last}-expression for the current value..
		 *
		 * @return
		 */
		public B last() {
			return apply(Accumulators.LAST);
		}

		/**
		 * Generates a builder for a {@code $first}-expression the current value.
		 *
		 * @return
		 */
		public B first() {
			return apply(Accumulators.FIRST);
		}

		/**
		 * Generates a builder for an {@code $avg}-expression for the current value.
		 *
		 * @param reference
		 * @return
		 */
		public B avg() {
			return apply(Accumulators.AVG);
		}

		/**
		 * Generates a builder for an {@code $min}-expression for the current value.
		 *
		 * @return
		 */
		public B min() {
			return apply(Accumulators.MIN);
		}

		/**
		 * Generates a builder for an {@code $max}-expression for the current value.
		 *
		 * @return
		 */
		public B max() {
			return apply(Accumulators.MAX);
		}

		/**
		 * Generates a builder for an {@code $push}-expression for the current value.
		 *
		 * @return
		 */
		public B push() {
			return apply(Accumulators.PUSH);
		}

		/**
		 * Generates a builder for an {@code $addToSet}-expression for the current value.
		 *
		 * @return
		 */
		public B addToSet() {
			return apply(Accumulators.ADDTOSET);
		}

		/**
		 * Apply an operator to the current value.
		 *
		 * @param operation the operation name, must not be {@literal null} or empty.
		 * @param values must not be {@literal null}.
		 * @return
		 */
		public B apply(String operation, Object... values) {

			Assert.hasText(operation, "Operation must not be empty or null!");
			Assert.notNull(value, "Values must not be null!");

			List<Object> objects = new ArrayList<Object>(values.length + 1);
			objects.add(value);
			objects.addAll(Arrays.asList(values));
			return apply(new OperationOutput(operation, objects));
		}

		/**
		 * Apply an {@link OperationOutput} to this output.
		 *
		 * @param operationOutput must not be {@literal null}.
		 * @return
		 */
		protected abstract B apply(OperationOutput operationOutput);

		private B apply(Accumulators operation) {
			return this.apply(operation.getMongoOperator());
		}

		/**
		 * Returns the finally to be applied {@link BucketOperation} with the given alias.
		 *
		 * @param alias will never be {@literal null} or empty.
		 * @return
		 */
		public T as(String alias) {

			if (value instanceof OperationOutput) {
				return this.operation.andOutput(((OperationOutput) this.value).withAlias(alias));
			}

			if (value instanceof Field) {
				throw new IllegalStateException("Cannot add a field as top-level output. Use accumulator expressions.");
			}

			return this.operation
					.andOutput(new AggregationExpressionOutput(Fields.field(alias), (AggregationExpression) value));
		}
	}

	private enum Accumulators {

		SUM("$sum"), AVG("$avg"), FIRST("$first"), LAST("$last"), MAX("$max"), MIN("$min"), PUSH("$push"), ADDTOSET(
				"$addToSet");

		private String mongoOperator;

		Accumulators(String mongoOperator) {
			this.mongoOperator = mongoOperator;
		}

		public String getMongoOperator() {
			return mongoOperator;
		}
	}

	/**
	 * Encapsulates {@link Output}s.
	 *
	 * @author Mark Paluch
	 */
	protected static class Outputs implements AggregationExpression {

		protected static final Outputs EMPTY = new Outputs();

		private List<Output> outputs;

		/**
		 * Creates a new, empty {@link Outputs}.
		 */
		private Outputs() {
			this.outputs = new ArrayList<Output>();
		}

		/**
		 * Creates new {@link Outputs} containing all given {@link Output}s.
		 *
		 * @param current
		 * @param output
		 */
		private Outputs(Collection<Output> current, Output output) {

			this.outputs = new ArrayList<Output>(current.size() + 1);
			this.outputs.addAll(current);
			this.outputs.add(output);
		}

		/**
		 * @return the {@link ExposedFields} derived from {@link Output}.
		 */
		protected ExposedFields asExposedFields() {

			// The count field is included by default when the output is not specified.
			if (isEmpty()) {
				return ExposedFields.from(new ExposedField("count", true));
			}

			ExposedFields fields = ExposedFields.from();

			for (Output output : outputs) {
				fields = fields.and(output.getExposedField());
			}

			return fields;
		}

		/**
		 * Create a new {@link Outputs} that contains the new {@link Output}.
		 *
		 * @param output must not be {@literal null}.
		 * @return the new {@link Outputs} that contains the new {@link Output}
		 */
		protected Outputs and(Output output) {

			Assert.notNull(output, "BucketOutput must not be null!");
			return new Outputs(this.outputs, output);
		}

		/**
		 * @return {@literal true} if {@link Outputs} contains no {@link Output}.
		 */
		protected boolean isEmpty() {
			return outputs.isEmpty();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(AggregationOperationContext context) {

			Document document = new Document();

			for (Output output : outputs) {
				document.put(output.getExposedField().getName(), output.toDocument(context));
			}

			return document;
		}

	}

	/**
	 * Encapsulates an output field in a bucket aggregation stage. <br />
	 * Output fields can be either top-level fields that define a valid field name or nested output fields using
	 * operators.
	 *
	 * @author Mark Paluch
	 */
	protected abstract static class Output implements AggregationExpression {

		private final ExposedField field;

		/**
		 * Creates new {@link Projection} for the given {@link Field}.
		 *
		 * @param field must not be {@literal null}.
		 */
		protected Output(Field field) {

			Assert.notNull(field, "Field must not be null!");
			this.field = new ExposedField(field, true);
		}

		/**
		 * Returns the field exposed by the {@link Output}.
		 *
		 * @return will never be {@literal null}.
		 */
		protected ExposedField getExposedField() {
			return field;
		}
	}

	/**
	 * Output field that uses a Mongo operation (expression object) to generate an output field value. <br />
	 * {@link OperationOutput} is used either with a regular field name or an operation keyword (e.g.
	 * {@literal $sum, $count}).
	 *
	 * @author Mark Paluch
	 */
	protected static class OperationOutput extends Output {

		private final String operation;
		private final List<Object> values;

		/**
		 * Creates a new {@link Output} for the given field.
		 *
		 * @param operation the actual operation key, must not be {@literal null} or empty.
		 * @param values the values to pass into the operation, must not be {@literal null}.
		 */
		public OperationOutput(String operation, Collection<? extends Object> values) {

			super(Fields.field(operation));

			Assert.hasText(operation, "Operation must not be null or empty!");
			Assert.notNull(values, "Values must not be null!");

			this.operation = operation;
			this.values = new ArrayList<Object>(values);
		}

		private OperationOutput(Field field, OperationOutput operationOutput) {

			super(field);

			this.operation = operationOutput.operation;
			this.values = operationOutput.values;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(AggregationOperationContext context) {

			List<Object> operationArguments = getOperationArguments(context);
			return new Document(operation,
					operationArguments.size() == 1 ? operationArguments.get(0) : operationArguments);
		}

		protected List<Object> getOperationArguments(AggregationOperationContext context) {

			List<Object> result = new ArrayList<Object>(values != null ? values.size() : 1);

			for (Object element : values) {

				if (element instanceof Field) {
					result.add(context.getReference((Field) element).toString());
				} else if (element instanceof Fields) {
					for (Field field : (Fields) element) {
						result.add(context.getReference(field).toString());
					}
				} else if (element instanceof AggregationExpression) {
					result.add(((AggregationExpression) element).toDocument(context));
				} else {
					result.add(element);
				}
			}

			return result;
		}

		/**
		 * Returns the field that holds the {@link ProjectionOperationBuilder.OperationProjection}.
		 *
		 * @return
		 */
		protected Field getField() {
			return getExposedField();
		}

		/**
		 * Creates a new instance of this {@link OperationOutput} with the given alias.
		 *
		 * @param alias the alias to set
		 * @return
		 */
		public OperationOutput withAlias(String alias) {

			final Field aliasedField = Fields.field(alias);
			return new OperationOutput(aliasedField, this) {

				@Override
				protected Field getField() {
					return aliasedField;
				}

				@Override
				protected List<Object> getOperationArguments(AggregationOperationContext context) {

					// We have to make sure that we use the arguments from the "previous" OperationOutput that we replace
					// with this new instance.
					return OperationOutput.this.getOperationArguments(context);
				}
			};
		}
	}

	/**
	 * A {@link Output} based on a SpEL expression.
	 */
	private static class SpelExpressionOutput extends Output {

		private static final SpelExpressionTransformer TRANSFORMER = new SpelExpressionTransformer();

		private final String expression;
		private final Object[] params;

		/**
		 * Creates a new {@link SpelExpressionOutput} for the given field, SpEL expression and parameters.
		 *
		 * @param expression must not be {@literal null} or empty.
		 * @param parameters must not be {@literal null}.
		 */
		public SpelExpressionOutput(String expression, Object[] parameters) {

			super(Fields.field(expression));

			Assert.hasText(expression, "Expression must not be null!");
			Assert.notNull(parameters, "Parameters must not be null!");

			this.expression = expression;
			this.params = parameters.clone();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.Output#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(AggregationOperationContext context) {
			return (Document) TRANSFORMER.transform(expression, context, params);
		}
	}

	/**
	 * @author Mark Paluch
	 */
	private static class AggregationExpressionOutput extends Output {

		private final AggregationExpression expression;

		/**
		 * Creates a new {@link AggregationExpressionOutput}.
		 *
		 * @param field
		 * @param expression
		 */
		protected AggregationExpressionOutput(Field field, AggregationExpression expression) {

			super(field);

			this.expression = expression;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport.Output#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public Document toDocument(AggregationOperationContext context) {
			return expression.toDocument(context);
		}
	}
}
