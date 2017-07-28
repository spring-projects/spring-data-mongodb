/*
 * Copyright 2013-2017 the original author or authors.
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

import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Cond;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.IfNull;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.Fields.AggregationField;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder.FieldProjection;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let.ExpressionVariable;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $project}-operation.
 * <p>
 * Projection of field to be used in an {@link Aggregation}. A projection is similar to a {@link Field}
 * inclusion/exclusion but more powerful. It can generate new fields, change values of given field etc.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#project(Fields)} instead of creating instances of
 * this class directly.
 * 
 * @author Tobias Trelle
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.3
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/project/">MongoDB Aggregation Framework: $project</a>
 */
public class ProjectionOperation implements FieldsExposingAggregationOperation {

	private static final List<Projection> NONE = Collections.emptyList();
	private static final String EXCLUSION_ERROR = "Exclusion of field %s not allowed. Projections by the mongodb "
			+ "aggregation framework only support the exclusion of the %s field!";

	private final List<Projection> projections;

	/**
	 * Creates a new empty {@link ProjectionOperation}.
	 */
	public ProjectionOperation() {
		this(NONE, NONE);
	}

	/**
	 * Creates a new {@link ProjectionOperation} including the given {@link Fields}.
	 * 
	 * @param fields must not be {@literal null}.
	 */
	public ProjectionOperation(Fields fields) {
		this(NONE, ProjectionOperationBuilder.FieldProjection.from(fields));
	}

	/**
	 * Copy constructor to allow building up {@link ProjectionOperation} instances from already existing
	 * {@link Projection}s.
	 * 
	 * @param current must not be {@literal null}.
	 * @param projections must not be {@literal null}.
	 */
	private ProjectionOperation(List<? extends Projection> current, List<? extends Projection> projections) {

		Assert.notNull(current, "Current projections must not be null!");
		Assert.notNull(projections, "Projections must not be null!");

		this.projections = new ArrayList<ProjectionOperation.Projection>(current.size() + projections.size());
		this.projections.addAll(current);
		this.projections.addAll(projections);
	}

	/**
	 * Creates a new {@link ProjectionOperation} with the current {@link Projection}s and the given one.
	 * 
	 * @param projection must not be {@literal null}.
	 * @return
	 */
	private ProjectionOperation and(Projection projection) {
		return new ProjectionOperation(this.projections, Arrays.asList(projection));
	}

	/**
	 * Creates a new {@link ProjectionOperation} with the current {@link Projection}s replacing the last current one with
	 * the given one.
	 * 
	 * @param projection must not be {@literal null}.
	 * @return
	 */
	private ProjectionOperation andReplaceLastOneWith(Projection projection) {

		List<Projection> projections = this.projections.isEmpty() ? Collections.<Projection> emptyList()
				: this.projections.subList(0, this.projections.size() - 1);
		return new ProjectionOperation(projections, Arrays.asList(projection));
	}

	/**
	 * Creates a new {@link ProjectionOperationBuilder} to define a projection for the field with the given name.
	 * 
	 * @param name must not be {@literal null} or empty.
	 * @return
	 */
	public ProjectionOperationBuilder and(String name) {
		return new ProjectionOperationBuilder(name, this, null);
	}

	public ExpressionProjectionOperationBuilder andExpression(String expression, Object... params) {
		return new ExpressionProjectionOperationBuilder(expression, this, params);
	}

	public ProjectionOperationBuilder and(AggregationExpression expression) {
		return new ProjectionOperationBuilder(expression, this, null);
	}

	/**
	 * Excludes the given fields from the projection.
	 * 
	 * @param fieldNames must not be {@literal null}.
	 * @return
	 */
	public ProjectionOperation andExclude(String... fieldNames) {

		for (String fieldName : fieldNames) {
			Assert.isTrue(Fields.UNDERSCORE_ID.equals(fieldName),
					String.format(EXCLUSION_ERROR, fieldName, Fields.UNDERSCORE_ID));
		}

		List<FieldProjection> excludeProjections = FieldProjection.from(Fields.fields(fieldNames), false);
		return new ProjectionOperation(this.projections, excludeProjections);
	}

	/**
	 * Includes the given fields into the projection.
	 * 
	 * @param fieldNames must not be {@literal null}.
	 * @return
	 */
	public ProjectionOperation andInclude(String... fieldNames) {

		List<FieldProjection> projections = FieldProjection.from(Fields.fields(fieldNames), true);
		return new ProjectionOperation(this.projections, projections);
	}

	/**
	 * Includes the given fields into the projection.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public ProjectionOperation andInclude(Fields fields) {
		return new ProjectionOperation(this.projections, FieldProjection.from(fields, true));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation#getFields()
	 */
	@Override
	public ExposedFields getFields() {

		ExposedFields fields = null;

		for (Projection projection : projections) {
			ExposedField field = projection.getExposedField();
			fields = fields == null ? ExposedFields.from(field) : fields.and(field);
		}

		return fields;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {

		BasicDBObject fieldObject = new BasicDBObject();

		for (Projection projection : projections) {
			fieldObject.putAll(projection.toDBObject(context));
		}

		return new BasicDBObject("$project", fieldObject);
	}

	/**
	 * Base class for {@link ProjectionOperationBuilder}s.
	 * 
	 * @author Thomas Darimont
	 */
	private static abstract class AbstractProjectionOperationBuilder implements AggregationOperation {

		protected final Object value;
		protected final ProjectionOperation operation;

		/**
		 * Creates a new {@link AbstractProjectionOperationBuilder} fot the given value and {@link ProjectionOperation}.
		 * 
		 * @param value must not be {@literal null}.
		 * @param operation must not be {@literal null}.
		 */
		public AbstractProjectionOperationBuilder(Object value, ProjectionOperation operation) {

			Assert.notNull(value, "value must not be null or empty!");
			Assert.notNull(operation, "ProjectionOperation must not be null!");

			this.value = value;
			this.operation = operation;
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDBObject(AggregationOperationContext context) {
			return this.operation.toDBObject(context);
		}

		/**
		 * Returns the finally to be applied {@link ProjectionOperation} with the given alias.
		 * 
		 * @param alias will never be {@literal null} or empty.
		 * @return
		 */
		public abstract ProjectionOperation as(String alias);

		/**
		 * Apply a conditional projection using {@link Cond}.
		 *
		 * @param cond must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public abstract ProjectionOperation applyCondition(Cond cond);

		/**
		 * Apply a conditional value replacement for {@literal null} values using {@link IfNull}.
		 *
		 * @param ifNull must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public abstract ProjectionOperation applyCondition(IfNull ifNull);
	}

	/**
	 * An {@link ProjectionOperationBuilder} that is used for SpEL expression based projections.
	 * 
	 * @author Thomas Darimont
	 */
	public static class ExpressionProjectionOperationBuilder extends ProjectionOperationBuilder {

		private final Object[] params;
		private final String expression;

		/**
		 * Creates a new {@link ExpressionProjectionOperationBuilder} for the given value, {@link ProjectionOperation} and
		 * parameters.
		 * 
		 * @param expression must not be {@literal null}.
		 * @param operation must not be {@literal null}.
		 * @param parameters
		 */
		public ExpressionProjectionOperationBuilder(String expression, ProjectionOperation operation, Object[] parameters) {

			super(expression, operation, null);
			this.expression = expression;
			this.params = parameters.clone();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder#project(java.lang.String, java.lang.Object[])
		 */
		@Override
		public ProjectionOperationBuilder project(String operation, final Object... values) {

			OperationProjection operationProjection = new OperationProjection(Fields.field(value.toString()), operation,
					values) {
				@Override
				protected List<Object> getOperationArguments(AggregationOperationContext context) {

					List<Object> result = new ArrayList<Object>(values.length + 1);
					result.add(ExpressionProjection.toMongoExpression(context,
							ExpressionProjectionOperationBuilder.this.expression, ExpressionProjectionOperationBuilder.this.params));
					result.addAll(Arrays.asList(values));

					return result;
				}
			};

			return new ProjectionOperationBuilder(value, this.operation.and(operationProjection), operationProjection);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.AbstractProjectionOperationBuilder#as(java.lang.String)
		 */
		@Override
		public ProjectionOperation as(String alias) {

			Field expressionField = Fields.field(alias, alias);
			return this.operation.and(new ExpressionProjection(expressionField, this.value.toString(), params));
		}

		/**
		 * A {@link Projection} based on a SpEL expression.
		 * 
		 * @author Thomas Darimont
		 * @author Oliver Gierke
		 */
		static class ExpressionProjection extends Projection {

			private static final SpelExpressionTransformer TRANSFORMER = new SpelExpressionTransformer();

			private final String expression;
			private final Object[] params;

			/**
			 * Creates a new {@link ExpressionProjection} for the given field, SpEL expression and parameters.
			 * 
			 * @param field must not be {@literal null}.
			 * @param expression must not be {@literal null} or empty.
			 * @param parameters must not be {@literal null}.
			 */
			public ExpressionProjection(Field field, String expression, Object[] parameters) {

				super(field);

				Assert.hasText(expression, "Expression must not be null!");
				Assert.notNull(parameters, "Parameters must not be null!");

				this.expression = expression;
				this.params = parameters.clone();
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
			 */
			@Override
			public DBObject toDBObject(AggregationOperationContext context) {
				return new BasicDBObject(getExposedField().getName(), toMongoExpression(context, expression, params));
			}

			protected static Object toMongoExpression(AggregationOperationContext context, String expression,
					Object[] params) {
				return TRANSFORMER.transform(expression, context, params);
			}
		}
	}

	/**
	 * Builder for {@link ProjectionOperation}s on a field.
	 * 
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 * @author Christoph Strobl
	 */
	public static class ProjectionOperationBuilder extends AbstractProjectionOperationBuilder {

		private static final String NUMBER_NOT_NULL = "Number must not be null!";
		private static final String FIELD_REFERENCE_NOT_NULL = "Field reference must not be null!";

		private final String name;
		private final OperationProjection previousProjection;

		/**
		 * Creates a new {@link ProjectionOperationBuilder} for the field with the given name on top of the given
		 * {@link ProjectionOperation}.
		 * 
		 * @param name must not be {@literal null} or empty.
		 * @param operation must not be {@literal null}.
		 * @param previousProjection the previous operation projection, may be {@literal null}.
		 */
		public ProjectionOperationBuilder(String name, ProjectionOperation operation,
				OperationProjection previousProjection) {
			super(name, operation);

			this.name = name;
			this.previousProjection = previousProjection;
		}

		/**
		 * Creates a new {@link ProjectionOperationBuilder} for the field with the given value on top of the given
		 * {@link ProjectionOperation}.
		 * 
		 * @param value
		 * @param operation
		 * @param previousProjection
		 */
		protected ProjectionOperationBuilder(Object value, ProjectionOperation operation,
				OperationProjection previousProjection) {

			super(value, operation);

			this.name = null;
			this.previousProjection = previousProjection;
		}

		/**
		 * Projects the result of the previous operation onto the current field. Will automatically add an exclusion for
		 * {@code _id} as what would be held in it by default will now go into the field just projected into.
		 * 
		 * @return
		 */
		public ProjectionOperation previousOperation() {

			return this.operation.andExclude(Fields.UNDERSCORE_ID) //
					.and(new PreviousOperationProjection(name));
		}

		/**
		 * Defines a nested field binding for the current field.
		 * 
		 * @param fields must not be {@literal null}.
		 * @return
		 */
		public ProjectionOperation nested(Fields fields) {
			return this.operation.and(new NestedFieldProjection(name, fields));
		}

		/**
		 * Allows to specify an alias for the previous projection operation.
		 * 
		 * @param alias
		 * @return
		 */
		@Override
		public ProjectionOperation as(String alias) {

			if (this.previousProjection != null) {
				return this.operation.andReplaceLastOneWith(this.previousProjection.withAlias(alias));
			}

			if (value instanceof AggregationExpression) {
				return this.operation.and(new ExpressionProjection(Fields.field(alias), (AggregationExpression) value));
			}

			return this.operation.and(new FieldProjection(Fields.field(alias, name), null));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.AbstractProjectionOperationBuilder#transform(org.springframework.data.mongodb.core.aggregation.ConditionalOperator)
		 */
		@Override
		public ProjectionOperation applyCondition(Cond cond) {

			Assert.notNull(cond, "ConditionalOperator must not be null!");
			return this.operation.and(new ExpressionProjection(Fields.field(name), cond));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.AbstractProjectionOperationBuilder#transform(org.springframework.data.mongodb.core.aggregation.IfNullOperator)
		 */
		@Override
		public ProjectionOperation applyCondition(IfNull ifNull) {

			Assert.notNull(ifNull, "IfNullOperator must not be null!");
			return this.operation.and(new ExpressionProjection(Fields.field(name), ifNull));
		}

		/**
		 * Generates an {@code $add} expression that adds the given number to the previously mentioned field.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder plus(Number number) {

			Assert.notNull(number, NUMBER_NOT_NULL);
			return project("add", number);
		}

		/**
		 * Generates an {@code $add} expression that adds the value of the given field to the previously mentioned field.
		 * 
		 * @param fieldReference
		 * @return
		 */
		public ProjectionOperationBuilder plus(String fieldReference) {

			Assert.notNull(fieldReference, "Field reference must not be null!");
			return project("add", Fields.field(fieldReference));
		}

		/**
		 * Generates an {@code $subtract} expression that subtracts the given number to the previously mentioned field.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder minus(Number number) {

			Assert.notNull(number, "Number must not be null!");
			return project("subtract", number);
		}

		/**
		 * Generates an {@code $subtract} expression that subtracts the value of the given field to the previously mentioned
		 * field.
		 * 
		 * @param fieldReference
		 * @return
		 */
		public ProjectionOperationBuilder minus(String fieldReference) {

			Assert.notNull(fieldReference, FIELD_REFERENCE_NOT_NULL);
			return project("subtract", Fields.field(fieldReference));
		}

		/**
		 * Generates an {@code $subtract} expression that subtracts the result of the given {@link AggregationExpression}
		 * from the previously mentioned field.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder minus(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return project("subtract", expression);
		}

		/**
		 * Generates an {@code $multiply} expression that multiplies the given number with the previously mentioned field.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder multiply(Number number) {

			Assert.notNull(number, NUMBER_NOT_NULL);
			return project("multiply", number);
		}

		/**
		 * Generates an {@code $multiply} expression that multiplies the value of the given field with the previously
		 * mentioned field.
		 * 
		 * @param fieldReference
		 * @return
		 */
		public ProjectionOperationBuilder multiply(String fieldReference) {

			Assert.notNull(fieldReference, FIELD_REFERENCE_NOT_NULL);
			return project("multiply", Fields.field(fieldReference));
		}

		/**
		 * Generates an {@code $multiply} expression that multiplies the previously with the result of the
		 * {@link AggregationExpression}. mentioned field.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder multiply(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return project("multiply", expression);
		}

		/**
		 * Generates an {@code $divide} expression that divides the previously mentioned field by the given number.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder divide(Number number) {

			Assert.notNull(number, FIELD_REFERENCE_NOT_NULL);
			Assert.isTrue(Math.abs(number.intValue()) != 0, "Number must not be zero!");
			return project("divide", number);
		}

		/**
		 * Generates an {@code $divide} expression that divides the value of the given field by the previously mentioned
		 * field.
		 * 
		 * @param fieldReference
		 * @return
		 */
		public ProjectionOperationBuilder divide(String fieldReference) {

			Assert.notNull(fieldReference, FIELD_REFERENCE_NOT_NULL);
			return project("divide", Fields.field(fieldReference));
		}

		/**
		 * Generates an {@code $divide} expression that divides the value of the previously mentioned by the result of the
		 * {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder divide(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return project("divide", expression);
		}

		/**
		 * Generates an {@code $mod} expression that divides the previously mentioned field by the given number and returns
		 * the remainder.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder mod(Number number) {

			Assert.notNull(number, NUMBER_NOT_NULL);
			Assert.isTrue(Math.abs(number.intValue()) != 0, "Number must not be zero!");
			return project("mod", number);
		}

		/**
		 * Generates an {@code $mod} expression that divides the value of the given field by the previously mentioned field
		 * and returns the remainder.
		 *
		 * @param fieldReference
		 * @return
		 */
		public ProjectionOperationBuilder mod(String fieldReference) {

			Assert.notNull(fieldReference, FIELD_REFERENCE_NOT_NULL);
			return project("mod", Fields.field(fieldReference));
		}

		/**
		 * Generates an {@code $mod} expression that divides the value of the previously mentioned field by the result of
		 * the {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder mod(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return project("mod", expression);
		}

		/**
		 * Generates a {@code $size} expression that returns the size of the array held by the given field. <br />
		 *
		 * @return never {@literal null}.
		 * @since 1.7
		 */
		public ProjectionOperationBuilder size() {
			return project("size");
		}

		/**
		 * Generates a {@code $cmp} expression (compare to) that compares the value of the field to a given value or field.
		 *
		 * @param compareValue compare value or a {@link Field} object.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder cmp(Object compareValue) {
			return project("cmp", compareValue);
		}

		/**
		 * Generates a {@code $eq} expression (equal) that compares the value of the field to a given value or field.
		 *
		 * @param compareValue compare value or a {@link Field} object.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder eq(Object compareValue) {
			return project("eq", compareValue);
		}

		/**
		 * Generates a {@code $gt} expression (greater than) that compares the value of the field to a given value or field.
		 *
		 * @param compareValue compare value or a {@link Field} object.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder gt(Object compareValue) {
			return project("gt", compareValue);
		}

		/**
		 * Generates a {@code $gte} expression (greater than equal) that compares the value of the field to a given value or
		 * field.
		 *
		 * @param compareValue compare value or a {@link Field} object.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder gte(Object compareValue) {
			return project("gte", compareValue);
		}

		/**
		 * Generates a {@code $lt} expression (less than) that compares the value of the field to a given value or field.
		 *
		 * @param compareValue compare value or a {@link Field} object.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder lt(Object compareValue) {
			return project("lt", compareValue);
		}

		/**
		 * Generates a {@code $lte} expression (less than equal) that compares the value of the field to a given value or
		 * field.
		 *
		 * @param compareValue the compare value or a {@link Field} object.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder lte(Object compareValue) {
			return project("lte", compareValue);
		}

		/**
		 * Generates a {@code $ne} expression (not equal) that compares the value of the field to a given value or field.
		 *
		 * @param compareValue compare value or a {@link Field} object.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder ne(Object compareValue) {
			return project("ne", compareValue);
		}

		/**
		 * Generates a {@code $slice} expression that returns a subset of the array held by the given field. <br />
		 * If {@literal n} is positive, $slice returns up to the first n elements in the array. <br />
		 * If {@literal n} is negative, $slice returns up to the last n elements in the array.
		 *
		 * @param count max number of elements.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder slice(int count) {
			return project("slice", count);
		}

		/**
		 * Generates a {@code $slice} expression that returns a subset of the array held by the given field. <br />
		 *
		 * @param count max number of elements. Must not be negative.
		 * @param offset the offset within the array to start from.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder slice(int count, int offset) {
			return project("slice", offset, count);
		}

		/**
		 * Generates a {@code $filter} expression that returns a subset of the array held by the given field.
		 *
		 * @param as The variable name for the element in the input array. Must not be {@literal null}.
		 * @param condition The {@link AggregationExpression} that determines whether to include the element in the
		 *          resulting array. Must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder filter(String as, AggregationExpression condition) {
			return this.operation.and(ArrayOperators.Filter.filter(name).as(as).by(condition));
		}

		// SET OPERATORS

		/**
		 * Generates a {@code $setEquals} expression that compares the previously mentioned field to one or more arrays and
		 * returns {@literal true} if they have the same distinct elements and {@literal false} otherwise.
		 *
		 * @param arrays must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder equalsArrays(String... arrays) {

			Assert.notEmpty(arrays, "Arrays must not be null or empty!");
			return project("setEquals", Fields.fields(arrays));
		}

		/**
		 * Generates a {@code $setIntersection} expression that takes array of the previously mentioned field and one or
		 * more arrays and returns an array that contains the elements that appear in every of those.
		 *
		 * @param arrays must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder intersectsArrays(String... arrays) {

			Assert.notEmpty(arrays, "Arrays must not be null or empty!");
			return project("setIntersection", Fields.fields(arrays));
		}

		/**
		 * Generates a {@code $setUnion} expression that takes array of the previously mentioned field and one or more
		 * arrays and returns an array that contains the elements that appear in any of those.
		 *
		 * @param arrays must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder unionArrays(String... arrays) {

			Assert.notEmpty(arrays, "Arrays must not be null or empty!");
			return project("setUnion", Fields.fields(arrays));
		}

		/**
		 * Generates a {@code $setDifference} expression that takes array of the previously mentioned field and returns an
		 * array containing the elements that do not exist in the given {@literal array}.
		 *
		 * @param array must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder differenceToArray(String array) {

			Assert.hasText(array, "Array must not be null or empty!");
			return project("setDifference", Fields.fields(array));
		}

		/**
		 * Generates a {@code $setIsSubset} expression that takes array of the previously mentioned field and returns
		 * {@literal true} if it is a subset of the given {@literal array}.
		 *
		 * @param array must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder subsetOfArray(String array) {

			Assert.hasText(array, "Array must not be null or empty!");
			return project("setIsSubset", Fields.fields(array));
		}

		/**
		 * Generates an {@code $anyElementTrue} expression that Takes array of the previously mentioned field and returns
		 * {@literal true} if any of the elements are {@literal true} and {@literal false} otherwise.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder anyElementInArrayTrue() {
			return project("anyElementTrue");
		}

		/**
		 * Generates an {@code $allElementsTrue} expression that takes array of the previously mentioned field and returns
		 * {@literal true} if no elements is {@literal false}.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder allElementsInArrayTrue() {
			return project("allElementsTrue");
		}

		/**
		 * Generates a {@code $abs} expression that takes the number of the previously mentioned field and returns the
		 * absolute value of it.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder absoluteValue() {
			return this.operation.and(ArithmeticOperators.Abs.absoluteValueOf(name));
		}

		/**
		 * Generates a {@code $ceil} expression that takes the number of the previously mentioned field and returns the
		 * smallest integer greater than or equal to the specified number.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder ceil() {
			return this.operation.and(ArithmeticOperators.Ceil.ceilValueOf(name));
		}

		/**
		 * Generates a {@code $exp} expression that takes the number of the previously mentioned field and raises Euler’s
		 * number (i.e. e ) on it.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder exp() {
			return this.operation.and(ArithmeticOperators.Exp.expValueOf(name));
		}

		/**
		 * Generates a {@code $floor} expression that takes the number of the previously mentioned field and returns the
		 * largest integer less than or equal to it.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder floor() {
			return this.operation.and(ArithmeticOperators.Floor.floorValueOf(name));
		}

		/**
		 * Generates a {@code $ln} expression that takes the number of the previously mentioned field and calculates the
		 * natural logarithm ln (i.e loge) of it.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder ln() {
			return this.operation.and(ArithmeticOperators.Ln.lnValueOf(name));
		}

		/**
		 * Generates a {@code $log} expression that takes the number of the previously mentioned field and calculates the
		 * log of the associated number in the specified base.
		 *
		 * @param baseFieldRef must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder log(String baseFieldRef) {
			return this.operation.and(ArithmeticOperators.Log.valueOf(name).log(baseFieldRef));
		}

		/**
		 * Generates a {@code $log} expression that takes the number of the previously mentioned field and calculates the
		 * log of the associated number in the specified base.
		 *
		 * @param base must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder log(Number base) {
			return this.operation.and(ArithmeticOperators.Log.valueOf(name).log(base));
		}

		/**
		 * Generates a {@code $log} expression that takes the number of the previously mentioned field and calculates the
		 * log of the associated number in the specified base.
		 *
		 * @param base must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder log(AggregationExpression base) {
			return this.operation.and(ArithmeticOperators.Log.valueOf(name).log(base));
		}

		/**
		 * Generates a {@code $log10} expression that takes the number of the previously mentioned field and calculates the
		 * log base 10.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder log10() {
			return this.operation.and(ArithmeticOperators.Log10.log10ValueOf(name));
		}

		/**
		 * Generates a {@code $pow} expression that takes the number of the previously mentioned field and raises it by the
		 * specified exponent.
		 *
		 * @param exponentFieldRef must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder pow(String exponentFieldRef) {
			return this.operation.and(ArithmeticOperators.Pow.valueOf(name).pow(exponentFieldRef));
		}

		/**
		 * Generates a {@code $pow} expression that takes the number of the previously mentioned field and raises it by the
		 * specified exponent.
		 *
		 * @param exponent must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder pow(Number exponent) {
			return this.operation.and(ArithmeticOperators.Pow.valueOf(name).pow(exponent));
		}

		/**
		 * Generates a {@code $pow} expression that Takes the number of the previously mentioned field and raises it by the
		 * specified exponent.
		 *
		 * @param exponentExpression must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder pow(AggregationExpression exponentExpression) {
			return this.operation.and(ArithmeticOperators.Pow.valueOf(name).pow(exponentExpression));
		}

		/**
		 * Generates a {@code $sqrt} expression that takes the number of the previously mentioned field and calculates the
		 * square root.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder sqrt() {
			return this.operation.and(ArithmeticOperators.Sqrt.sqrtOf(name));
		}

		/**
		 * Takes the number of the previously mentioned field and truncates it to its integer value.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder trunc() {
			return this.operation.and(ArithmeticOperators.Trunc.truncValueOf(name));
		}

		/**
		 * Generates a {@code $concat} expression that takes the string representation of the previously mentioned field and
		 * concats given values to it.
		 *
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder concat(Object... values) {
			return project("concat", values);
		}

		/**
		 * Generates a {@code $substr} expression that Takes the string representation of the previously mentioned field and
		 * returns a substring starting at a specified index position.
		 *
		 * @param start
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder substring(int start) {
			return substring(start, -1);
		}

		/**
		 * Generates a {@code $substr} expression that takes the string representation of the previously mentioned field and
		 * returns a substring starting at a specified index position including the specified number of characters.
		 *
		 * @param start
		 * @param nrOfChars
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder substring(int start, int nrOfChars) {
			return project("substr", start, nrOfChars);
		}

		/**
		 * Generates a {@code $toLower} expression that takes the string representation of the previously mentioned field
		 * and lowers it.
		 *
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder toLower() {
			return this.operation.and(StringOperators.ToLower.lowerValueOf(name));
		}

		/**
		 * Generates a {@code $toUpper} expression that takes the string representation of the previously mentioned field
		 * and uppers it.
		 *
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder toUpper() {
			return this.operation.and(StringOperators.ToUpper.upperValueOf(name));
		}

		/**
		 * Generates a {@code $strcasecmp} expression that takes the string representation of the previously mentioned field
		 * and performs case-insensitive comparison to the given {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder strCaseCmp(String value) {
			return project("strcasecmp", value);
		}

		/**
		 * Generates a {@code $strcasecmp} expression that takes the string representation of the previously mentioned field
		 * and performs case-insensitive comparison to the referenced {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder strCaseCmpValueOf(String fieldRef) {
			return project("strcasecmp", fieldRef);
		}

		/**
		 * Generates a {@code $strcasecmp} expression that takes the string representation of the previously mentioned field
		 * and performs case-insensitive comparison to the result of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder strCaseCmp(AggregationExpression expression) {
			return project("strcasecmp", expression);
		}

		/**
		 * Generates a {@code $arrayElemAt} expression that takes the string representation of the previously mentioned
		 * field and returns the element at the specified array {@literal position}.
		 *
		 * @param position
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder arrayElementAt(int position) {
			return project("arrayElemAt", position);
		}

		/**
		 * Generates a {@code $concatArrays} expression that takes the string representation of the previously mentioned
		 * field and concats it with the arrays from the referenced {@literal fields}.
		 *
		 * @param fields must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder concatArrays(String... fields) {
			return project("concatArrays", Fields.fields(fields));
		}

		/**
		 * Generates a {@code $isArray} expression that takes the string representation of the previously mentioned field
		 * and checks if its an array.
		 *
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder isArray() {
			return this.operation.and(ArrayOperators.IsArray.isArray(name));
		}

		/**
		 * Generates a {@code $literal} expression that Takes the value previously and uses it as literal.
		 *
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder asLiteral() {
			return this.operation.and(LiteralOperators.Literal.asLiteral(name));
		}

		/**
		 * Generates a {@code $dateToString} expression that takes the date representation of the previously mentioned field
		 * and applies given {@literal format} to it.
		 *
		 * @param format must not be {@literal null}.
		 * @return
		 * @since 1.10
		 */
		public ProjectionOperationBuilder dateAsFormattedString(String format) {
			return this.operation.and(DateOperators.DateToString.dateOf(name).toString(format));
		}

		/**
		 * Generates a {@code $let} expression that binds variables for use in the specified expression, and returns the
		 * result of the expression.
		 *
		 * @param valueExpression The {@link AggregationExpression} bound to {@literal variableName}.
		 * @param variableName The variable name to be used in the {@literal in} {@link AggregationExpression}.
		 * @param in The {@link AggregationExpression} to evaluate.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder let(AggregationExpression valueExpression, String variableName,
				AggregationExpression in) {
			return this.operation.and(VariableOperators.Let
					.define(ExpressionVariable.newVariable(variableName).forExpression(valueExpression)).andApply(in));
		}

		/**
		 * Generates a {@code $let} expression that binds variables for use in the specified expression, and returns the
		 * result of the expression.
		 *
		 * @param variables The bound {@link ExpressionVariable}s.
		 * @param in The {@link AggregationExpression} to evaluate.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public ProjectionOperationBuilder let(Collection<ExpressionVariable> variables, AggregationExpression in) {
			return this.operation.and(VariableOperators.Let.define(variables).andApply(in));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDBObject(AggregationOperationContext context) {
			return this.operation.toDBObject(context);
		}

		/**
		 * Adds a generic projection for the current field.
		 * 
		 * @param operation the operation key, e.g. {@code $add}.
		 * @param values the values to be set for the projection operation.
		 * @return
		 */
		public ProjectionOperationBuilder project(String operation, Object... values) {
			OperationProjection operationProjection = new OperationProjection(Fields.field(value.toString()), operation,
					values);
			return new ProjectionOperationBuilder(value, this.operation.and(operationProjection), operationProjection);
		}

		/**
		 * A {@link Projection} to pull in the result of the previous operation.
		 * 
		 * @author Oliver Gierke
		 */
		static class PreviousOperationProjection extends Projection {

			private final String name;

			/**
			 * Creates a new {@link PreviousOperationProjection} for the field with the given name.
			 * 
			 * @param name must not be {@literal null} or empty.
			 */
			public PreviousOperationProjection(String name) {
				super(Fields.field(name));
				this.name = name;
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
			 */
			@Override
			public DBObject toDBObject(AggregationOperationContext context) {
				return new BasicDBObject(name, Fields.UNDERSCORE_ID_REF);
			}
		}

		/**
		 * A {@link FieldProjection} to map a result of a previous {@link AggregationOperation} to a new field.
		 * 
		 * @author Oliver Gierke
		 * @author Thomas Darimont
		 * @author Mark Paluch
		 */
		static class FieldProjection extends Projection {

			private final Field field;
			private final Object value;

			/**
			 * Creates a new {@link FieldProjection} for the field of the given name, assigning the given value.
			 * 
			 * @param name must not be {@literal null} or empty.
			 * @param value
			 */
			public FieldProjection(String name, Object value) {
				this(Fields.field(name), value);
			}

			private FieldProjection(Field field, Object value) {

				super(new ExposedField(field.getName(), true));

				this.field = field;
				this.value = value;
			}

			/**
			 * Factory method to easily create {@link FieldProjection}s for the given {@link Fields}. Fields are projected as
			 * references with their given name. A field {@code foo} will be projected as: {@code foo : 1 } .
			 * 
			 * @param fields the {@link Fields} to in- or exclude, must not be {@literal null}.
			 * @return
			 */
			public static List<? extends Projection> from(Fields fields) {
				return from(fields, null);
			}

			/**
			 * Factory method to easily create {@link FieldProjection}s for the given {@link Fields}.
			 * 
			 * @param fields the {@link Fields} to in- or exclude, must not be {@literal null}.
			 * @param value to use for the given field.
			 * @return
			 */
			public static List<FieldProjection> from(Fields fields, Object value) {

				Assert.notNull(fields, "Fields must not be null!");
				List<FieldProjection> projections = new ArrayList<FieldProjection>();

				for (Field field : fields) {
					projections.add(new FieldProjection(field, value));
				}

				return projections;
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
			 */
			@Override
			public DBObject toDBObject(AggregationOperationContext context) {
				return new BasicDBObject(field.getName(), renderFieldValue(context));
			}

			private Object renderFieldValue(AggregationOperationContext context) {

				// implicit reference or explicit include?
				if (value == null || Boolean.TRUE.equals(value)) {

					if (Aggregation.SystemVariable.isReferingToSystemVariable(field.getTarget())) {
						return field.getTarget();
					}

					// check whether referenced field exists in the context
					return context.getReference(field).getReferenceValue();

				} else if (Boolean.FALSE.equals(value)) {

					// render field as excluded
					return 0;
				}

				return value;
			}
		}

		static class OperationProjection extends Projection {

			private final Field field;
			private final String operation;
			private final List<Object> values;

			/**
			 * Creates a new {@link OperationProjection} for the given field.
			 * 
			 * @param field the name of the field to add the operation projection for, must not be {@literal null} or empty.
			 * @param operation the actual operation key, must not be {@literal null} or empty.
			 * @param values the values to pass into the operation, must not be {@literal null}.
			 */
			public OperationProjection(Field field, String operation, Object[] values) {

				super(field);

				Assert.hasText(operation, "Operation must not be null or empty!");
				Assert.notNull(values, "Values must not be null!");

				this.field = field;
				this.operation = operation;
				this.values = Arrays.asList(values);
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
			 */
			@Override
			public DBObject toDBObject(AggregationOperationContext context) {

				DBObject inner = new BasicDBObject("$" + operation, getOperationArguments(context));

				return new BasicDBObject(getField().getName(), inner);
			}

			protected List<Object> getOperationArguments(AggregationOperationContext context) {

				List<Object> result = new ArrayList<Object>(values.size());
				result.add(context.getReference(getField()).toString());

				for (Object element : values) {

					if (element instanceof Field) {
						result.add(context.getReference((Field) element).toString());
					} else if (element instanceof Fields) {
						for (Field field : (Fields) element) {
							result.add(context.getReference(field).toString());
						}
					} else if (element instanceof AggregationExpression) {
						result.add(((AggregationExpression) element).toDbObject(context));
					} else {
						result.add(element);
					}
				}

				return result;
			}

			/**
			 * Returns the field that holds the {@link OperationProjection}.
			 * 
			 * @return
			 */
			protected Field getField() {
				return field;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#getExposedField()
			 */
			@Override
			public ExposedField getExposedField() {

				if (!getField().isAliased()) {
					return super.getExposedField();
				}

				return new ExposedField(new AggregationField(getField().getName()), true);
			}

			/**
			 * Creates a new instance of this {@link OperationProjection} with the given alias.
			 * 
			 * @param alias the alias to set
			 * @return
			 */
			public OperationProjection withAlias(String alias) {

				final Field aliasedField = Fields.field(alias, this.field.getName());
				return new OperationProjection(aliasedField, operation, values.toArray()) {

					/* (non-Javadoc)
					 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder.OperationProjection#getField()
					 */
					@Override
					protected Field getField() {
						return aliasedField;
					}

					@Override
					protected List<Object> getOperationArguments(AggregationOperationContext context) {

						// We have to make sure that we use the arguments from the "previous" OperationProjection that we replace
						// with this new instance.

						return OperationProjection.this.getOperationArguments(context);
					}
				};
			}
		}

		static class NestedFieldProjection extends Projection {

			private final String name;
			private final Fields fields;

			public NestedFieldProjection(String name, Fields fields) {

				super(Fields.field(name));
				this.name = name;
				this.fields = fields;
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
			 */
			@Override
			public DBObject toDBObject(AggregationOperationContext context) {

				DBObject nestedObject = new BasicDBObject();

				for (Field field : fields) {
					nestedObject.put(field.getName(), context.getReference(field.getTarget()).toString());
				}

				return new BasicDBObject(name, nestedObject);
			}
		}

		/**
		 * Extracts the minute from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractMinute() {
			return project("minute");
		}

		/**
		 * Extracts the hour from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractHour() {
			return project("hour");
		}

		/**
		 * Extracts the second from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractSecond() {
			return project("second");
		}

		/**
		 * Extracts the millisecond from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractMillisecond() {
			return project("millisecond");
		}

		/**
		 * Extracts the year from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractYear() {
			return project("year");
		}

		/**
		 * Extracts the month from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractMonth() {
			return project("month");
		}

		/**
		 * Extracts the week from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractWeek() {
			return project("week");
		}

		/**
		 * Extracts the dayOfYear from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractDayOfYear() {
			return project("dayOfYear");
		}

		/**
		 * Extracts the dayOfMonth from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractDayOfMonth() {
			return project("dayOfMonth");
		}

		/**
		 * Extracts the dayOfWeek from a date expression.
		 * 
		 * @return
		 */
		public ProjectionOperationBuilder extractDayOfWeek() {
			return project("dayOfWeek");
		}
	}

	/**
	 * Base class for {@link Projection} implementations.
	 * 
	 * @author Oliver Gierke
	 */
	private static abstract class Projection {

		private final ExposedField field;

		/**
		 * Creates new {@link Projection} for the given {@link Field}.
		 * 
		 * @param field must not be {@literal null}.
		 */
		public Projection(Field field) {

			Assert.notNull(field, "Field must not be null!");
			this.field = new ExposedField(field, true);
		}

		/**
		 * Returns the field exposed by the {@link Projection}.
		 * 
		 * @return will never be {@literal null}.
		 */
		public ExposedField getExposedField() {
			return field;
		}

		/**
		 * Renders the current {@link Projection} into a {@link DBObject} based on the given
		 * {@link AggregationOperationContext}.
		 * 
		 * @param context will never be {@literal null}.
		 * @return
		 */
		public abstract DBObject toDBObject(AggregationOperationContext context);
	}

	/**
	 * @author Thomas Darimont
	 */
	static class ExpressionProjection extends Projection {

		private final AggregationExpression expression;
		private final Field field;

		/**
		 * Creates a new {@link ExpressionProjection}.
		 * 
		 * @param field
		 * @param expression
		 */
		public ExpressionProjection(Field field, AggregationExpression expression) {

			super(field);
			this.field = field;
			this.expression = expression;
		}

		@Override
		public DBObject toDBObject(AggregationOperationContext context) {
			return new BasicDBObject(field.getName(), expression.toDbObject(context));
		}
	}
}
