/*
 * Copyright 2013 the original author or authors.
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
import java.util.Collections;
import java.util.List;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder.FieldProjection;
import org.springframework.util.Assert;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $project}-operation. Projection of field to be used in an
 * {@link Aggregation}. A projection is similar to a {@link Field} inclusion/exclusion but more powerful. It can
 * generate new fields, change values of given field etc.
 * <p>
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/project/
 * @author Tobias Trelle
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.3
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

		List<Projection> projections = this.projections.isEmpty() ? Collections.<Projection> emptyList() : this.projections
				.subList(0, this.projections.size() - 1);
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
	 * @author Thomas Darimont
	 */
	public static abstract class AbstractProjectionOperationBuilder implements AggregationOperation {

		protected final Object value;
		protected final ProjectionOperation operation;

		public AbstractProjectionOperationBuilder(Object value, ProjectionOperation operation) {

			Assert.notNull(value, "value must not be null or empty!");
			Assert.notNull(operation, "ProjectionOperation must not be null!");

			this.value = value;
			this.operation = operation;
		}

		public abstract ProjectionOperation as(String alias);

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDBObject(AggregationOperationContext context) {
			return this.operation.toDBObject(context);
		}
	}

	/**
	 * @author Thomas Darimont
	 */
	public static class ExpressionProjectionOperationBuilder extends AbstractProjectionOperationBuilder {

		private Object[] params;

		public ExpressionProjectionOperationBuilder(Object value, ProjectionOperation operation, Object[] params) {
			super(value, operation);
			this.params = params;
		}

		public ProjectionOperation as(String alias) {

			return this.operation.and(new ExpressionProjection(Fields.field(alias, "expr"), this.value.toString(), params));
		}

		static class ExpressionProjection extends Projection {

			private String expression;
			private Object[] params;

			public ExpressionProjection(Field field, String expression, Object[] params) {
				super(field);
				this.expression = expression;
				this.params = params;
			}

			/* (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
			 */
			@Override
			public DBObject toDBObject(AggregationOperationContext context) {
				return new BasicDBObject(getExposedField().getName(),
						SpelExpressionToMongoExpressionTransformer.INSTANCE.transform(expression, context, params));
			}
		}
	}

	/**
	 * Builder for {@link ProjectionOperation}s on a field.
	 * 
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	public static class ProjectionOperationBuilder extends AbstractProjectionOperationBuilder {

		private final String name;
		private final ProjectionOperation operation;
		private final OperationProjection previousProjection;

		/**
		 * Creates a new {@link ProjectionOperationBuilder} for the field with the given name on top of the given
		 * {@link ProjectionOperation}.
		 * 
		 * @param name must not be {@literal null} or empty.
		 * @param operation must not be {@literal null}.
		 * @param previousProjection the previous operation projection, may be {@literal null}.
		 */
		public ProjectionOperationBuilder(String name, ProjectionOperation operation, OperationProjection previousProjection) {
			super(name, operation);

			this.name = name;
			this.operation = operation;
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
		 * @param string
		 * @return
		 */
		public ProjectionOperation as(String alias) {

			if (this.previousProjection != null) {
				return this.operation.andReplaceLastOneWith(this.previousProjection.withAlias(alias));
			} else {
				return this.operation.and(new FieldProjection(Fields.field(alias, name), null));
			}
		}

		/**
		 * Generates an {@code $add} expression that adds the given number to the previously mentioned field.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder plus(Number number) {

			Assert.notNull(number, "Number must not be null!");
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

			Assert.notNull(fieldReference, "Field reference must not be null!");
			return project("subtract", Fields.field(fieldReference));
		}

		/**
		 * Generates an {@code $multiply} expression that multiplies the given number with the previously mentioned field.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder multiply(Number number) {

			Assert.notNull(number, "Number must not be null!");
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

			Assert.notNull(fieldReference, "Field reference must not be null!");
			return project("multiply", Fields.field(fieldReference));
		}

		/**
		 * Generates an {@code $divide} expression that divides the previously mentioned field by the given number.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder divide(Number number) {

			Assert.notNull(number, "Number must not be null!");
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

			Assert.notNull(fieldReference, "Field reference must not be null!");
			return project("divide", Fields.field(fieldReference));
		}

		/**
		 * Generates an {@code $mod} expression that divides the previously mentioned field by the given number and returns
		 * the remainder.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder mod(Number number) {

			Assert.notNull(number, "Number must not be null!");
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

			Assert.notNull(fieldReference, "Field reference must not be null!");
			return project("mod", Fields.field(fieldReference));
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
			OperationProjection projectionOperation = new OperationProjection(Fields.field(name), operation, values);
			return new ProjectionOperationBuilder(name, this.operation.and(projectionOperation), projectionOperation);
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

				super(field);

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

					// check whether referenced field exists in the context
					FieldReference reference = context.getReference(field.getTarget());

					if (field.getName().equals(field.getTarget())) {

						// render field as included
						return 1;
					}

					// render field reference
					return reference.toString();
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
			 * @param name the name of the field to add the operation projection for, must not be {@literal null} or empty.
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

				BasicDBList values = new BasicDBList();
				values.addAll(buildReferences(context));

				DBObject inner = new BasicDBObject("$" + operation, values);

				return new BasicDBObject(this.field.getName(), inner);
			}

			private List<Object> buildReferences(AggregationOperationContext context) {

				List<Object> result = new ArrayList<Object>(values.size());
				result.add(context.getReference(field.getTarget()).toString());

				for (Object element : values) {
					result.add(element instanceof Field ? context.getReference((Field) element).toString() : element);
				}

				return result;
			}

			/**
			 * Creates a new instance of this {@link OperationProjection} with the given alias.
			 * 
			 * @param alias the alias to set
			 * @return
			 */
			public OperationProjection withAlias(String alias) {
				return new OperationProjection(Fields.field(alias, this.field.getName()), operation, values.toArray());
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

}
