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
public class ProjectionOperation extends ExposedFieldsAggregationOperationContext implements AggregationOperation {

	private static final List<Projection> NONE = Collections.emptyList();

	private final List<Projection> projections;

	public ProjectionOperation() {
		this(NONE, NONE);
	}

	public ProjectionOperation(Fields fields) {
		this(NONE, ProjectionOperationBuilder.FieldProjection.from(fields, true));
	}

	private ProjectionOperation(List<? extends Projection> current, List<? extends Projection> projections) {

		this.projections = new ArrayList<ProjectionOperation.Projection>(current.size() + projections.size());
		this.projections.addAll(current);
		this.projections.addAll(projections);
	}

	protected ProjectionOperation and(Projection projection) {
		return new ProjectionOperation(this.projections, Arrays.asList(projection));
	}

	/**
	 * Creates a new {@link ProjectionOperationBuilder} to define a projection for the field with the given name.
	 * 
	 * @param name must not be {@literal null} or empty.
	 * @return
	 */
	public ProjectionOperationBuilder and(String name) {
		return new ProjectionOperationBuilder(name, this);
	}

	/**
	 * Excludes the given fields from the projection.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public ProjectionOperation andExclude(String... fields) {
		List<FieldProjection> excludeProjections = FieldProjection.from(Fields.fields(fields), false);
		return new ProjectionOperation(this.projections, excludeProjections);
	}

	/**
	 * Includes the given fields into the projection.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public ProjectionOperation andInclude(String... fields) {

		List<FieldProjection> projections = FieldProjection.from(Fields.fields(fields), true);
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
	 * @see org.springframework.data.mongodb.core.aggregation.ExposedFieldsAggregationOperationContext#getFields()
	 */
	@Override
	protected ExposedFields getFields() {

		ExposedFields fields = null;

		for (Projection projection : projections) {
			ExposedField field = projection.getField();
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
	 * Builder for {@link ProjectionOperation}s on a field.
	 * 
	 * @author Oliver Gierke
	 */
	public static class ProjectionOperationBuilder implements AggregationOperation {

		private final String name;
		private ProjectionOperation operation;
		private OperationProjection previousProjection;

		/**
		 * Creates a new {@link ProjectionOperationBuilder} for the field with the given name on top of the given
		 * {@link ProjectionOperation}.
		 * 
		 * @param name must not be {@literal null} or empty.
		 * @param operation must not be {@literal null}.
		 */
		public ProjectionOperationBuilder(String name, ProjectionOperation operation) {

			Assert.hasText(name, "Field name must not be null or empty!");
			Assert.notNull(operation, "ProjectionOperation must not be null!");

			this.name = name;
			this.operation = operation;
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

			Assert.notNull(this.previousProjection, "previousProjection must not be null!");
			this.previousProjection.setAliasName(alias);
			return this.operation;
		}

		/**
		 * Generates an {@code $add} expression that adds the given number to the previously mentioned field.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder plus(Number number) {

			Assert.notNull(number, "Number must not be null!");
			this.operation = project("add", number);
			return this;
		}

		/**
		 * Generates an {@code $subtract} expression that subtracts the given number to the previously mentioned field.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder minus(Number number) {

			Assert.notNull(number, "Number must not be null!");
			this.operation = project("subtract", number);
			return this;
		}

		/**
		 * Generates an {@code $multiply} expression that multiplies the given number with the previously mentioned field.
		 * 
		 * @param number
		 * @return
		 */
		public ProjectionOperationBuilder multiply(Number number) {

			Assert.notNull(number, "Number must not be null!");
			this.operation = project("multiply", number);
			return this;
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

			this.operation = project("divide", number);
			return this;
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

			this.operation = project("mod", number);
			return this;
		}

		/* (non-Javadoc)
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
		public ProjectionOperation project(String operation, Object... values) {
			this.previousProjection = new OperationProjection(name, operation, name, values);
			return this.operation.and(previousProjection);
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
			 * Factory method to easily create {@link FieldProjection}s for the given {@link Fields}.
			 * 
			 * @param fields the {@link Fields} to in- or exclude, must not be {@literal null}.
			 * @param include whether to include or exclude the fields.
			 * @return
			 */
			public static List<FieldProjection> from(Fields fields, boolean include) {

				Assert.notNull(fields, "Fields must not be null!");
				List<FieldProjection> projections = new ArrayList<FieldProjection>();

				for (Field field : fields) {
					projections.add(new FieldProjection(field, include ? null : 0));
				}

				return projections;
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
			 */
			@Override
			public DBObject toDBObject(AggregationOperationContext context) {

				if (value != null) {
					return new BasicDBObject(field.getName(), value);
				}

				FieldReference reference = context.getReference(field.getTarget());
				return new BasicDBObject(field.getName(), reference.toString());
			}
		}

		static class OperationProjection extends Projection {

			private final String name;
			private final String operation;
			private final List<Object> values;

			private String aliasName;

			/**
			 * Creates a new {@link OperationProjection} for the given field.
			 * 
			 * @param name the name of the field to add the operation projection for, must not be {@literal null} or empty.
			 * @param aliasName the name of the field alias to write that will hold the operation projection, must not be
			 *          {@literal null} or empty.
			 * @param operation the actual operation key, must not be {@literal null} or empty.
			 * @param values the values to pass into the operation, must not be {@literal null}.
			 */
			public OperationProjection(String name, String operation, String aliasName, Object... values) {

				super(Fields.field(name));

				Assert.hasText(aliasName, "aliasName must not be null or empty!");
				Assert.hasText(operation, "Operation must not be null or empty!");
				Assert.notNull(values, "Values must not be null!");

				this.name = name;
				this.aliasName = aliasName;
				this.operation = operation;
				this.values = Arrays.asList(values);
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.ProjectionOperation.Projection#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
			 */
			@Override
			public DBObject toDBObject(AggregationOperationContext context) {

				List<Object> values = buildReferences(context);
				DBObject inner = new BasicDBObject("$" + operation, values.toArray());

				return new BasicDBObject(this.aliasName, inner);
			}

			private List<Object> buildReferences(AggregationOperationContext context) {

				List<Object> result = new ArrayList<Object>(values.size());
				result.add(context.getReference(this.name).toString());

				for (Object element : values) {
					result.add(element instanceof Field ? context.getReference((Field) element).toString() : element);
				}

				return result;
			}

			/**
			 * @param aliasName the aliasName to set
			 */
			public void setAliasName(String aliasName) {
				this.aliasName = aliasName;
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

	private static abstract class Projection {

		private final ExposedField field;

		public Projection(Field name) {

			Assert.notNull(name, "Field must not be null!");
			this.field = new ExposedField(name, true);
		}

		public ExposedField getField() {
			return field;
		}

		public abstract DBObject toDBObject(AggregationOperationContext context);
	}
}
