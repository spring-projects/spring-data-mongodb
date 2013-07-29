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
		this(NONE, ProjectionOperationBuilder.FieldProjection.from(fields));
	}

	private ProjectionOperation(List<? extends Projection> current, List<? extends Projection> projections) {

		this.projections = new ArrayList<ProjectionOperation.Projection>(current.size() + projections.size());
		this.projections.addAll(current);
		this.projections.addAll(projections);
	}

	protected ProjectionOperation and(Projection projection) {
		return new ProjectionOperation(this.projections, Arrays.asList(projection));
	}

	public ProjectionOperationBuilder and(String name) {
		return new ProjectionOperationBuilder(name, this);
	}

	public ProjectionOperation and(Fields fields) {
		return new ProjectionOperation(this.projections, ProjectionOperationBuilder.FieldProjection.from(fields));
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

	public static class ProjectionOperationBuilder {

		private final String name;
		private final ProjectionOperation operation;

		public ProjectionOperationBuilder(String name) {
			this(name, new ProjectionOperation());
		}

		public ProjectionOperationBuilder(String name, ProjectionOperation operation) {
			this.name = name;
			this.operation = operation;
		}

		public ProjectionOperation drop() {
			return this.operation.and(new FieldProjection(name, 0));
		}

		public ProjectionOperation backReference() {
			return this.operation.and(new BackReferenceProjection(name));
		}

		public ProjectionOperation nested(Fields fields) {
			return this.operation.and(new NestedFieldProjection(name, fields));
		}

		public ProjectionOperation plus(Number number) {
			Assert.notNull(number, "Number must not be null!");
			return project("add", number);
		}

		public ProjectionOperation minus(Number number) {
			Assert.notNull(number, "Number must not be null!");
			return project("substract", number);
		}

		public ProjectionOperation project(String operation, Object... values) {
			return this.operation.and(new OperationProjection(name, operation, values));
		}

		static class BackReferenceProjection extends Projection {

			private final String name;

			public BackReferenceProjection(String name) {
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

		private static class FieldProjection extends Projection {

			private final Field field;
			private final Object value;

			private FieldProjection(Field field) {
				this(field, null);
			}

			public FieldProjection(String name, Object value) {
				this(Fields.field(name), value);
			}

			private FieldProjection(Field field, Object value) {

				super(field);

				this.field = field;
				this.value = value;
			}

			public static List<FieldProjection> from(Fields fields) {

				List<FieldProjection> projections = new ArrayList<FieldProjection>();

				for (Field field : fields) {
					projections.add(new FieldProjection(field));
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

		private static class OperationProjection extends Projection {

			private final String name;
			private final String operation;
			private final List<Object> values;

			public OperationProjection(String name, String operation, Object... values) {

				super(Fields.field(name));

				this.name = name;
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
				DBObject inner = new BasicDBObject(operation, values.size() == 1 ? values.get(0) : values.toArray());

				return new BasicDBObject(name, inner);
			}

			private List<Object> buildReferences(AggregationOperationContext context) {

				List<Object> result = new ArrayList<Object>(values.size());

				for (Object element : values) {
					result.add(element instanceof Field ? context.getReference((Field) element).toString() : element);
				}

				return result;
			}
		}

		private static class NestedFieldProjection extends Projection {

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

	static abstract class Projection {

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
