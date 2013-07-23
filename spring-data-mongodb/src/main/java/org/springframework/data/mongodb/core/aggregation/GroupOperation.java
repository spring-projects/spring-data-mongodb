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
import java.util.List;
import java.util.Locale;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $group}-operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/group/#stage._S_group
 * @author Sebastian Herold
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.3
 */
public class GroupOperation extends ExposedFieldsAggregationOperationContext implements AggregationOperation {

	private final ExposedFields nonSynthecticFields;
	private final List<Operation> operations;

	/**
	 * Creates a new {@link GroupOperation} including the given {@link Fields}.
	 * 
	 * @param fields must not be {@literal null}.
	 */
	public GroupOperation(Fields fields) {

		this.nonSynthecticFields = ExposedFields.nonSynthetic(fields);
		this.operations = new ArrayList<Operation>();
	}

	/**
	 * Creates a new {@link GroupOperation} from the given {@link GroupOperation} and the given {@link Operation}.
	 * 
	 * @param current must not be {@literal null}.
	 * @param operation must not be {@literal null}.
	 */
	protected GroupOperation(GroupOperation current, Operation operation) {

		Assert.notNull(current, "GroupOperation must not be null!");
		Assert.notNull(operation, "Operation must not be null!");

		this.nonSynthecticFields = current.nonSynthecticFields;
		this.operations = new ArrayList<Operation>(current.operations.size() + 1);
		this.operations.addAll(current.operations);
		this.operations.add(operation);
	}

	/**
	 * Creates a new {@link GroupOperation} from the current one adding the given {@link Operation}.
	 * 
	 * @param operation must not be {@literal null}.
	 * @return
	 */
	protected GroupOperation and(Operation operation) {
		return new GroupOperation(this, operation);
	}

	/**
	 * Returns a {@link GroupOperationBuilder} to build a grouping operation for the field with the given name
	 * 
	 * @param field must not be {@literal null} or empty.
	 * @return
	 */
	public GroupOperationBuilder and(String field) {
		return new GroupOperationBuilder(field, this);
	}

	public class GroupOperationBuilder {

		private final String name;
		private final GroupOperation current;

		public GroupOperationBuilder(String name, GroupOperation current) {

			Assert.hasText(name, "Field name must not be null or empty!");
			Assert.notNull(current, "GroupOperation must not be null!");

			this.name = name;
			this.current = current;
		}

		public GroupOperation count() {
			return sum(1);
		}

		public GroupOperation count(String reference) {
			return sum(reference, 1);
		}

		public GroupOperation sum() {
			return sum(name);
		}

		public GroupOperation sum(String reference) {
			return sum(reference, null);
		}

		public GroupOperation sum(Object value) {
			return sum(null, value);
		}

		public GroupOperation sum(String reference, Object value) {
			return current.and(new Operation(GroupOps.SUM, name, reference, value));
		}

		public GroupOperation addToSet() {
			return addToSet(null);
		}

		public GroupOperation addToSet(String reference) {
			return current.and(new Operation(GroupOps.ADD_TO_SET, name, reference, null));
		}

		public GroupOperation last() {
			return last(null);
		}

		public GroupOperation last(String reference) {
			return current.and(new Operation(GroupOps.LAST, name, reference, null));
		}

		public GroupOperation first() {
			return first(null);
		}

		public GroupOperation first(String reference) {
			return current.and(new Operation(GroupOps.FIRST, name, reference, null));
		}

		public GroupOperation avg() {
			return avg(null);
		}

		public GroupOperation avg(String reference) {
			return current.and(new Operation(GroupOps.AVG, name, reference, null));
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getFields()
	 */
	@Override
	public ExposedFields getFields() {

		ExposedFields fields = this.nonSynthecticFields.and(new ExposedField(Fields.UNDERSCORE_ID, true));

		for (Operation operation : operations) {
			fields = fields.and(operation.asField());
		}

		return fields;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public com.mongodb.DBObject toDBObject(AggregationOperationContext context) {

		BasicDBObject operationObject = new BasicDBObject();

		if (nonSynthecticFields.exposesSingleFieldOnly()) {

			FieldReference reference = context.getReference(nonSynthecticFields.iterator().next());
			operationObject.put(Fields.UNDERSCORE_ID, reference.toString());

		} else {

			BasicDBObject inner = new BasicDBObject();

			for (ExposedField field : nonSynthecticFields) {
				FieldReference reference = context.getReference(field);
				inner.put(field.getName(), reference.toString());
			}

			operationObject.put(Fields.UNDERSCORE_ID, inner);
		}

		for (Operation operation : operations) {
			operationObject.putAll(operation.toDBObject(context));
		}

		return new BasicDBObject("$group", operationObject);
	}

	interface Keyword {

		String toString();
	}

	private static enum GroupOps implements Keyword {

		SUM, LAST, FIRST, PUSH, AVG, MIN, MAX, ADD_TO_SET, COUNT;

		@Override
		public String toString() {

			String[] parts = name().split("_");

			StringBuilder builder = new StringBuilder();

			for (String part : parts) {
				String lowerCase = part.toLowerCase(Locale.US);
				builder.append(builder.length() == 0 ? lowerCase : StringUtils.capitalize(lowerCase));
			}

			return "$" + builder.toString();
		}
	}

	static class Operation implements AggregationOperation {

		private final Keyword op;
		private final String key;
		private final String reference;
		private final Object value;

		public Operation(Keyword op, String key, String reference, Object value) {

			this.op = op;
			this.key = key;
			this.reference = reference;
			this.value = value;
		}

		public ExposedField asField() {
			return new ExposedField(key, true);
		}

		public DBObject toDBObject(AggregationOperationContext context) {
			return new BasicDBObject(key, new BasicDBObject(op.toString(), getValue(context)));
		}

		public Object getValue(AggregationOperationContext context) {
			return reference == null ? value : context.getReference(reference).toString();
		}
	}
}
