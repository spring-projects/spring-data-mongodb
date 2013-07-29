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

	private static final String UNDERSCORE_ID = "_id";

	private final ExposedFields fields;
	private final List<Operation> operations;

	public GroupOperation(Fields fields) {

		this.fields = ExposedFields.nonSynthetic(fields);
		this.operations = new ArrayList<Operation>();
	}

	public GroupOperation count(String field) {
		return sum(field, 1);
	}

	public GroupOperation sum(String field) {
		return sum(field, field);
	}

	public GroupOperation sum(String field, String reference) {
		return sum(field, reference, null);
	}

	public GroupOperation sum(String field, Object value) {
		return sum(field, null, value);
	}

	public GroupOperation sum(String field, String reference, Object value) {

		this.operations.add(new Operation(GroupOps.SUM, field, reference, value));
		return this;
	}

	public GroupOperation addToSet(String field) {
		return addToSet(field, field);
	}

	public GroupOperation addToSet(String field, String reference) {

		this.operations.add(new Operation(GroupOps.ADD_TO_SET, field, reference, null));
		return this;
	}

	public GroupOperation last(String field) {
		return last(field, field);
	}

	public GroupOperation last(String field, String reference) {

		this.operations.add(new Operation(GroupOps.LAST, field, reference, null));
		return this;
	}

	public GroupOperation first(String field) {
		return first(field, field);
	}

	public GroupOperation first(String field, String reference) {

		this.operations.add(new Operation(GroupOps.FIRST, field, reference, null));
		return this;
	}

	public GroupOperation avg(String field) {
		return avg(field, field);
	}

	public GroupOperation avg(String field, String reference) {
		this.operations.add(new Operation(GroupOps.AVG, field, reference, null));
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getFields()
	 */
	@Override
	public ExposedFields getFields() {

		ExposedFields fields = this.fields.and(new ExposedField(UNDERSCORE_ID, true));

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

		if (fields.exposesSingleFieldOnly()) {

			FieldReference reference = context.getReference(fields.iterator().next());
			operationObject.put(UNDERSCORE_ID, reference.toString());

		} else {

			BasicDBObject inner = new BasicDBObject();

			for (ExposedField field : fields) {
				FieldReference reference = context.getReference(field);
				inner.put(field.getName(), reference.toString());
			}

			operationObject.put(UNDERSCORE_ID, inner);
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

	private static class Operation implements AggregationOperation {

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
