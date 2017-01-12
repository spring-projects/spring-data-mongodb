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
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Encapsulates the aggregation framework {@code $group}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#group(Fields)} instead of creating instances of this
 * class directly.
 * 
 * @author Sebastian Herold
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Gustavo de Geus
 * @author Christoph Strobl
 * @since 1.3
 * @see <a href="https://docs.mongodb.org/manual/reference/aggregation/group/">MongoDB Aggregation Framework: $group</a>
 */
public class GroupOperation implements FieldsExposingAggregationOperation {

	/**
	 * Holds the non-synthetic fields which are the fields of the group-id structure.
	 */
	private final ExposedFields idFields;

	private final List<Operation> operations;

	/**
	 * Creates a new {@link GroupOperation} including the given {@link Fields}.
	 * 
	 * @param fields must not be {@literal null}.
	 */
	public GroupOperation(Fields fields) {

		this.idFields = ExposedFields.nonSynthetic(fields);
		this.operations = new ArrayList<Operation>();
	}

	/**
	 * Creates a new {@link GroupOperation} from the given {@link GroupOperation}.
	 * 
	 * @param groupOperation must not be {@literal null}.
	 */
	protected GroupOperation(GroupOperation groupOperation) {
		this(groupOperation, Collections.<Operation> emptyList());
	}

	/**
	 * Creates a new {@link GroupOperation} from the given {@link GroupOperation} and the given {@link Operation}s.
	 * 
	 * @param groupOperation
	 * @param nextOperations
	 */
	private GroupOperation(GroupOperation groupOperation, List<Operation> nextOperations) {

		Assert.notNull(groupOperation, "GroupOperation must not be null!");
		Assert.notNull(nextOperations, "NextOperations must not be null!");

		this.idFields = groupOperation.idFields;
		this.operations = new ArrayList<Operation>(nextOperations.size() + 1);
		this.operations.addAll(groupOperation.operations);
		this.operations.addAll(nextOperations);
	}

	/**
	 * Creates a new {@link GroupOperation} from the current one adding the given {@link Operation}.
	 * 
	 * @param operation must not be {@literal null}.
	 * @return
	 */
	protected GroupOperation and(Operation operation) {
		return new GroupOperation(this, Arrays.asList(operation));
	}

	/**
	 * Builder for {@link GroupOperation}s on a field.
	 * 
	 * @author Thomas Darimont
	 */
	public static final class GroupOperationBuilder {

		private final GroupOperation groupOperation;
		private final Operation operation;

		/**
		 * Creates a new {@link GroupOperationBuilder} from the given {@link GroupOperation} and {@link Operation}.
		 * 
		 * @param groupOperation
		 * @param operation
		 */
		private GroupOperationBuilder(GroupOperation groupOperation, Operation operation) {

			Assert.notNull(groupOperation, "GroupOperation must not be null!");
			Assert.notNull(operation, "Operation must not be null!");

			this.groupOperation = groupOperation;
			this.operation = operation;
		}

		/**
		 * Allows to specify an alias for the new-operation operation.
		 * 
		 * @param alias
		 * @return
		 */
		public GroupOperation as(String alias) {
			return this.groupOperation.and(operation.withAlias(alias));
		}
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for a {@code $sum}-expression.
	 * <p>
	 * Count expressions are emulated via {@code $sum: 1}.
	 * <p>
	 * 
	 * @return
	 */
	public GroupOperationBuilder count() {
		return newBuilder(GroupOps.SUM, null, 1);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for a {@code $sum}-expression for the given field-reference.
	 * 
	 * @param reference
	 * @return
	 */
	public GroupOperationBuilder sum(String reference) {
		return sum(reference, null);
	}

	private GroupOperationBuilder sum(String reference, Object value) {
		return newBuilder(GroupOps.SUM, reference, value);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $add_to_set}-expression for the given field-reference.
	 * 
	 * @param reference
	 * @return
	 */
	public GroupOperationBuilder addToSet(String reference) {
		return addToSet(reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $add_to_set}-expression for the given value.
	 * 
	 * @param value
	 * @return
	 */
	public GroupOperationBuilder addToSet(Object value) {
		return addToSet(null, value);
	}

	private GroupOperationBuilder addToSet(String reference, Object value) {
		return newBuilder(GroupOps.ADD_TO_SET, reference, value);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $last}-expression for the given field-reference.
	 * 
	 * @param reference
	 * @return
	 */
	public GroupOperationBuilder last(String reference) {
		return newBuilder(GroupOps.LAST, reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $last}-expression for the given
	 * {@link AggregationExpression}.
	 * 
	 * @param expr
	 * @return
	 */
	public GroupOperationBuilder last(AggregationExpression expr) {
		return newBuilder(GroupOps.LAST, null, expr);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for a {@code $first}-expression for the given field-reference.
	 * 
	 * @param reference
	 * @return
	 */
	public GroupOperationBuilder first(String reference) {
		return newBuilder(GroupOps.FIRST, reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for a {@code $first}-expression for the given
	 * {@link AggregationExpression}.
	 * 
	 * @param expr
	 * @return
	 */
	public GroupOperationBuilder first(AggregationExpression expr) {
		return newBuilder(GroupOps.FIRST, null, expr);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $avg}-expression for the given field-reference.
	 * 
	 * @param reference
	 * @return
	 */
	public GroupOperationBuilder avg(String reference) {
		return newBuilder(GroupOps.AVG, reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $avg}-expression for the given
	 * {@link AggregationExpression}.
	 * 
	 * @param expr
	 * @return
	 */
	public GroupOperationBuilder avg(AggregationExpression expr) {
		return newBuilder(GroupOps.AVG, null, expr);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $push}-expression for the given field-reference.
	 * 
	 * @param reference
	 * @return
	 */
	public GroupOperationBuilder push(String reference) {
		return push(reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $push}-expression for the given value.
	 * 
	 * @param value
	 * @return
	 */
	public GroupOperationBuilder push(Object value) {
		return push(null, value);
	}

	private GroupOperationBuilder push(String reference, Object value) {
		return newBuilder(GroupOps.PUSH, reference, value);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $min}-expression that for the given field-reference.
	 * 
	 * @param reference
	 * @return
	 */
	public GroupOperationBuilder min(String reference) {
		return newBuilder(GroupOps.MIN, reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $min}-expression that for the given
	 * {@link AggregationExpression}.
	 * 
	 * @param expr
	 * @return
	 */
	public GroupOperationBuilder min(AggregationExpression expr) {
		return newBuilder(GroupOps.MIN, null, expr);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $max}-expression that for the given field-reference.
	 * 
	 * @param reference
	 * @return
	 */
	public GroupOperationBuilder max(String reference) {
		return newBuilder(GroupOps.MAX, reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $max}-expression that for the given
	 * {@link AggregationExpression}.
	 * 
	 * @param expr
	 * @return
	 */
	public GroupOperationBuilder max(AggregationExpression expr) {
		return newBuilder(GroupOps.MAX, null, expr);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $stdDevSamp}-expression that for the given
	 * field-reference.
	 *
	 * @param reference must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public GroupOperationBuilder stdDevSamp(String reference) {
		return newBuilder(GroupOps.STD_DEV_SAMP, reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $stdDevSamp}-expression that for the given {@link AggregationExpression}.
	 *
	 * @param expr must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public GroupOperationBuilder stdDevSamp(AggregationExpression expr) {
		return newBuilder(GroupOps.STD_DEV_SAMP, null, expr);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $stdDevPop}-expression that for the given field-reference.
	 *
	 * @param reference must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public GroupOperationBuilder stdDevPop(String reference) {
		return newBuilder(GroupOps.STD_DEV_POP, reference, null);
	}

	/**
	 * Generates an {@link GroupOperationBuilder} for an {@code $stdDevPop}-expression that for the given {@link AggregationExpression}.
	 *
	 * @param expr must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public GroupOperationBuilder stdDevPop(AggregationExpression expr) {
		return newBuilder(GroupOps.STD_DEV_POP, null, expr);
	}

	private GroupOperationBuilder newBuilder(Keyword keyword, String reference, Object value) {
		return new GroupOperationBuilder(this, new Operation(keyword, null, reference, value));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getFields()
	 */
	@Override
	public ExposedFields getFields() {

		ExposedFields fields = this.idFields.and(new ExposedField(Fields.UNDERSCORE_ID, true));

		for (Operation operation : operations) {
			fields = fields.and(operation.asField());
		}

		return fields;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document operationObject = new Document();

		if (idFields.exposesNoNonSyntheticFields()) {

			operationObject.put(Fields.UNDERSCORE_ID, null);

		} else if (idFields.exposesSingleNonSyntheticFieldOnly()) {

			FieldReference reference = context.getReference(idFields.iterator().next());
			operationObject.put(Fields.UNDERSCORE_ID, reference.toString());

		} else {

			Document inner = new Document();

			for (ExposedField field : idFields) {
				FieldReference reference = context.getReference(field);
				inner.put(field.getName(), reference.toString());
			}

			operationObject.put(Fields.UNDERSCORE_ID, inner);
		}

		for (Operation operation : operations) {
			operationObject.putAll(operation.toDocument(context));
		}

		return new Document("$group", operationObject);
	}

	interface Keyword {

		String toString();
	}

	private static enum GroupOps implements Keyword {

		SUM("$sum"), LAST("$last"), FIRST("$first"), PUSH("$push"), AVG("$avg"), MIN("$min"), MAX("$max"), ADD_TO_SET("$addToSet"), STD_DEV_POP("$stdDevPop"), STD_DEV_SAMP("$stdDevSamp");

		private String mongoOperator;

		GroupOps(String mongoOperator) {
			this.mongoOperator = mongoOperator;
		}


		@Override
		public String toString() {
			return mongoOperator;
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

		public Operation withAlias(String key) {
			return new Operation(op, key, reference, value);
		}

		public ExposedField asField() {
			return new ExposedField(key, true);
		}

		public Document toDocument(AggregationOperationContext context) {
			return new Document(key, new Document(op.toString(), getValue(context)));
		}

		public Object getValue(AggregationOperationContext context) {

			if (reference == null) {

				if (value instanceof AggregationExpression) {
					return ((AggregationExpression) value).toDocument(context);
				}

				return value;
			}

			if (Aggregation.SystemVariable.isReferingToSystemVariable(reference)) {
				return reference;
			}

			return context.getReference(reference).toString();
		}

		@Override
		public String toString() {
			return "Operation [op=" + op + ", key=" + key + ", reference=" + reference + ", value=" + value + "]";
		}
	}
}
