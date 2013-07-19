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
import java.util.Map;

import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $group}-operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/group/#stage._S_group
 * @author Sebastian Herold
 * @author Thomas Darimont
 * @since 1.3
 */
public class GroupOperation extends AbstractContextProducingAggregateOperation {

	final Object id;
	final List<GroupingOperation> ops = new ArrayList<GroupOperation.GroupingOperation>();

	/**
	 * Creates a <code>$group</code> operation with <code>_id</code> referencing to a field of the document. The returned
	 * db object equals to
	 * 
	 * <pre>
	 * {_id: "$field"}
	 * </pre>
	 * 
	 * @param id
	 * @param moreIdFields
	 */
	public GroupOperation(Fields fields) {
		super("group");
		this.id = createGroupIdFrom(fields);
	}

	/**
	 * @param fields
	 * @return
	 */
	private Object createGroupIdFrom(Fields fields) {

		Assert.notNull(fields, "fields must not be null!");
		Map<String, Object> values = fields.getValues();
		Assert.notEmpty(values, "fields.values must not be empty!");

		DBObject idReferences = new BasicDBObject(values.size());
		for (Map.Entry<String, Object> entry : values.entrySet()) {
			String idFieldName = ReferenceUtil.safeNonReference(entry.getKey());
			Object idFieldValue = entry.getValue() instanceof String ? ReferenceUtil.safeReference(entry.getValue()
					.toString()) : entry.getValue();
			idReferences.put(idFieldName, idFieldValue);
		}
		return idReferences;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AbstractAggregateOperation#getOperationArgument()
	 */
	@Override
	public Object getOperationArgument(AggregateOperationContext inputAggregateOperationContext) {

		DBObject projection = new BasicDBObject();

		Object idToUse = id;
		if (idToUse instanceof DBObject) {
			idToUse = createGroupIdObject((DBObject) idToUse, inputAggregateOperationContext);
		}
		projection.put(ReferenceUtil.ID_KEY, idToUse);

		for (GroupingOperation op : ops) {
			projection.put(op.alias, op.toDbObject(inputAggregateOperationContext));
		}

		return projection;
	}

	/**
	 * @param idCandidate
	 * @param inputAggregateOperationContext
	 * @return
	 */
	private Object createGroupIdObject(DBObject groupIdObject, AggregateOperationContext inputAggregateOperationContext) {

		Object simpleIdOrNull = returnIfGroupIdIsSingleFieldReference(inputAggregateOperationContext, groupIdObject);
		if (simpleIdOrNull != null) {
			return simpleIdOrNull;
		}

		DBObject idObject = new BasicDBObject();
		for (String idFieldName : groupIdObject.keySet()) {

			Object idFieldValue = groupIdObject.get(idFieldName);
			Object idFieldValueOrNull = returnIfFieldValueReferencesAvailableField(inputAggregateOperationContext,
					idFieldName, idFieldValue);
			if (idFieldValueOrNull != null) {
				idFieldValue = idFieldValueOrNull;
			}

			getOutputAggregateOperationContext().registerAvailableField(idFieldName, ReferenceUtil.id(idFieldName));
			idObject.put(idFieldName, idFieldValue);
		}

		return idObject;
	}

	private Object returnIfGroupIdIsSingleFieldReference(AggregateOperationContext inputAggregateOperationContext,
			DBObject idObject) {

		if (idObject.keySet().size() != 1) {
			return null;
		}

		return returnIfFieldNameIsSimpleReference(inputAggregateOperationContext, idObject, idObject.keySet().iterator()
				.next());
	}

	private Object returnIfFieldValueReferencesAvailableField(AggregateOperationContext inputAggregateOperationContext,
			String idFieldName, Object idFieldValue) {

		Assert.notNull(inputAggregateOperationContext, "inputAggregateOperationContext must not be null");

		if (!ReferenceUtil.isValueFieldReference(idFieldName, idFieldValue)) {
			return null;
		}

		if (!inputAggregateOperationContext.isFieldAvailable(idFieldName)) {
			return null;
		}

		String idFieldNameToUse = inputAggregateOperationContext.returnFieldNameAliasIfAvailableOr(idFieldName);
		return ReferenceUtil.safeReference(inputAggregateOperationContext instanceof GroupOperation ? ReferenceUtil
				.id(idFieldNameToUse) : idFieldNameToUse);
	}

	private Object returnIfFieldNameIsSimpleReference(AggregateOperationContext inputAggregateOperationContext,
			DBObject idObject, String idFieldName) {

		Object idFieldValue = idObject.get(idFieldName);

		if (!idFieldValueIsSimpleIdFieldExpression(idFieldName, idFieldValue)) {
			return null;
		}

		getOutputAggregateOperationContext().registerAvailableField(idFieldName, ReferenceUtil.id(idFieldName));
		idFieldValue = ReferenceUtil.safeReference(inputAggregateOperationContext
				.returnFieldNameAliasIfAvailableOr(idFieldName));

		return idFieldValue;
	}

	private static boolean idFieldValueIsSimpleIdFieldExpression(String idFieldName, Object idFieldValue) {
		return idFieldValue instanceof String
				&& idFieldName.equals(ReferenceUtil.safeNonReference(idFieldValue.toString()));
	}

	/**
	 * Adds a field with the <a
	 * href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_addToSet">$addToSet operation</a>.
	 * 
	 * <pre>
	 * { $group : {
	 *      _id : "$id_field",
	 *     name : { $addToSet : "$field" }
	 * }}
	 * </pre>
	 * 
	 * @see http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_addToSet
	 * @param name key of the field.
	 * @param field reference to a field of the document.
	 * @return
	 */
	public GroupOperation addToSet(String name, String field) {
		return addOperation("$addToSet", name, field);
	}

	/**
	 * Adds a field with the {@code $first} operation.
	 * 
	 * <pre>
	 * { $group : {
	 *      _id : "$id_field",
	 *     name : { $first : "$field" }
	 * }}
	 * </pre>
	 * 
	 * @see http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_first
	 * @param name key of the field
	 * @param field reference to a field of the document
	 * @return
	 */
	public GroupOperation first(String name, String field) {
		return addOperation("$first", name, field);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_last">$last
	 * operation</a>.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          name: {$last: "$field"}
	 *     }}
	 * </pre>
	 * 
	 * @param name key of the field
	 * @param field reference to a field of the document
	 * @return
	 */
	public GroupOperation last(String name, String field) {
		return addOperation("$last", name, field);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_max">$max
	 * operation</a>.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          name: {$max: "$field"}
	 *     }}
	 * </pre>
	 * 
	 * @param name key of the field
	 * @param field reference to a field of the document
	 * @return
	 */
	public GroupOperation max(String name, String field) {
		return addOperation("$max", name, field);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_min">$min
	 * operation</a>.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          name: {$min: "$field"}
	 *     }}
	 * </pre>
	 * 
	 * @param name key of the field
	 * @param field reference to a field of the document
	 * @return
	 */
	public GroupOperation min(String name, String field) {
		return addOperation("$min", name, field);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_avg">$avg
	 * operation</a>.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          name: {$avg: "$field"}
	 *     }}
	 * </pre>
	 * 
	 * @param name key of the field
	 * @param field reference to a field of the document
	 * @return
	 */
	public GroupOperation avg(String name, String field) {
		return addOperation("$avg", name, field);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_push">$push
	 * operation</a>.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          name: {$push: "$field"}
	 *     }}
	 * </pre>
	 * 
	 * @param name key of the field
	 * @param field reference to a field of the document
	 * @return
	 */
	public GroupOperation push(String name, String field) {
		return addOperation("$push", name, field);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_sum">$sum
	 * operation</a> with a constant value.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          name: {$sum: increment}
	 *     }}
	 * </pre>
	 * 
	 * @param name key of the field
	 * @param increment increment for each item
	 * @return
	 */
	public GroupOperation count(String name, double increment) {
		return sum(name, increment);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_sum">$sum
	 * operation</a> count every item.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          name: {$sum: 1}
	 *     }}
	 * </pre>
	 * 
	 * @param name key of the field
	 * @return
	 */
	public GroupOperation count(String name) {
		return count(name, 1);
	}

	private GroupOperation sum(String name, Object field) {
		return addOperation("$sum", name, field);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_sum">$sum
	 * operation</a>.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          name: {$sum: "$field"}
	 *     }}
	 * </pre>
	 * 
	 * @param name key of the field
	 * @param field reference to a field of the document
	 * @return
	 */
	public GroupOperation sum(String name, String field) {
		return sum(name, (Object) field);
	}

	/**
	 * Adds a field with the <a href="http://docs.mongodb.org/manual/reference/aggregation/addToSet/#grp._S_sum">$sum
	 * operation</a>.
	 * 
	 * <pre>
	 *     {$group: {
	 *          _id: "$id_field",
	 *          field: {$sum: "$field"}
	 *     }}
	 * </pre>
	 * 
	 * @param field reference to a field of the document
	 * @return
	 */
	public GroupOperation sum(String field) {
		return sum(field, field);
	}

	protected GroupOperation addOperation(String operation, String name, Object field) {

		getOutputAggregateOperationContext().registerAvailableField(name);
		this.ops.add(new GroupingOperation(operation, name, field));
		return this;
	}

	static class GroupingOperation {
		final String operation;
		final String alias;
		final Object fieldNameOrValue;

		public GroupingOperation(String operation, String alias, Object fieldNameOrValue) {
			this.operation = operation;
			this.alias = alias;
			this.fieldNameOrValue = fieldNameOrValue;
		}

		public DBObject toDbObject(AggregateOperationContext inputAggregateOperationContext) {

			Object fieldNameOrValueToUse = fieldNameOrValue;

			if (fieldNameOrValue instanceof String) {
				if (inputAggregateOperationContext != null) {
					fieldNameOrValueToUse = inputAggregateOperationContext
							.returnFieldNameAliasIfAvailableOr((String) fieldNameOrValueToUse);
				}
				fieldNameOrValueToUse = ReferenceUtil.safeReference((String) fieldNameOrValueToUse);
			}

			return new BasicDBObject(operation, fieldNameOrValueToUse);
		}
	}
}
