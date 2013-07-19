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
import java.util.Collections;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.query.Field;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $project}-operation.
 * <p>
 * Projection of field to be used in an {@link Aggregation}. A projection is similar to a {@link Field}
 * inclusion/exclusion but more powerful. It can generate new fields, change values of given field etc.
 * <p>
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/project/
 * @author Tobias Trelle
 * @author Thomas Darimont
 * @since 1.3
 */
public class ProjectionOperation extends AbstractContextProducingAggregateOperation {

	/** Stack of key names. Size is 0 or 1. */
	private final Stack<String> reference = new Stack<String>();
	private final Map<String, Object> projection = new HashMap<String, Object>();

	private DBObject rightHandExpression;

	/**
	 * This convenience constructor excludes the field {@code _id} and includes the given fields.
	 * 
	 * @param includes Keys of field to include, must not be {@literal null} or empty.
	 */
	public ProjectionOperation(String... includes) {

		super("project");

		Assert.notNull(includes, "includes must not be null");
		exclude("_id");

		for (String key : includes) {
			include(key);
		}
	}

	/**
	 * Create an empty projection.
	 * 
	 * @param targetClass
	 */
	public ProjectionOperation(Class<?> targetClass) {
		this(extractFieldsFrom(targetClass));
	}

	/**
	 * @param targetClass
	 * @return
	 */
	private static String[] extractFieldsFrom(Class<?> targetClass) {
		return new String[0];
	}

	/**
	 * Excludes a given field.
	 * 
	 * @param key The key of the field.
	 */
	public final ProjectionOperation exclude(String key) {

		Assert.hasText(key, "Missing key");
		getOutputAggregateOperationContext().unregisterAvailableField(ReferenceUtil.safeNonReference(key));

		projection.put(key, 0);
		return this;
	}

	/**
	 * Includes a given field.
	 * 
	 * @param key The key of the field, must not be {@literal null} or empty.
	 */
	public final ProjectionOperation include(String key) {

		Assert.hasText(key, "Missing key");

		safePop();
		reference.push(key);
		getOutputAggregateOperationContext().registerAvailableField(key);

		return this;
	}

	/**
	 * Sets the key for a computed field.
	 * 
	 * @param key must not be {@literal null} or empty.
	 */
	public final ProjectionOperation as(String key) {

		Assert.hasText(key, "Missing key");

		try {
			String rhsFieldName = reference.pop();
			getOutputAggregateOperationContext().unregisterAvailableField(ReferenceUtil.safeNonReference(rhsFieldName));
			getOutputAggregateOperationContext().registerAvailableField(ReferenceUtil.safeNonReference(key));
			projection.put(key, rightHandSide(ReferenceUtil.safeReference(rhsFieldName)));
		} catch (EmptyStackException e) {
			throw new InvalidDataAccessApiUsageException("Invalid use of as()", e);
		}

		return this;
	}

	/**
	 * Sets the key for a computed field.
	 * 
	 * @param key must not be {@literal null} or empty.
	 */
	public final ProjectionOperation asSelf() {

		try {
			String selfRef = reference.pop();
			projection.put(selfRef, rightHandSide(ReferenceUtil.safeReference(selfRef)));
		} catch (EmptyStackException e) {
			throw new InvalidDataAccessApiUsageException("Invalid use of as()", e);
		}

		return this;
	}

	public final ProjectionOperation plus(Number n) {
		return arithmeticOperation("add", n);
	}

	public final ProjectionOperation minus(Number n) {
		return arithmeticOperation("substract", n);
	}

	private ProjectionOperation arithmeticOperation(String op, Number n) {

		Assert.notNull(n, "Missing number");
		rightHandExpression = createArrayObject(op, ReferenceUtil.safeReference(reference.peek()), n);
		return this;
	}

	private DBObject createArrayObject(String op, Object... items) {

		List<Object> list = new ArrayList<Object>();
		Collections.addAll(list, items);

		return new BasicDBObject(ReferenceUtil.safeReference(op), list);
	}

	private void safePop() {

		if (!reference.empty()) {
			projection.put(reference.pop(), rightHandSide(1));
		}
	}

	private Object rightHandSide(Object defaultValue) {
		Object value = rightHandExpression != null ? rightHandExpression : defaultValue;
		rightHandExpression = null;
		return value;
	}

	/**
	 * @param string
	 * @param projection
	 * @return
	 */
	public ProjectionOperation addField(String key, Object value) {

		Assert.notNull(key, "Missing Key");
		Assert.notNull(value);

		getOutputAggregateOperationContext().registerAvailableField(key);
		registerAvailableFieldsRecursive(key, value);
		this.projection.put(key, value);

		return this;
	}

	private void registerAvailableFieldsRecursive(String outerKey, Object value) {

		if (value instanceof Fields) {
			Map<String, Object> values = ((Fields) value).getValues();
			for (String key : values.keySet()) {
				String innerKey = outerKey + "." + key;
				getOutputAggregateOperationContext().registerAvailableField(innerKey);
				registerAvailableFieldsRecursive(innerKey, values.get(key));
			}
		}
	}

	/**
	 * @param name
	 * @param value
	 * @return
	 */
	public ProjectionOperation field(String name, Object value) {
		addField(name, value);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AbstractAggregateOperation#getOperationArgument()
	 */
	@Override
	public Object getOperationArgument(AggregateOperationContext inputAggregateOperationContext) {

		Assert.notNull(inputAggregateOperationContext, "inputAggregateOperationContext must not be null");
		safePop();

		DBObject projectionObject = new BasicDBObject();
		for (Map.Entry<String, Object> entry : projection.entrySet()) {
			Object fieldNameOrValueToUse = entry.getValue();

			DBObject fieldsObject = returnIfValueIsIdFields(inputAggregateOperationContext, fieldNameOrValueToUse);
			if (fieldsObject != null) {
				projectionObject.put(entry.getKey(), fieldsObject != null ? fieldsObject : fieldNameOrValueToUse);
				continue;
			}

			if (fieldNameOrValueToUse instanceof String) {
				String fieldName = inputAggregateOperationContext
						.returnFieldNameAliasIfAvailableOr((String) fieldNameOrValueToUse);
				fieldNameOrValueToUse = ReferenceUtil.safeReference(fieldName);
			}

			projectionObject.put(entry.getKey(), fieldNameOrValueToUse);
		}

		return projectionObject;
	}

	private DBObject returnIfValueIsIdFields(AggregateOperationContext inputAggregateOperationContext,
			Object fieldNameOrValueToUse) {

		Assert.notNull(inputAggregateOperationContext, "inputAggregateOperationContext must not be null");

		if (!(fieldNameOrValueToUse instanceof Fields)) {
			return null;
		}

		DBObject fieldsObject = new BasicDBObject();
		for (Map.Entry<String, Object> fieldsEntry : ((Fields) fieldNameOrValueToUse).getValues().entrySet()) {

			Object fieldsEntryFieldNameOrValueToUse = fieldsEntry.getValue();
			if (fieldsEntryFieldNameOrValueToUse instanceof String && inputAggregateOperationContext != null) {
				String fieldName = inputAggregateOperationContext
						.returnFieldNameAliasIfAvailableOr((String) fieldsEntryFieldNameOrValueToUse);
				fieldsEntryFieldNameOrValueToUse = ReferenceUtil.safeReference(fieldName);
			}
			fieldsObject.put(fieldsEntry.getKey(), fieldsEntryFieldNameOrValueToUse);
		}
		return fieldsObject;
	}
}
