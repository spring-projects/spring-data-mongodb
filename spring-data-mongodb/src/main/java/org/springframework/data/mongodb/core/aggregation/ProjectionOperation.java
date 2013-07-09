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
import java.util.List;
import java.util.Stack;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.query.Field;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Projection of field to be used in an {@link Aggregation}.
 * <p>
 * A projection is similar to a {@link Field} inclusion/exclusion but more powerful. It can generate new fields, change
 * values of given field etc.
 * <p>
 * 
 * @author Tobias Trelle
 * @author Thomas Darimont
 * @since 1.3
 */
public class ProjectionOperation extends AbstractAggregateOperation {

	/** Stack of key names. Size is 0 or 1. */
	private final Stack<String> reference = new Stack<String>();
	private final DBObject projection = new BasicDBObject();

	private DBObject rightHandExpression;

	/**
	 * Create an empty projection.
	 */
	public ProjectionOperation() {
		super("project");
	}

	/**
	 * This convenience constructor excludes the field {@code _id} and includes the given fields.
	 * 
	 * @param includes Keys of field to include, must not be {@literal null} or empty.
	 */
	public ProjectionOperation(String... includes) {
		this();

		Assert.notEmpty(includes);
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
		this();
	}

	/**
	 * Excludes a given field.
	 * 
	 * @param key The key of the field.
	 */
	public final ProjectionOperation exclude(String key) {

		Assert.hasText(key, "Missing key");
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
			projection.put(key, rightHandSide(ReferenceUtil.safeReference(reference.pop())));
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
		this.projection.put(key, value instanceof Fields ? ((Fields) value).getValues() : value);
		return this;
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
	public Object getOperationArgument() {

		safePop();
		return projection;
	}
}
