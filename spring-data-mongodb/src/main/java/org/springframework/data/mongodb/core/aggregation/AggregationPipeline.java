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

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

/**
 * Holds the operations of an aggregation pipeline.
 * 
 * @author Tobias Trelle
 * @since 1.3
 */
public class AggregationPipeline {

	private static final String OPERATOR_PREFIX = "$";

	private final List<DBObject> operations = new ArrayList<DBObject>();

	/**
	 * Adds a projection operation to the pipeline.
	 * 
	 * @param projection JSON string holding the projection, must not be {@literal null} or empty.
	 * @return The pipeline.
	 */
	public AggregationPipeline project(String projection) {
		return addDocumentOperation("project", projection);
	}

	/**
	 * Adds a projection operation to the pipeline.
	 * 
	 * @param projection Type safe projection object, must not be {@literal null}.
	 * @return The pipeline.
	 */
	public AggregationPipeline project(Projection projection) {

		Assert.notNull(projection, "Projection must not be null!");
		return addOperation("project", projection.toDBObject());
	}

	/**
	 * Adds an unwind operation to the pipeline.
	 * 
	 * @param field Name of the field to unwind (should be an array), must not be {@literal null} or empty.
	 * @return The pipeline.
	 */
	public AggregationPipeline unwind(String field) {

		Assert.hasText(field, "Missing field name");

		if (!field.startsWith(OPERATOR_PREFIX)) {
			field = OPERATOR_PREFIX + field;
		}

		return addOperation("unwind", field);
	}

	/**
	 * Adds a group operation to the pipeline.
	 * 
	 * @param group JSON string holding the group, must not be {@literal null} or empty.
	 * @return The pipeline.
	 */
	public AggregationPipeline group(String group) {
		return addDocumentOperation("group", group);
	}

	/**
	 * Adds a sort operation to the pipeline.
	 * 
	 * @param sort JSON string holding the sorting, must not be {@literal null} or empty.
	 * @return The pipeline.
	 */
	public AggregationPipeline sort(String sort) {
		return addDocumentOperation("sort", sort);
	}

	/**
	 * Adds a sort operation to the pipeline.
	 * 
	 * @param sort Type safe sort operation, must not be {@literal null}.
	 * @return The pipeline.
	 */
	public AggregationPipeline sort(Sort sort) {

		Assert.notNull(sort);
		DBObject dbo = new BasicDBObject();

		for (org.springframework.data.domain.Sort.Order order : sort) {
			dbo.put(order.getProperty(), order.isAscending() ? 1 : -1);
		}
		return addOperation("sort", dbo);
	}

	/**
	 * Adds a match operation to the pipeline that is basically a query on the collections.
	 * 
	 * @param match JSON string holding the criteria, must not be {@literal null} or empty.
	 * @return The pipeline.
	 */
	public AggregationPipeline match(String match) {
		return addDocumentOperation("match", match);
	}

	/**
	 * Adds a match operation to the pipeline that is basically a query on the collection.s
	 * 
	 * @param criteria Type safe criteria to filter documents from the collection, must not be {@literal null}.
	 * @return The pipeline.
	 */
	public AggregationPipeline match(Criteria criteria) {

		Assert.notNull(criteria);
		return addOperation("match", criteria.getCriteriaObject());
	}

	/**
	 * Adds an limit operation to the pipeline.
	 * 
	 * @param n Number of document to consider.
	 * @return The pipeline.
	 */
	public AggregationPipeline limit(long n) {
		return addOperation("limit", n);
	}

	/**
	 * Adds an skip operation to the pipeline.
	 * 
	 * @param n Number of documents to skip.
	 * @return The pipeline.
	 */
	public AggregationPipeline skip(long n) {
		return addOperation("skip", n);
	}

	public List<DBObject> getOperations() {
		return operations;
	}

	private AggregationPipeline addDocumentOperation(String opName, String operation) {

		Assert.hasText(operation, "Missing operation name!");
		return addOperation(opName, parseJson(operation));
	}

	private AggregationPipeline addOperation(String key, Object value) {
		this.operations.add(new BasicDBObject(OPERATOR_PREFIX + key, value));
		return this;
	}

	private DBObject parseJson(String json) {
		try {
			return (DBObject) JSON.parse(json);
		} catch (JSONParseException e) {
			throw new IllegalArgumentException("Not a valid JSON document: " + json, e);
		}
	}
}
