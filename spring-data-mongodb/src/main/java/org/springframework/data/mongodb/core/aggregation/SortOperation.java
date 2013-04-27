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

import java.util.LinkedHashMap;

import org.springframework.util.Assert;

import com.google.common.base.Strings;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $sort} operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/sort/
 * @author Sebastian Herold
 * @since 1.3
 */
public class SortOperation implements AggregationOperation {

	private final LinkedHashMap<String, Integer> fields = new LinkedHashMap<String, Integer>();

	public DBObject getDBObject() {
		Assert.isTrue(!fields.isEmpty(), "You need at least one sort field");
		return new BasicDBObject("$sort", new BasicDBObject(fields));
	}

	/**
	 * Creates an {@code $sort} operation as described <a
	 * href="http://docs.mongodb.org/manual/reference/aggregation/sort/">here</a>.
	 * 
	 * Make sure that you add at least one sort field via {@link #field(String, SortOrder)}, {@link #asc(Strings)} or
	 * {@link #desc(String)}.
	 * 
	 * @return the pipeline operation
	 * 
	 * @see http://docs.mongodb.org/manual/reference/aggregation/sort/
	 */
	public static SortOperation sort() {
		return new SortOperation();
	}

	/**
	 * Adds a sorting for the given field name in the given order.
	 * 
	 * @param field name/dotted path of the field
	 * @param order sort order
	 * @return the sort operation
	 */
	public SortOperation field(String field, SortOrder order) {
		Assert.hasText(field, "Field name is empty.");
		Assert.notNull(order, "Order is not set.");

		fields.put(field, order.getValue());
		return this;
	}

	/**
	 * Adds a ascending sorting for the given field name.
	 * 
	 * @param field name/dotted path of the field
	 * @return the sort operation
	 */
	public SortOperation asc(String field) {
		return field(field, SortOrder.ASCENDING);
	}

	/**
	 * Adds a descending sorting for the given field name.
	 * 
	 * @param field name/dotted path of the field
	 * @return the sort operation
	 */
	public SortOperation desc(String field) {
		return field(field, SortOrder.DESCENDING);
	}

	/**
	 * Described the MongoDB's sort order.
	 */
	public static enum SortOrder {
		ASCENDING(1), DESCENDING(-1);

		private final int value;

		private SortOrder(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}
}
