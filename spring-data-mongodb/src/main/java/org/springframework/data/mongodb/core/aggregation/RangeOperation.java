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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $limit} and {@code $skip} operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/limit/
 * @see http://docs.mongodb.org/manual/reference/aggregation/skip/
 * @author Sebastian Herold
 * @since 1.3
 */
public class RangeOperation implements AggregationOperation {

	private final String name;
	private final long value;

	/**
	 * Creates a simple {@link RangeOperation} consisting of operation name and an integer value.
	 * 
	 * @param name name of the operation with or without leading operation prefix
	 * @param value long value
	 */
	public RangeOperation(String name, long value) {
		this.name = name;
		this.value = value;
	}

	public DBObject getDBObject() {
		return new BasicDBObject(ReferenceUtil.safeReference(name), value);
	}

	/**
	 * Creates a {@code $limit} operation with a given value. This limitates the result stream of the pipeline to the
	 * given amount of entries as described <a
	 * href="http://docs.mongodb.org/manual/reference/aggregation/limit/">here</a>.
	 * 
	 * @param value
	 * @return the pipeline operation
	 * 
	 * @see http://docs.mongodb.org/manual/reference/aggregation/limit/
	 */
	public static RangeOperation limit(long value) {
		return new RangeOperation("limit", value);
	}

	/**
	 * Creates a {@code $skip} operation with a given value. This skips the first x entries of the result stream as
	 * described <a href="http://docs.mongodb.org/manual/reference/aggregation/skip/">here</a>.
	 * 
	 * @param value
	 * @return the pipeline operation
	 * 
	 * @see http://docs.mongodb.org/manual/reference/aggregation/skip/
	 */
	public static RangeOperation skip(long value) {
		return new RangeOperation("skip", value);
	}

}
