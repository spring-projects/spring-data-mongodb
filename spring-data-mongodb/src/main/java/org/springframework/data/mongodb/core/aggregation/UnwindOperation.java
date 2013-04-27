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
 * Encapsulates the aggregation framework {@code $unwind} operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/unwind/
 * @author Sebastian Herold
 * @since 1.3
 */
public class UnwindOperation implements AggregationOperation {

	private final String field;

	/**
	 * Creates a simple {@link UnwindOperation}.
	 * 
	 * @param field reference to a field to unwind.
	 */
	public UnwindOperation(String field) {
		this.field = field;
	}

	public DBObject getDBObject() {
		return new BasicDBObject("$unwind", ReferenceUtil.safeReference(field));
	}

	/**
	 * Creates an {@code $unwind} operation for the given field as described <a
	 * href="http://docs.mongodb.org/manual/reference/aggregation/unwind/">here</a>.
	 * 
	 * @param field reference to a field to unwind.
	 * @return the pipeline operation
	 * 
	 * @see http://docs.mongodb.org/manual/reference/aggregation/unwind/
	 */
	public static UnwindOperation unwind(String field) {
		return new UnwindOperation(field);
	}
}
