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

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $sort}-operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/sort/#pipe._S_sort
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.3
 */
public class SortOperation implements AggregationOperation {

	private final Sort sort;

	/**
	 * Creates a new {@link SortOperation} for the given {@link Sort} instance.
	 * 
	 * @param sort must not be {@literal null}.
	 */
	public SortOperation(Sort sort) {

		Assert.notNull(sort, "Sort must not be null!");
		this.sort = sort;
	}

	public SortOperation and(Direction direction, String... fields) {
		return and(new Sort(direction, fields));
	}

	public SortOperation and(Sort sort) {
		return new SortOperation(this.sort.and(sort));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {

		BasicDBObject object = new BasicDBObject();

		for (Order order : sort) {

			// Check reference
			FieldReference reference = context.getReference(order.getProperty());
			object.put(reference.getRaw(), order.isAscending() ? 1 : -1);
		}

		return new BasicDBObject("$sort", object);
	}
}
