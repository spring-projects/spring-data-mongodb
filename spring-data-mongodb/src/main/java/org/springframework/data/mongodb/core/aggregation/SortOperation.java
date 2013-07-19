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
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $sort}-operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/sort/#pipe._S_sort
 * @author Thomas Darimont
 */
public class SortOperation extends AbstractContextAwareAggregateOperation implements ContextConsumingAggregateOperation {

	private Sort sort;

	/**
	 * @param sort
	 */
	public SortOperation(Sort sort) {
		super("sort");

		Assert.notNull(sort);
		this.sort = sort;
	}

	public SortOperation and(Sort sort) {
		return new SortOperation(this.sort.and(sort));
	}

	public SortOperation and(Sort.Direction direction, String... fields) {
		return and(new Sort(direction, fields));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AbstractAggregateOperation#getOperationArgument()
	 */
	@Override
	public Object getOperationArgument(AggregateOperationContext inputAggregateOperationContext) {

		Assert.notNull(inputAggregateOperationContext, "inputAggregateOperationContext must not be null!");

		DBObject sortProperties = new BasicDBObject();
		for (org.springframework.data.domain.Sort.Order order : sort) {
			String fieldName = inputAggregateOperationContext.returnFieldNameAliasIfAvailableOr(order.getProperty());
			sortProperties.put(fieldName, order.isAscending() ? 1 : -1);
		}
		return sortProperties;
	}
}
