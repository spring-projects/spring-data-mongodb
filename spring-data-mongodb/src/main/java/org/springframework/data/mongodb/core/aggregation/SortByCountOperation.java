/*
 * Copyright 2017-2018 the original author or authors.
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

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Encapsulates the aggregation framework {@code $sortByCount}-operation.
 * <p/>
 * {@code $sortByCount} stage is typically used with {@link Aggregation} and {@code $facet}. Groups incoming documents
 * based on the value of a specified expression and computes the count of documents in each distinct group.
 * {@link SortByCountOperation} is equivalent to {@code { $group: { _id: <expression>, count: { $sum: 1 } } }, { $sort:
 * { count: -1 } }}.
 * <p/>
 * We recommend to use the static factory method {@link Aggregation#sortByCount(String)} instead of creating instances
 * of this class directly.
 *
 * @see <a href=
 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/">https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/</a>
 * @author Jérôme Guyon
 * @author Mark Paluch
 * @since 2.1
 */
public class SortByCountOperation implements AggregationOperation {

	private final @Nullable Field groupByField;
	private final @Nullable AggregationExpression groupByExpression;

	/**
	 * Creates a new {@link SortByCountOperation} given a {@link Field group-by field}.
	 *
	 * @param groupByField must not be {@literal null}.
	 */
	public SortByCountOperation(Field groupByField) {

		Assert.notNull(groupByField, "Group by field must not be null!");

		this.groupByField = groupByField;
		this.groupByExpression = null;
	}

	/**
	 * Creates a new {@link SortByCountOperation} given a {@link AggregationExpression group-by expression}.
	 *
	 * @param groupByExpression must not be {@literal null}.
	 */
	public SortByCountOperation(AggregationExpression groupByExpression) {

		Assert.notNull(groupByExpression, "Group by expression must not be null!");

		this.groupByExpression = groupByExpression;
		this.groupByField = null;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		return new Document("$sortByCount", groupByExpression == null ? context.getReference(groupByField).toString()
				: groupByExpression.toDocument(context));
	}
}
