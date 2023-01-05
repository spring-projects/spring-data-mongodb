/*
 * Copyright 2015-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import org.bson.Document;
import org.springframework.data.mongodb.MongoExpression;

/**
 * An {@link AggregationExpression} can be used with field expressions in aggregation pipeline stages like
 * {@code project} and {@code group}.
 * <p>
 * The {@link AggregationExpression expressions} {@link #toDocument(AggregationOperationContext)} method is called during
 * the mapping process to obtain the mapped, ready to use representation that can be handed over to the driver as part
 * of an {@link AggregationOperation pipleine stage}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public interface AggregationExpression extends MongoExpression {

	/**
	 * Create an {@link AggregationExpression} out of a given {@link MongoExpression} to ensure the resulting
	 * {@link MongoExpression#toDocument() Document} is mapped against the {@link AggregationOperationContext}. <br />
	 * If the given expression is already an {@link AggregationExpression} the very same instance is returned.
	 *
	 * @param expression must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 3.2
	 */
	static AggregationExpression from(MongoExpression expression) {

		if (expression instanceof AggregationExpression aggregationExpression) {
			return aggregationExpression;
		}

		return context -> context.getMappedObject(expression.toDocument());
	}

	/**
	 * Obtain the as is (unmapped) representation of the {@link AggregationExpression}. Use
	 * {@link #toDocument(AggregationOperationContext)} with a matching {@link AggregationOperationContext context} to
	 * engage domain type mapping including field name resolution.
	 *
	 * @see org.springframework.data.mongodb.MongoExpression#toDocument()
	 */
	@Override
	default Document toDocument() {
		return toDocument(Aggregation.DEFAULT_CONTEXT);
	}

	/**
	 * Turns the {@link AggregationExpression} into a {@link Document} within the given
	 * {@link AggregationOperationContext}.
	 *
	 * @param context must not be {@literal null}.
	 * @return the MongoDB native ({@link Document}) form of the expression.
	 */
	Document toDocument(AggregationOperationContext context);
}
