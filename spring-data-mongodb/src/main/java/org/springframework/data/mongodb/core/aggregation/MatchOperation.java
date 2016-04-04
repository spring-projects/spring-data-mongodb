/*
 * Copyright 2013-2015 the original author or authors.
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
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.util.Assert;

/**
 * Encapsulates the {@code $match}-operation.
 * <p>
 * We recommend to use the static factory method
 * {@link Aggregation#match(org.springframework.data.mongodb.core.query.Criteria)} instead of creating instances of this
 * class directly.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/match/
 * @author Sebastian Herold
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.3
 */
public class MatchOperation implements AggregationOperation {

	private final CriteriaDefinition criteriaDefinition;

	/**
	 * Creates a new {@link MatchOperation} for the given {@link CriteriaDefinition}.
	 * 
	 * @param criteriaDefinition must not be {@literal null}.
	 */
	public MatchOperation(CriteriaDefinition criteriaDefinition) {

		Assert.notNull(criteriaDefinition, "Criteria must not be null!");
		this.criteriaDefinition = criteriaDefinition;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {
		return new Document("$match", context.getMappedObject(criteriaDefinition.getCriteriaObject()));
	}
}
