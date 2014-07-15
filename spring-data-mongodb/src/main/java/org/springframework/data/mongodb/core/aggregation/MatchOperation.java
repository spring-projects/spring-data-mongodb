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

import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the {@code $match}-operation
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
	 * Creates a new {@link MatchOperation} for the given {@link Criteria}.
	 * 
	 * @param criteria must not be {@literal null}.
	 */
	public MatchOperation(Criteria criteria) {
		this((CriteriaDefinition) criteria);
	}

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
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {
		return new BasicDBObject("$match", context.getMappedObject(criteriaDefinition.getCriteriaObject()));
	}
}
