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
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the {@code $match}-operation
 * 
 * @author Sebastian Herold
 * @since 1.3
 */
public class MatchOperation implements AggregationOperation {

	private final DBObject criteria;

	/**
	 * Creates a new {@link MatchOperation} for the given {@link Criteria}.
	 * 
	 * @param criteria must not be {@literal null}.
	 */
	public MatchOperation(Criteria criteria) {

		Assert.notNull(criteria, "Criteria must not be null!");
		this.criteria = criteria.getCriteriaObject();
	}

	/**
	 * Factory method to create a new {@link MatchOperation} for the given {@link Criteria}.
	 * 
	 * @param criteria must not be {@literal null}.
	 * @return
	 */
	public static MatchOperation match(Criteria criteria) {
		return new MatchOperation(criteria);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#getDBObject()
	 */
	public DBObject getDBObject() {
		return new BasicDBObject("$match", criteria);
	}

}
