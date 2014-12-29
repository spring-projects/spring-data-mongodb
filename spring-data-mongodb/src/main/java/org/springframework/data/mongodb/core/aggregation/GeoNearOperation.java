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

import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Thomas Darimont
 * @since 1.3
 */
public class GeoNearOperation implements AggregationOperation {

	private final NearQuery nearQuery;

	public GeoNearOperation(NearQuery nearQuery) {

		Assert.notNull(nearQuery);
		Assert.notNull(nearQuery.getDistanceField(), "distanceField must be configured in NearQuery.");

		this.nearQuery = nearQuery;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {
		return new BasicDBObject("$geoNear", context.getMappedObject(nearQuery.toDBObject()));
	}
}
