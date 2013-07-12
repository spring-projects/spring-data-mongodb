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

/**
 * @author Thomas Darimont
 */
public class GeoNearOperation extends AbstractAggregateOperation {

	private final NearQuery nearQuery;

	public GeoNearOperation(NearQuery nearQuery) {
		super("geoNear");
		Assert.notNull(nearQuery);
		this.nearQuery = nearQuery;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AbstractAggregateOperation#getOperationArgument()
	 */
	@Override
	public Object getOperationArgument() {
		return nearQuery.toDBObject();
	}

}
