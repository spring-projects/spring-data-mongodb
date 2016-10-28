/*
 * Copyright 2013-2016 the original author or authors.
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
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.util.Assert;

/**
 * Represents a {@code geoNear} aggregation operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#geoNear(NearQuery, String)} instead of creating
 * instances of this class directly.
 * 
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @since 1.3
 */
public class GeoNearOperation implements AggregationOperation {

	private final NearQuery nearQuery;
	private final String distanceField;

	/**
	 * Creates a new {@link GeoNearOperation} from the given {@link NearQuery} and the given distance field. The
	 * {@code distanceField} defines output field that contains the calculated distance.
	 * 
	 * @param query must not be {@literal null}.
	 * @param distanceField must not be {@literal null}.
	 */
	public GeoNearOperation(NearQuery nearQuery, String distanceField) {

		Assert.notNull(nearQuery, "NearQuery must not be null.");
		Assert.hasLength(distanceField, "Distance field must not be null or empty.");

		this.nearQuery = nearQuery;
		this.distanceField = distanceField;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document command = context.getMappedObject(nearQuery.toDocument());
		command.put("distanceField", distanceField);

		return new Document("$geoNear", command);
	}
}
