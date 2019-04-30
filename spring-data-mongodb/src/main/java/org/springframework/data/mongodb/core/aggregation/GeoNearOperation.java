/*
 * Copyright 2013-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * Represents a {@code geoNear} aggregation operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#geoNear(NearQuery, String)} instead of creating
 * instances of this class directly.
 *
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @since 1.3
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/geoNear/">MongoDB Aggregation Framework:
 *      $geoNear</a>
 */
public class GeoNearOperation implements AggregationOperation {

	private final NearQuery nearQuery;
	private final String distanceField;
	private final @Nullable String indexKey;

	/**
	 * Creates a new {@link GeoNearOperation} from the given {@link NearQuery} and the given distance field. The
	 * {@code distanceField} defines output field that contains the calculated distance.
	 *
	 * @param nearQuery must not be {@literal null}.
	 * @param distanceField must not be {@literal null}.
	 */
	public GeoNearOperation(NearQuery nearQuery, String distanceField) {
		this(nearQuery, distanceField, null);
	}

	/**
	 * Creates a new {@link GeoNearOperation} from the given {@link NearQuery} and the given distance field. The
	 * {@code distanceField} defines output field that contains the calculated distance.
	 *
	 * @param nearQuery must not be {@literal null}.
	 * @param distanceField must not be {@literal null}.
	 * @param indexKey can be {@literal null};
	 * @since 2.1
	 */
	private GeoNearOperation(NearQuery nearQuery, String distanceField, @Nullable String indexKey) {

		Assert.notNull(nearQuery, "NearQuery must not be null.");
		Assert.hasLength(distanceField, "Distance field must not be null or empty.");

		this.nearQuery = nearQuery;
		this.distanceField = distanceField;
		this.indexKey = indexKey;
	}

	/**
	 * Optionally specify the geospatial index to use via the field to use in the calculation. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @param key the geospatial index field to use when calculating the distance.
	 * @return new instance of {@link GeoNearOperation}.
	 * @since 2.1
	 */
	public GeoNearOperation useIndex(String key) {
		return new GeoNearOperation(nearQuery, distanceField, key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document command = context.getMappedObject(nearQuery.toDocument());

		if(command.containsKey("query")) {
			command.replace("query", context.getMappedObject(command.get("query", Document.class)));
		}

		if(command.containsKey("collation")) {
			command.remove("collation");
		}

		command.put("distanceField", distanceField);

		if (StringUtils.hasText(indexKey)) {
			command.put("key", indexKey);
		}

		return new Document("$geoNear", command);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toPipelineStages(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public List<Document> toPipelineStages(AggregationOperationContext context) {

		Document command = toDocument(context);
		Number limit = (Number) command.get("$geoNear", Document.class).remove("num");

		List<Document> stages = new ArrayList<>();
		stages.add(command);

		if(nearQuery.getSkip() != null && nearQuery.getSkip() > 0){
			stages.add(new Document("$skip", nearQuery.getSkip()));
		}

		if(limit != null) {
			stages.add(new Document("$limit", limit.longValue()));
		}

		return stages;
	}
}
