/*
 * Copyright 2013-2025 the original author or authors.
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
import java.util.List;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;
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
	private final @Nullable Long skip;
	private final @Nullable Integer limit;

	/**
	 * Creates a new {@link GeoNearOperation} from the given {@link NearQuery} and the given distance field. The
	 * {@code distanceField} defines output field that contains the calculated distance.
	 *
	 * @param nearQuery must not be {@literal null}.
	 * @param distanceField must not be {@literal null}.
	 */
	public GeoNearOperation(NearQuery nearQuery, String distanceField) {
		this(nearQuery, distanceField, null, nearQuery.getSkip(), null);
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
	private GeoNearOperation(NearQuery nearQuery, String distanceField, @Nullable String indexKey, @Nullable Long skip,
			@Nullable Integer limit) {

		Assert.notNull(nearQuery, "NearQuery must not be null");
		Assert.hasLength(distanceField, "Distance field must not be null or empty");

		this.nearQuery = nearQuery;
		this.distanceField = distanceField;
		this.indexKey = indexKey;
		this.skip = skip;
		this.limit = limit;
	}

	/**
	 * Optionally specify the geospatial index to use via the field to use in the calculation. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @param key the geospatial index field to use when calculating the distance.
	 * @return new instance of {@link GeoNearOperation}.
	 * @since 2.1
	 */
	@Contract("_ -> new")
	public GeoNearOperation useIndex(String key) {
		return new GeoNearOperation(nearQuery, distanceField, key, skip, limit);
	}

	/**
	 * Override potential skip applied via {@link NearQuery#getSkip()}. Adds an additional {@link SkipOperation} if value
	 * is non negative.
	 * 
	 * @param skip
	 * @return new instance of {@link GeoNearOperation}.
	 * @since 5.0
	 */
	public GeoNearOperation skip(long skip) {
		return new GeoNearOperation(nearQuery, distanceField, indexKey, skip, limit);
	}

	/**
	 * Override potential limit value. Adds an additional {@link LimitOperation} if value is non negative.
	 *
	 * @param limit
	 * @return new instance of {@link GeoNearOperation}.
	 * @since 5.0
	 */
	public GeoNearOperation limit(Integer limit) {
		return new GeoNearOperation(nearQuery, distanceField, indexKey, skip, limit);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document command = context.getMappedObject(nearQuery.toDocument());

		if (command.containsKey("query")) {
			Document query = command.get("query", Document.class);
			if (query == null || query.isEmpty()) {
				command.remove("query");
			} else {
				command.replace("query", context.getMappedObject(query));
			}

		}

		command.remove("collation");
		command.put("distanceField", distanceField);

		if (StringUtils.hasText(indexKey)) {
			command.put("key", indexKey);
		}

		return new Document(getOperator(), command);
	}

	@Override
	public String getOperator() {
		return "$geoNear";
	}

	@Override
	public List<Document> toPipelineStages(AggregationOperationContext context) {

		Document command = toDocument(context);
		Number limit = (Number) command.get("$geoNear", Document.class).remove("num");
		if (limit != null && this.limit != null) {
			limit = this.limit;
		}

		List<Document> stages = new ArrayList<>(3);
		stages.add(command);

		if (this.skip != null && this.skip > 0) {
			stages.add(new Document("$skip", this.skip));
		}

		if (limit != null && limit.longValue() > 0) {
			stages.add(new Document("$limit", limit.longValue()));
		}

		return stages;
	}
}
