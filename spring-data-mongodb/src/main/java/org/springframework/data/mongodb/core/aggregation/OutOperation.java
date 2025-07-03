/*
 * Copyright 2016-2025 the original author or authors.
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
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.CollectionOptions.TimeSeriesOptions;
import org.springframework.data.mongodb.core.timeseries.Granularity;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Encapsulates the {@code $out}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#out(String)} instead of creating instances of this
 * class directly.
 *
 * @author Nikolay Bogdanov
 * @author Christoph Strobl
 * @author Hyunsang Han
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/out/">MongoDB Aggregation Framework:
 *      $out</a>
 */
public class OutOperation implements AggregationOperation {

	private final @Nullable String databaseName;
	private final String collectionName;
	private final @Nullable TimeSeriesOptions timeSeriesOptions;

	/**
	 * @param outCollectionName Collection name to export the results. Must not be {@literal null}.
	 */
	public OutOperation(String outCollectionName) {
		this(null, outCollectionName, null);
	}

	/**
	 * @param databaseName Optional database name the target collection is located in. Can be {@literal null}.
	 * @param collectionName Collection name to export the results. Must not be {@literal null}. Can be {@literal null}.
	 * @param timeSeriesOptions Optional time series options for creating a time series collection. Can be
	 *          {@literal null}.
	 * @since 5.0
	 */
	private OutOperation(@Nullable String databaseName, String collectionName,
			@Nullable TimeSeriesOptions timeSeriesOptions) {

		Assert.notNull(collectionName, "Collection name must not be null");

		this.databaseName = databaseName;
		this.collectionName = collectionName;
		this.timeSeriesOptions = timeSeriesOptions;
	}

	/**
	 * Optionally specify the database of the target collection. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @param database can be {@literal null}. Defaulted to aggregation target database.
	 * @return new instance of {@link OutOperation}.
	 * @since 2.2
	 */
	@Contract("_ -> new")
	public OutOperation in(@Nullable String database) {
		return new OutOperation(database, collectionName, timeSeriesOptions);
	}

	/**
	 * Set the time series options for creating a time series collection.
	 *
	 * @param timeSeriesOptions must not be {@literal null}.
	 * @return new instance of {@link OutOperation}.
	 * @since 5.0
	 */
	@Contract("_ -> new")
	public OutOperation timeSeries(TimeSeriesOptions timeSeriesOptions) {

		Assert.notNull(timeSeriesOptions, "TimeSeriesOptions must not be null");
		return new OutOperation(databaseName, collectionName, timeSeriesOptions);
	}

	/**
	 * Set the time series options for creating a time series collection with only the time field.
	 *
	 * @param timeField must not be {@literal null} or empty.
	 * @return new instance of {@link OutOperation}.
	 * @since 5.0
	 */
	@Contract("_ -> new")
	public OutOperation timeSeries(String timeField) {

		Assert.hasText(timeField, "TimeField must not be null or empty");
		return timeSeries(TimeSeriesOptions.timeSeries(timeField));
	}

	/**
	 * Set the time series options for creating a time series collection with time field, meta field, and granularity.
	 *
	 * @param timeField must not be {@literal null} or empty.
	 * @param metaField can be {@literal null}.
	 * @param granularity defaults to {@link Granularity#DEFAULT} if {@literal null}.
	 * @return new instance of {@link OutOperation}.
	 * @since 5.0
	 */
	@Contract("_, _, _ -> new")
	public OutOperation timeSeries(String timeField, @Nullable String metaField, @Nullable Granularity granularity) {

		Assert.hasText(timeField, "TimeField must not be null or empty");

		TimeSeriesOptions options = TimeSeriesOptions.timeSeries(timeField).metaField(metaField)
				.granularity(granularity != null ? granularity : Granularity.DEFAULT);
		return timeSeries(options);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		if (!StringUtils.hasText(databaseName) && timeSeriesOptions == null) {
			return new Document(getOperator(), collectionName);
		}

		Document outDocument = new Document("coll", collectionName);

		if (StringUtils.hasText(databaseName)) {
			outDocument.put("db", databaseName);
		}

		if (timeSeriesOptions != null) {

			Document timeSeriesDoc = new Document("timeField", timeSeriesOptions.getTimeField());

			if (StringUtils.hasText(timeSeriesOptions.getMetaField())) {
				timeSeriesDoc.put("metaField", timeSeriesOptions.getMetaField());
			}

			if (timeSeriesOptions.getGranularity() != null && timeSeriesOptions.getGranularity() != Granularity.DEFAULT) {
				timeSeriesDoc.put("granularity", timeSeriesOptions.getGranularity().name().toLowerCase());
			}

			if (timeSeriesOptions.getSpan() != null && timeSeriesOptions.getSpan().time() != null) {

				long spanSeconds = timeSeriesOptions.getSpan().time().getSeconds();
				timeSeriesDoc.put("bucketMaxSpanSeconds", spanSeconds);
				timeSeriesDoc.put("bucketRoundingSeconds", spanSeconds);
			}

			outDocument.put("timeseries", timeSeriesDoc);
		}

		return new Document(getOperator(), outDocument);
	}

	@Override
	public String getOperator() {
		return "$out";
	}
}
