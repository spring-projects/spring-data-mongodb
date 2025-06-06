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

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.CollectionOptions.TimeSeriesOptions;
import org.springframework.data.mongodb.core.timeseries.Granularity;

/**
 * Unit tests for {@link OutOperation}.
 *
 * @author Nikolay Bogdanov
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Hyunsang Han
 */
class OutOperationUnitTest {

	@Test // DATAMONGO-1418
	void shouldCheckNPEInCreation() {
		assertThatIllegalArgumentException().isThrownBy(() -> new OutOperation(null));
	}

	@Test // DATAMONGO-2259
	void shouldUsePreMongoDB42FormatWhenOnlyCollectionIsPresent() {
		assertThat(out("out-col").toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(new Document("$out", "out-col"));
	}

	@Test // DATAMONGO-2259, GH-4969
	void shouldRenderDocument() {

		assertThat(out("out-col").in("database-2").toDocument(Aggregation.DEFAULT_CONTEXT))
				.containsEntry("$out.coll", "out-col") //
				.containsEntry("$out.db", "database-2");
	}

	@Test // GH-4985
	void shouldRenderTimeSeriesCollectionWithTimeFieldOnly() {

		Document result = out("timeseries-col").timeSeries("timestamp").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$out.coll", "timeseries-col");
		assertThat(result).containsEntry("$out.timeseries.timeField", "timestamp");
		assertThat(result).doesNotContainKey("$out.timeseries.metaField");
		assertThat(result).doesNotContainKey("$out.timeseries.granularity");
	}

	@Test // GH-4985
	void shouldRenderTimeSeriesCollectionWithAllOptions() {

		Document result = out("timeseries-col").timeSeries("timestamp", "metadata", Granularity.SECONDS)
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$out.coll", "timeseries-col");
		assertThat(result).containsEntry("$out.timeseries.timeField", "timestamp");
		assertThat(result).containsEntry("$out.timeseries.metaField", "metadata");
		assertThat(result).containsEntry("$out.timeseries.granularity", "seconds");
	}

	@Test // GH-4985
	void shouldRenderTimeSeriesCollectionWithDatabaseAndAllOptions() {

		Document result = out("timeseries-col").in("test-db").timeSeries("timestamp", "metadata", Granularity.MINUTES)
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$out.coll", "timeseries-col");
		assertThat(result).containsEntry("$out.db", "test-db");
		assertThat(result).containsEntry("$out.timeseries.timeField", "timestamp");
		assertThat(result).containsEntry("$out.timeseries.metaField", "metadata");
		assertThat(result).containsEntry("$out.timeseries.granularity", "minutes");
	}

	@Test // GH-4985
	void shouldRenderTimeSeriesCollectionWithTimeSeriesOptions() {

		TimeSeriesOptions options = TimeSeriesOptions.timeSeries("timestamp").metaField("metadata").granularity(Granularity.HOURS);
		Document result = out("timeseries-col").timeSeries(options).toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$out.coll", "timeseries-col");
		assertThat(result).containsEntry("$out.timeseries.timeField", "timestamp");
		assertThat(result).containsEntry("$out.timeseries.metaField", "metadata");
		assertThat(result).containsEntry("$out.timeseries.granularity", "hours");
	}

	@Test // GH-4985
	void shouldRenderTimeSeriesCollectionWithPartialOptions() {

		Document result = out("timeseries-col").timeSeries("timestamp", "metadata", null)
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$out.coll", "timeseries-col");
		assertThat(result).containsEntry("$out.timeseries.timeField", "timestamp");
		assertThat(result).containsEntry("$out.timeseries.metaField", "metadata");
		assertThat(result).doesNotContainKey("$out.timeseries.granularity");
	}

	@Test // GH-4985
	void outWithTimeSeriesOptionsShouldRenderCorrectly() {

		TimeSeriesOptions options = TimeSeriesOptions.timeSeries("timestamp").metaField("metadata").granularity(Granularity.SECONDS);
		Document result = Aggregation.out("timeseries-col", options).toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$out.coll", "timeseries-col");
		assertThat(result).containsEntry("$out.timeseries.timeField", "timestamp");
		assertThat(result).containsEntry("$out.timeseries.metaField", "metadata");
		assertThat(result).containsEntry("$out.timeseries.granularity", "seconds");
	}

	@Test // GH-4985
	void outWithTimeFieldOnlyShouldRenderCorrectly() {

		Document result = Aggregation.out("timeseries-col", "timestamp").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$out.coll", "timeseries-col");
		assertThat(result).containsEntry("$out.timeseries.timeField", "timestamp");
		assertThat(result).doesNotContainKey("$out.timeseries.metaField");
		assertThat(result).doesNotContainKey("$out.timeseries.granularity");
	}

	@Test // GH-4985
	void outWithAllOptionsShouldRenderCorrectly() {

		Document result = Aggregation.out("timeseries-col", "timestamp", "metadata", Granularity.MINUTES)
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$out.coll", "timeseries-col");
		assertThat(result).containsEntry("$out.timeseries.timeField", "timestamp");
		assertThat(result).containsEntry("$out.timeseries.metaField", "metadata");
		assertThat(result).containsEntry("$out.timeseries.granularity", "minutes");
	}

}
