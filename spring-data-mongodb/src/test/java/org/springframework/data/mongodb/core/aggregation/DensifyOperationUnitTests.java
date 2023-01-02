/*
 * Copyright 2022-2023 the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Date;

import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.aggregation.DensifyOperation.DensifyUnits;
import org.springframework.data.mongodb.core.aggregation.DensifyOperation.Range;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link DensifyOperation}.
 *
 * @author Christoph Strobl
 */
class DensifyOperationUnitTests {

	@Test // GH-4139
	void rendersFieldNamesAsIsForUntypedContext() {

		DensifyOperation densify = DensifyOperation.builder().densify("ts")
				.range(Range.bounded("2021-05-18T00:00:00", "2021-05-18T08:00:00").incrementBy(1).unit(DensifyUnits.HOUR))
				.build();

		assertThat(densify.toDocument(contextFor(null))).isEqualTo("""
				{
				   $densify: {
				      field: "ts",
				      range: {
				         step: 1,
				         unit: "hour",
				         bounds:[ "2021-05-18T00:00:00", "2021-05-18T08:00:00" ]
				      }
				   }
				}
				""");
	}

	@Test // GH-4139
	void rendersFieldNamesCorrectly() {

		DensifyOperation densify = DensifyOperation.builder().densify("ts")
				.range(Range.bounded("2021-05-18T00:00:00", "2021-05-18T08:00:00").incrementBy(1).unit(DensifyUnits.HOUR))
				.build();

		assertThat(densify.toDocument(contextFor(Weather.class))).isEqualTo("""
				{
				   $densify: {
				      field: "timestamp",
				      range: {
				         step: 1,
				         unit: "hour",
				         bounds:[ "2021-05-18T00:00:00", "2021-05-18T08:00:00" ]
				      }
				   }
				}
				""");
	}

	@Test // GH-4139
	void rendersPartitonNamesCorrectly() {

		DensifyOperation densify = DensifyOperation.builder().densify("alt").partitionBy("var")
				.fullRange(range -> range.incrementBy(200)).build();

		assertThat(densify.toDocument(contextFor(Coffee.class))).isEqualTo("""
				{
				   $densify: {
				      field: "altitude",
				      partitionByFields : [ "variety" ],
				      range: {
				         step: 200,
				         bounds: "full"
				      }
				   }
				}
				""");
	}

	@Test // GH-4139
	void rendersPartitionRangeCorrectly() {

		DensifyOperation densify = DensifyOperation.builder().densify("alt").partitionBy("var")
				.partitionRange(range -> range.incrementBy(200)).build();

		assertThat(densify.toDocument(contextFor(Coffee.class))).isEqualTo("""
				{
				   $densify: {
				      field: "altitude",
				      partitionByFields : [ "variety" ],
				      range: {
				         step: 200,
				         bounds: "partition"
				      }
				   }
				}
				""");
	}

	private static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return new TypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter));
	}

	class Weather {

		@Field("timestamp") Date ts;

		@Field("temp") Long temperature;
	}

	class Coffee {

		@Field("altitude") Long alt;

		@Field("variety") String var;

		Float score;
	}
}
