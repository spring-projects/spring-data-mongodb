/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.TimeSeries;
import org.springframework.data.mongodb.test.util.MongoTestMappingContext;

/**
 * Unit tests for {@link EntityOperations}.
 *
 * @author Mark Paluch
 */
class EntityOperationsUnitTests {

	ConversionService conversionService = new DefaultConversionService();

	EntityOperations operations = new EntityOperations(
			new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, MongoTestMappingContext.newTestContext()));

	@Test // GH-3731
	void shouldReportInvalidTimeField() {
		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> operations.forType(InvalidTimeField.class).getCollectionOptions())
				.withMessageContaining("Time series field 'foo' does not exist");
	}

	@Test // GH-3731
	void shouldReportInvalidMetaField() {
		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> operations.forType(InvalidMetaField.class).getCollectionOptions())
				.withMessageContaining("Meta field 'foo' does not exist");
	}

	@Test // DATAMONGO-2293
	void populateIdShouldReturnTargetBeanWhenIdIsNull() {
		assertThat(initAdaptibleEntity(new DomainTypeWithIdProperty()).populateIdIfNecessary(null)).isNotNull();
	}

	<T> EntityOperations.AdaptibleEntity<T> initAdaptibleEntity(T source) {
		return operations.forEntity(source, conversionService);
	}

	private static class DomainTypeWithIdProperty {

		@Id String id;
		String value;
	}

	@TimeSeries(timeField = "foo")
	static class InvalidTimeField {

	}

	@TimeSeries(timeField = "time", metaField = "foo")
	static class InvalidMetaField {
		Instant time;
	}
}
