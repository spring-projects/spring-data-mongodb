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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.MongoClientSettings;

/**
 * Unit tests for {@link BasicAggregationOperation}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BasicAggregationOperationUnitTests {

	@Mock QueryMapper queryMapper;
	@Mock MongoConverter converter;

	TypeBasedAggregationOperationContext ctx;

	@BeforeEach
	void beforeEach() {

		// no field mapping though having a type based context
		ctx = new TypeBasedAggregationOperationContext(Person.class, new MongoMappingContext(), queryMapper);
		when(queryMapper.getConverter()).thenReturn(converter);
		when(converter.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
	}

	@Test // GH-4038
	void usesGivenDocumentAsIs() {

		Document source = new Document("value", 1);
		assertThat(new BasicAggregationOperation(source).toDocument(ctx)).isSameAs(source);
	}

	@Test // GH-4038
	void parsesJson() {

		Document source = new Document("value", 1);
		assertThat(new BasicAggregationOperation(source.toJson()).toDocument(ctx)).isEqualTo(source);
	}

	@Test // GH-4038
	void errorsOnInvalidValue() {

		BasicAggregationOperation agg = new BasicAggregationOperation(new Object());
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> agg.toDocument(ctx));
	}

	@Test // GH-4038
	void errorsOnNonJsonSting() {

		BasicAggregationOperation agg = new BasicAggregationOperation("#005BBB #FFD500");
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> agg.toDocument(ctx));
	}

	private static class Person {

		@Field("v-a-l-u-e") Object value;
	}
}
