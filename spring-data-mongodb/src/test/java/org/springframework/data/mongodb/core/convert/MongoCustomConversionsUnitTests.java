/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;

import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mongodb.core.convert.QueryMapperUnitTests.Foo;

/**
 * Unit tests for {@link MongoCustomConversions}.
 *
 * @author Christoph Strobl
 */
class MongoCustomConversionsUnitTests {

	@Test // DATAMONGO-2349
	void nonAnnotatedConverterForJavaTimeTypeShouldOnlyBeRegisteredAsReadingConverter() {

		MongoCustomConversions conversions = new MongoCustomConversions(
				Collections.singletonList(new DateToZonedDateTimeConverter()));

		assertThat(conversions.hasCustomReadTarget(Date.class, ZonedDateTime.class)).isTrue();
		assertThat(conversions.hasCustomWriteTarget(Date.class)).isFalse();
	}

	@Test // GH-3596
	void propertyValueConverterRegistrationWorksAsExpected() {

		PersistentProperty<?> persistentProperty = mock(PersistentProperty.class);
		PersistentEntity owner = mock(PersistentEntity.class);
		when(persistentProperty.getName()).thenReturn("name");
		when(persistentProperty.getOwner()).thenReturn(owner);
		when(owner.getType()).thenReturn(Foo.class);

		MongoCustomConversions conversions = MongoCustomConversions.create(config -> {

			config.configurePropertyConversions(
					registry -> registry.registerConverter(Foo.class, "name", mock(PropertyValueConverter.class)));
		});

		assertThat(conversions.getPropertyValueConversions().hasValueConverter(persistentProperty)).isTrue();
	}

	@Test // GH-4390
	void doesNotReturnConverterForNativeTimeTimeIfUsingDriverCodec() {

		MongoCustomConversions conversions = MongoCustomConversions.create(config -> {
			config.useNativeDriverJavaTimeCodecs();
		});

		assertThat(conversions.getCustomWriteTarget(Date.class)).isEmpty();
	}

	static class DateToZonedDateTimeConverter implements Converter<Date, ZonedDateTime> {

		@Override
		public ZonedDateTime convert(Date source) {
			return ZonedDateTime.now();
		}
	}
}
