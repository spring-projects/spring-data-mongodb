/*
 * Copyright 2019-2020 the original author or authors.
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

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;

import org.junit.Test;
import org.springframework.core.convert.converter.Converter;

/**
 * Unit tests for {@link MongoCustomConversions}.
 *
 * @author Christoph Strobl
 */
public class MongoCustomConversionsUnitTests {

	@Test // DATAMONGO-2349
	public void nonAnnotatedConverterForJavaTimeTypeShouldOnlyBeRegisteredAsReadingConverter() {

		MongoCustomConversions conversions = new MongoCustomConversions(
				Collections.singletonList(new DateToZonedDateTimeConverter()));

		assertThat(conversions.hasCustomReadTarget(Date.class, ZonedDateTime.class)).isTrue();
		assertThat(conversions.hasCustomWriteTarget(Date.class)).isFalse();
	}

	static class DateToZonedDateTimeConverter implements Converter<Date, ZonedDateTime> {

		@Override
		public ZonedDateTime convert(Date source) {
			return ZonedDateTime.now();
		}
	}
}
