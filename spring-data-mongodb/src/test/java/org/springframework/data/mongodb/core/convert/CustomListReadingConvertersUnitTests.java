/*
 * Copyright 2011-2021 the original author or authors.
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

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

/**
 * Test case to verify correct usage of custom {@link Converter} implementations for Lists to be used.
 *
 * @author Jürgen Diez
 */
@ExtendWith(MockitoExtension.class)
class CustomListReadingConvertersUnitTests {

	private MappingMongoConverter converter;

	@Mock ListReadingConverter listReadingConverter;
	@Captor ArgumentCaptor<ArrayList<TestEnum>> enumListCaptor;

	private MongoMappingContext context;

	@BeforeEach
	void setUp() {
		CustomConversions conversions = new MongoCustomConversions(
				Collections.singletonList(listReadingConverter));

		context = new MongoMappingContext();
		context.setInitialEntitySet(new HashSet<>(Collections.singletonList(TestList.class)));
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.initialize();

		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();
	}

	@Test
	void invokeCustomListConverterForEnumsAfterResolvingTheListTypes() {
		Document document = new Document();
		document.append("list", Arrays.asList("ENUM_VALUE1", "ENUM_VALUE2"));

		converter.read(TestList.class, document);

		verify(listReadingConverter).convert(enumListCaptor.capture());
		assertThat(enumListCaptor.getValue()).containsExactly(TestEnum.ENUM_VALUE1, TestEnum.ENUM_VALUE2);
	}


	@ReadingConverter
	private interface ListReadingConverter extends Converter<ArrayList<?>, List<?>> {}

	private static class TestList {
		@SuppressWarnings("unused")
		List<TestEnum> list;
	}

	private enum TestEnum {
		ENUM_VALUE1,
		ENUM_VALUE2
	}
}
