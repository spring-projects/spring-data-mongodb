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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

/**
 * Test case to verify correct usage of custom {@link Converter} implementations for different List implementations to be used.
 *
 * @author Jürgen Diez
 */
@ExtendWith(MockitoExtension.class)
class CustomListReadingConvertersUnitTests {

	private MappingMongoConverter converter;

	@Mock JavaListReadingConverter javaListReadingConverter;
	@Mock OtherListReadingConverter otherListReadingConverter;
	@Captor ArgumentCaptor<List<TestEnum>> enumListCaptor;

	private MongoMappingContext context;

	@BeforeEach
	void setUp() {
		CustomConversions conversions = new MongoCustomConversions(
				Arrays.asList(javaListReadingConverter, otherListReadingConverter));

		context = new MongoMappingContext();
		context.setInitialEntitySet(new HashSet<>(Collections.singletonList(TestJavaList.class)));
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.initialize();

		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();
	}

	@Test
	void invokeCustomListConverterForEnumsInJavaListAfterResolvingTheListTypes() {
		Document document = new Document();
		document.append("list", Arrays.asList("ENUM_VALUE1", "ENUM_VALUE2"));

		converter.read(TestJavaList.class, document);

		verify(javaListReadingConverter).convert(enumListCaptor.capture());
		assertThat(enumListCaptor.getValue()).containsExactly(TestEnum.ENUM_VALUE1, TestEnum.ENUM_VALUE2);
	}

	@Test
	void invokeCustomListConverterForEnumsInIterableListAfterResolvingTheListTypes() {
		Document document = new Document();
		document.append("list", Arrays.asList("ENUM_VALUE1", "ENUM_VALUE2"));

		converter.read(TestIterableList.class, document);

		verify(otherListReadingConverter).convert(enumListCaptor.capture());
		assertThat(enumListCaptor.getValue()).containsExactly(TestEnum.ENUM_VALUE1, TestEnum.ENUM_VALUE2);
	}


	@ReadingConverter
	private interface JavaListReadingConverter extends Converter<List<?>, List<?>> {}

	@ReadingConverter
	private interface OtherListReadingConverter extends Converter<List<?>, Iterable<?>> {}

	private enum TestEnum {
		ENUM_VALUE1,
		ENUM_VALUE2
	}

	private static class TestJavaList {
		@SuppressWarnings("unused")
		List<TestEnum> list;
	}

	private static class TestIterableList {
		@SuppressWarnings("unused")
		Iterable<TestEnum> list;
	}

}
