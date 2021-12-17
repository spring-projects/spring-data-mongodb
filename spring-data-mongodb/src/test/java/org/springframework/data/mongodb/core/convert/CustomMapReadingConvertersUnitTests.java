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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.ConversionException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

/**
 * Test case to verify correct usage of custom {@link Converter} implementations for different Map implementations to be used.
 *
 * @author Jürgen Diez
 */
@ExtendWith(MockitoExtension.class)
class CustomMapReadingConvertersUnitTests {

	private MappingMongoConverter converter;

	@Mock JavaMapReadingConverter javaMapReadingConverter;
	@Mock ExtendedMapReadingConverter extendedMapReadingConverter;
	@Mock VavrMapReadingConverter vavrMapReadingConverter;
	@Captor ArgumentCaptor<Map<String, TestEnum>> enumMapCaptor;

	private Document source;

	@BeforeEach
	void setUp() {
		CustomConversions conversions = new MongoCustomConversions(
				Arrays.asList(javaMapReadingConverter, extendedMapReadingConverter, vavrMapReadingConverter));

		MongoMappingContext context = new MongoMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.initialize();

		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();

		source = new Document();
		Document map = new Document();
		map.append("key1", "ENUM_VALUE1");
		map.append("key2", "ENUM_VALUE2");
		source.append("map", map);
	}

	@Test
	void invokeJavaMapConverterForEnumsAfterResolvingTheMapTypes() {

		converter.read(TestJavaMap.class, source);

		verify(javaMapReadingConverter).convert(enumMapCaptor.capture());
		assertThat(enumMapCaptor.getValue()).containsOnly(entry("key1", TestEnum.ENUM_VALUE1), entry("key2", TestEnum.ENUM_VALUE2));
	}

	@Test
	void invokeExtendedMapConverterForEnumsAfterResolvingTheMapTypes() {

		converter.read(TestExtendedMap.class, source);

		verify(extendedMapReadingConverter).convert(enumMapCaptor.capture());
		assertThat(enumMapCaptor.getValue()).containsOnly(entry("key1", TestEnum.ENUM_VALUE1), entry("key2", TestEnum.ENUM_VALUE2));
	}

	@Test
	void invokeVavrMapConverterForEnumsAfterResolvingTheMapTypes() {

		converter.read(TestVavrMap.class, source);

		verify(vavrMapReadingConverter).convert(enumMapCaptor.capture());
		assertThat(enumMapCaptor.getValue()).containsOnly(entry("key1", TestEnum.ENUM_VALUE1), entry("key2", TestEnum.ENUM_VALUE2));
	}


	@ReadingConverter
	private interface JavaMapReadingConverter extends Converter<Map<?, ?>, Map<?, ?>> {
	}

	@ReadingConverter
	private interface ExtendedMapReadingConverter extends Converter<Map<?, ?>, ExtendedMap<?, ?>> {
	}

	@ReadingConverter
	private interface VavrMapReadingConverter extends Converter<Map<?, ?>, io.vavr.collection.Map<?, ?>> {
	}

	private enum TestEnum {
		ENUM_VALUE1,
		ENUM_VALUE2
	}

	private static class TestJavaMap {
		@SuppressWarnings("unused")
		Map<String, TestEnum> map;
	}

	private static class TestExtendedMap {
		@SuppressWarnings("unused")
		ExtendedMap<String, TestEnum> map;
	}

	private static class TestVavrMap {
		@SuppressWarnings("unused")
		io.vavr.collection.Map<String, TestEnum> map;
	}

	private static class ExtendedMap<K, V> implements Map<K, V> {
		HashMap<K, V> internalMap = new HashMap<>();

		@Override
		public int size() {
			return internalMap.size();
		}

		@Override
		public boolean isEmpty() {
			return internalMap.isEmpty();
		}

		@Override
		public boolean containsKey(Object o) {
			return internalMap.containsKey(o);
		}

		@Override
		public boolean containsValue(Object o) {
			return internalMap.containsValue(o);
		}

		@Override
		public V get(Object o) {
			return internalMap.get(o);
		}

		@Nullable
		@Override
		public V put(K k, V v) {
			return internalMap.put(k, v);
		}

		@Override
		public V remove(Object o) {
			return internalMap.remove(o);
		}

		@Override
		public void putAll(@NotNull Map<? extends K, ? extends V> map) {
			internalMap.putAll(map);
		}

		@Override
		public void clear() {
			internalMap.clear();
		}

		@NotNull
		@Override
		public Set<K> keySet() {
			return internalMap.keySet();
		}

		@NotNull
		@Override
		public Collection<V> values() {
			return internalMap.values();
		}

		@NotNull
		@Override
		public Set<Entry<K, V>> entrySet() {
			return internalMap.entrySet();
		}
	}
}