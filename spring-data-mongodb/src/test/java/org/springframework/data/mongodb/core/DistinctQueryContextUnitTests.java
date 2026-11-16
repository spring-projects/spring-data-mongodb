/*
 * Copyright 2026-present the original author or authors.
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
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Unit tests for {@link QueryOperations.DistinctQueryContext}.
 *
 * @author Christoph Strobl
 */
class DistinctQueryContextUnitTests {

	QueryOperations queryOperations;

	@BeforeEach
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void beforeEach() {

		QueryMapper queryMapper = mock(QueryMapper.class);
		when(queryMapper.getMappingContext()).thenReturn((MappingContext) new MongoMappingContext());

		queryOperations = new QueryOperations(queryMapper, mock(UpdateMapper.class), mock(EntityOperations.class),
				mock(PropertyOperations.class), mock(CodecRegistryProvider.class));
	}

	@Test // GH-4516
	void usesActualTypeOfProperty() {

		assertThat(targetType("name")).isEqualTo(String.class);
		assertThat(targetType("tags")).isEqualTo(String.class);
		assertThat(targetType("address.street")).isEqualTo(String.class);
	}

	@Test // GH-4516
	void usesValueTypeOfMapKeyPath() {

		assertThat(targetType("map.key")).isEqualTo(Address.class);
		assertThat(targetType("map.key.street")).isEqualTo(String.class);
	}

	@Test // GH-4516
	void retainsStoredRepresentationForStoredFieldNames() {

		assertThat(targetType("id")).isEqualTo(String.class);
		assertThat(targetType("_id")).isEqualTo(Object.class);
		assertThat(targetType("addr.s")).isEqualTo(Object.class);
		assertThat(targetType("address.s")).isEqualTo(Object.class);
	}

	@Test // GH-4516
	void fallsBackToRequestedTypeForUnresolvablePaths() {

		assertThat(targetType("unknown")).isEqualTo(Object.class);
		assertThat(targetType("addressStreet")).isEqualTo(Object.class);
		assertThat(queryOperations.distinctQueryContext(new Query(), "name")
				.getMostSpecificConversionTargetType(Object.class, Document.class)).isEqualTo(Object.class);
	}

	@Test // GH-4516
	void favorsRequestedTypeIfNotAssignable() {
		assertThat(queryOperations.distinctQueryContext(new Query(), "name")
				.getMostSpecificConversionTargetType(Integer.class, Root.class)).isEqualTo(Integer.class);
	}

	private Class<?> targetType(String fieldName) {
		return queryOperations.distinctQueryContext(new Query(), fieldName)
				.getMostSpecificConversionTargetType(Object.class, Root.class);
	}

	static class Root {

		String id;
		String name;
		List<String> tags;
		@Field("addr") Address address;
		Map<String, Address> map;
	}

	static class Address {
		@Field("s") String street;
	}
}
