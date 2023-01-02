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
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link UnsetOperation}.
 *
 * @author Christoph Strobl
 */
public class UnsetOperationUnitTests {

	@Test // DATAMONGO-2331
	public void raisesErrorOnNullField() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new UnsetOperation(null));
	}

	@Test // DATAMONGO-2331
	public void rendersSingleFieldReferenceCorrectly() {

		assertThat(new UnsetOperation(Collections.singletonList("title")).toPipelineStages(contextFor(Book.class)))
				.containsExactly(Document.parse("{\"$unset\" : \"title\" }"));
	}

	@Test // DATAMONGO-2331
	public void rendersSingleMappedFieldReferenceCorrectly() {

		assertThat(new UnsetOperation(Collections.singletonList("stock")).toPipelineStages(contextFor(Book.class)))
				.containsExactly(Document.parse("{\"$unset\" : \"copies\" }"));
	}

	@Test // DATAMONGO-2331
	public void rendersSingleNestedMappedFieldReferenceCorrectly() {

		assertThat(
				new UnsetOperation(Collections.singletonList("author.firstname")).toPipelineStages(contextFor(Book.class)))
						.containsExactly(Document.parse("{\"$unset\" : \"author.first\"}"));
	}

	@Test // DATAMONGO-2331
	public void rendersMultipleFieldReferencesCorrectly() {

		assertThat(new UnsetOperation(Arrays.asList("title", "author.firstname", "stock.location"))
				.toPipelineStages(contextFor(Book.class)))
						.containsExactly(Document.parse("{\"$unset\" : [\"title\", \"author.first\", \"copies.warehouse\"] }"));
	}

	@Test // DATAMONGO-2331
	public void exposesFieldsCorrectly() {
		assertThat(UnsetOperation.unset("title").and("isbn").getFields()).isEqualTo(ExposedFields.from());
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

	static class Book {

		@Id Integer id;
		String title;
		String isbn;
		Author author;
		@Field("copies") Collection<Warehouse> stock;
	}

	static class Author {

		@Field("first") String firstname;
		@Field("last") String lastname;
	}

	static class Warehouse {

		@Field("warehouse") String location;
		Integer qty;
	}
}
