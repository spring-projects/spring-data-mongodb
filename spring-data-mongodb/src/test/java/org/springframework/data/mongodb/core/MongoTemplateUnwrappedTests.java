/*
 * Copyright 2020-2024 the original author or authors.
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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.Unwrapped;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.Template;

/**
 * Integration tests for {@link Unwrapped}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MongoTemplateExtension.class)
class MongoTemplateUnwrappedTests {

	private static @Template MongoTemplate template;

	@Test // DATAMONGO-1902
	void readWrite() {

		WithUnwrapped source = new WithUnwrapped();
		source.id = "id-1";
		source.embeddableValue = new UnwrappableType();
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		template.save(source);

		assertThat(template.findOne(query(where("id").is(source.id)), WithUnwrapped.class)).isEqualTo(source);
	}

	@Test // DATAMONGO-1902
	void filterOnUnwrappedValue() {

		WithUnwrapped source = new WithUnwrapped();
		source.id = "id-1";
		source.embeddableValue = new UnwrappableType();
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		template.save(source);

		assertThat(template.findOne(
				Query.query(where("embeddableValue.stringValue").is(source.embeddableValue.stringValue)), WithUnwrapped.class))
						.isEqualTo(source);
	}

	@Test // DATAMONGO-1902
	void readWritePrefixed() {

		WithPrefixedUnwrapped source = new WithPrefixedUnwrapped();
		source.id = "id-1";
		source.embeddableValue = new UnwrappableType();
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		template.save(source);

		assertThat(template.findOne(query(where("id").is(source.id)), WithPrefixedUnwrapped.class)).isEqualTo(source);
	}

	@Test // DATAMONGO-1902
	void filterOnPrefixedUnwrappedValue() {

		WithPrefixedUnwrapped source = new WithPrefixedUnwrapped();
		source.id = "id-1";
		source.embeddableValue = new UnwrappableType();
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		template.save(source);

		assertThat(
				template.findOne(Query.query(where("embeddableValue.stringValue").is(source.embeddableValue.stringValue)),
						WithPrefixedUnwrapped.class)).isEqualTo(source);
	}

	@EqualsAndHashCode
	@ToString
	static class WithUnwrapped {

		String id;

		@Unwrapped.Nullable UnwrappableType embeddableValue;
	}

	@EqualsAndHashCode
	@ToString
	static class WithPrefixedUnwrapped {

		String id;

		@Unwrapped.Nullable("prefix-") UnwrappableType embeddableValue;
	}

	@EqualsAndHashCode
	@ToString
	static class UnwrappableType {

		String stringValue;
		List<String> listValue;

		@Field("with-at-field-annotation") //
		String atFieldAnnotatedValue;
	}
}
