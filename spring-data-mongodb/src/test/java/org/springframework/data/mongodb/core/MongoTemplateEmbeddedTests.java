/*
 * Copyright 2020 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.mongodb.core.mapping.Embedded;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.Template;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MongoTemplateExtension.class)
class MongoTemplateEmbeddedTests {

	private static @Template MongoTemplate template;

	@Test // DATAMONGO-1902
	void readWrite() {

		WithEmbedded source = new WithEmbedded();
		source.id = "id-1";
		source.embeddableValue = new EmbeddableType();
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		template.save(source);

		assertThat(template.findOne(query(where("id").is(source.id)), WithEmbedded.class)).isEqualTo(source);
	}

	@Test // DATAMONGO-1902
	void filterOnEmbeddedValue() {

		WithEmbedded source = new WithEmbedded();
		source.id = "id-1";
		source.embeddableValue = new EmbeddableType();
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		template.save(source);

		assertThat(template.findOne(
				Query.query(where("embeddableValue.stringValue").is(source.embeddableValue.stringValue)), WithEmbedded.class))
						.isEqualTo(source);
	}

	@Test // DATAMONGO-1902
	void readWritePrefixed() {

		WithPrefixedEmbedded source = new WithPrefixedEmbedded();
		source.id = "id-1";
		source.embeddableValue = new EmbeddableType();
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		template.save(source);

		assertThat(template.findOne(query(where("id").is(source.id)), WithPrefixedEmbedded.class)).isEqualTo(source);
	}

	@Test // DATAMONGO-1902
	void filterOnPrefixedEmbeddedValue() {

		WithPrefixedEmbedded source = new WithPrefixedEmbedded();
		source.id = "id-1";
		source.embeddableValue = new EmbeddableType();
		source.embeddableValue.stringValue = "string-val";
		source.embeddableValue.listValue = Arrays.asList("list-val-1", "list-val-2");
		source.embeddableValue.atFieldAnnotatedValue = "@Field";

		template.save(source);

		assertThat(
				template.findOne(Query.query(where("embeddableValue.stringValue").is(source.embeddableValue.stringValue)),
						WithPrefixedEmbedded.class)).isEqualTo(source);
	}

	@EqualsAndHashCode
	static class WithEmbedded {

		String id;

		@Embedded.Nullable EmbeddableType embeddableValue;
	}

	@EqualsAndHashCode
	static class WithPrefixedEmbedded {

		String id;

		@Embedded.Nullable("prefix-") EmbeddableType embeddableValue;
	}

	@EqualsAndHashCode
	static class EmbeddableType {

		String stringValue;
		List<String> listValue;

		@Field("with-at-field-annotation") //
		String atFieldAnnotatedValue;
	}
}
