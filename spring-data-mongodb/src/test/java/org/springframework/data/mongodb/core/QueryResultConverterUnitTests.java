/*
 * Copyright 2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.QueryResultConverter.ConversionResultSupplier;

/**
 * @author Christoph Strobl
 */
class QueryResultConverterUnitTests {

	public static final ConversionResultSupplier<Document> ERROR_SUPPLIER = () -> {
		throw new IllegalStateException("must not read conversion result");
	};

	@Test // GH-
	void converterDoesNotEagerlyRetrieveConversionResultFromSupplier() {

		QueryResultConverter<Document, String> converter = new QueryResultConverter<Document, String>() {

			@Override
			public String mapDocument(Document document, ConversionResultSupplier<Document> reader) {
				return "done";
			}
		};

		assertThat(converter.mapDocument(new Document(), ERROR_SUPPLIER)).isEqualTo("done");
	}

	@Test // GH-
	void converterPassesOnConversionResultToNextStage() {

		Document source = new Document("value", "10");

		QueryResultConverter<Document, Integer> stagedConverter = new QueryResultConverter<Document, String>() {

			@Override
			public String mapDocument(Document document, ConversionResultSupplier<Document> reader) {
				return document.get("value", "-1");
			}
		}.andThen(new QueryResultConverter<String, Integer>() {

			@Override
			public Integer mapDocument(Document document, ConversionResultSupplier<String> reader) {

				assertThat(document).isEqualTo(source);
				return Integer.valueOf(reader.get());
			}
		});

		assertThat(stagedConverter.mapDocument(source, ERROR_SUPPLIER)).isEqualTo(10);
	}

	@Test // GH-
	void entityConverterDelaysConversion() {

		Document source = new Document("value", "10");

		QueryResultConverter<Document, Integer> converter = QueryResultConverter.<Document> entity()
				.andThen(new QueryResultConverter<Document, Integer>() {

					@Override
					public Integer mapDocument(Document document, ConversionResultSupplier<Document> reader) {

						assertThat(document).isEqualTo(source);
						return Integer.valueOf(document.get("value", "20"));
					}
				});

		assertThat(converter.mapDocument(source, ERROR_SUPPLIER)).isEqualTo(10);
	}
}
