/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.mongodb.core;

import static org.springframework.data.domain.Sort.Direction.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.util.ObjectUtils;

import com.mongodb.client.model.IndexOptions;

/**
 * {@link Converter Converters} for index-related MongoDB documents/types.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
abstract class IndexConverters {

	private static final Converter<IndexDefinition, IndexOptions> DEFINITION_TO_MONGO_INDEX_OPTIONS;
	private static final Converter<Document, IndexInfo> DOCUMENT_INDEX_INFO;

	private static final Double ONE = Double.valueOf(1);
	private static final Double MINUS_ONE = Double.valueOf(-1);
	private static final Collection<String> TWO_D_IDENTIFIERS = Arrays.asList("2d", "2dsphere");

	static {

		DEFINITION_TO_MONGO_INDEX_OPTIONS = getIndexDefinitionIndexOptionsConverter();
		DOCUMENT_INDEX_INFO = getDocumentIndexInfoConverter();
	}

	private IndexConverters() {

	}

	static Converter<IndexDefinition, IndexOptions> indexDefinitionToIndexOptionsConverter() {
		return DEFINITION_TO_MONGO_INDEX_OPTIONS;
	}

	static Converter<Document, IndexInfo> documentToIndexInfoConverter() {
		return DOCUMENT_INDEX_INFO;
	}

	private static Converter<IndexDefinition, IndexOptions> getIndexDefinitionIndexOptionsConverter() {

		return indexDefinition -> {

			Document indexOptions = indexDefinition.getIndexOptions();
			IndexOptions ops = new IndexOptions();

			if (indexOptions.containsKey("name")) {
				ops = ops.name(indexOptions.get("name").toString());
			}
			if (indexOptions.containsKey("unique")) {
				ops = ops.unique((Boolean) indexOptions.get("unique"));
			}
			if (indexOptions.containsKey("sparse")) {
				ops = ops.sparse((Boolean) indexOptions.get("sparse"));
			}
			if (indexOptions.containsKey("background")) {
				ops = ops.background((Boolean) indexOptions.get("background"));
			}
			if (indexOptions.containsKey("expireAfterSeconds")) {
				ops = ops.expireAfter((Long) indexOptions.get("expireAfterSeconds"), TimeUnit.SECONDS);
			}
			if (indexOptions.containsKey("min")) {
				ops = ops.min(((Number) indexOptions.get("min")).doubleValue());
			}
			if (indexOptions.containsKey("max")) {
				ops = ops.max(((Number) indexOptions.get("max")).doubleValue());
			}
			if (indexOptions.containsKey("bits")) {
				ops = ops.bits((Integer) indexOptions.get("bits"));
			}
			if (indexOptions.containsKey("bucketSize")) {
				ops = ops.bucketSize(((Number) indexOptions.get("bucketSize")).doubleValue());
			}
			if (indexOptions.containsKey("default_language")) {
				ops = ops.defaultLanguage(indexOptions.get("default_language").toString());
			}
			if (indexOptions.containsKey("language_override")) {
				ops = ops.languageOverride(indexOptions.get("language_override").toString());
			}
			if (indexOptions.containsKey("weights")) {
				ops = ops.weights((org.bson.Document) indexOptions.get("weights"));
			}

			for (String key : indexOptions.keySet()) {
				if (ObjectUtils.nullSafeEquals("2dsphere", indexOptions.get(key))) {
					ops = ops.sphereVersion(2);
				}
			}

			return ops;
		};
	}

	private static Converter<Document, IndexInfo> getDocumentIndexInfoConverter() {

		return ix -> {
			Document keyDocument = (Document) ix.get("key");
			int numberOfElements = keyDocument.keySet().size();

			List<IndexField> indexFields = new ArrayList<IndexField>(numberOfElements);

			for (String key : keyDocument.keySet()) {

				Object value = keyDocument.get(key);

				if (TWO_D_IDENTIFIERS.contains(value)) {
					indexFields.add(IndexField.geo(key));
				} else if ("text".equals(value)) {

					Document weights = (Document) ix.get("weights");
					for (String fieldName : weights.keySet()) {
						indexFields.add(IndexField.text(fieldName, Float.valueOf(weights.get(fieldName).toString())));
					}

				} else {

					Double keyValue = new Double(value.toString());

					if (ONE.equals(keyValue)) {
						indexFields.add(IndexField.create(key, ASC));
					} else if (MINUS_ONE.equals(keyValue)) {
						indexFields.add(IndexField.create(key, DESC));
					}
				}
			}

			String name = ix.get("name").toString();

			boolean unique = ix.containsKey("unique") ? (Boolean) ix.get("unique") : false;
			boolean sparse = ix.containsKey("sparse") ? (Boolean) ix.get("sparse") : false;

			String language = ix.containsKey("default_language") ? (String) ix.get("default_language") : "";
			return new IndexInfo(indexFields, name, unique, sparse, language);
		};
	}
}
