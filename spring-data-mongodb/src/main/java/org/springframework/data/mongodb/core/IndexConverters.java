/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

import com.mongodb.client.model.Collation;
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

			if (indexOptions.containsKey("partialFilterExpression")) {
				ops = ops.partialFilterExpression((org.bson.Document) indexOptions.get("partialFilterExpression"));
			}

			if (indexOptions.containsKey("collation")) {
				ops = ops.collation(fromDocument(indexOptions.get("collation", Document.class)));
			}

			return ops;
		};
	}

	@Nullable
	public static Collation fromDocument(@Nullable Document source) {

		if (source == null) {
			return null;
		}

		return org.springframework.data.mongodb.core.query.Collation.from(source).toMongoCollation();
	}

	private static Converter<Document, IndexInfo> getDocumentIndexInfoConverter() {
		return IndexInfo::indexInfoOf;
	}

}
