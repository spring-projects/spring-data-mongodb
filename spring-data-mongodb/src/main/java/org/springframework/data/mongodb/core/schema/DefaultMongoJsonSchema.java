/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Value object representing a MongoDB-specific JSON schema which is the default {@link MongoJsonSchema} implementation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
class DefaultMongoJsonSchema implements MongoJsonSchema {

	private final JsonSchemaObject root;

	@Nullable //
	private final Document encryptionMetadata;

	DefaultMongoJsonSchema(JsonSchemaObject root) {
		this(root, null);
	}

	/**
	 * Create new instance of {@link DefaultMongoJsonSchema}.
	 *
	 * @param root the schema root element.
	 * @param encryptionMetadata can be {@literal null}.
	 * @since 3.3
	 */
	DefaultMongoJsonSchema(JsonSchemaObject root, @Nullable Document encryptionMetadata) {

		Assert.notNull(root, "Root schema object must not be null!");

		this.root = root;
		this.encryptionMetadata = encryptionMetadata;
	}

	@Override
	public Document schemaDocument() {

		Document schemaDocument = new Document();

		// we want this to be the first element rendered, so it reads nice when printed to json
		if (!CollectionUtils.isEmpty(encryptionMetadata)) {
			schemaDocument.append("encryptMetadata", encryptionMetadata);
		}

		schemaDocument.putAll(root.toDocument());

		return schemaDocument;
	}
}
