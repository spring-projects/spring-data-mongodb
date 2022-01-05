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
import org.springframework.util.Assert;

/**
 * JSON schema backed by a {@link org.bson.Document} object.
 *
 * @author Mark Paluch
 * @since 2.1
 */
class DocumentJsonSchema implements MongoJsonSchema {

	private final Document document;

	DocumentJsonSchema(Document document) {

		Assert.notNull(document, "Document must not be null!");
		this.document = document;
	}

	@Override
	public Document schemaDocument() {
		return new Document(document);
	}
}
