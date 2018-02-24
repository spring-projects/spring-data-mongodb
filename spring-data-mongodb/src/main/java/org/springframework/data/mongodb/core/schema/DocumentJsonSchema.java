/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import org.bson.Document;

/**
 * JSON schema backed by a {@link org.bson.Document} object.
 *
 * @author Mark Paluch
 * @since 2.1
 */
@AllArgsConstructor
class DocumentJsonSchema implements MongoJsonSchema {

	private final @NonNull Document document;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.schema.MongoJsonSchema#toDocument()
	 */
	@Override
	public Document toDocument() {
		return new Document("$jsonSchema", new Document(document));
	}
}
