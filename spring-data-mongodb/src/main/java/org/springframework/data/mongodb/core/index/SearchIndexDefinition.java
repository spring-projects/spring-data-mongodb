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
package org.springframework.data.mongodb.core.index;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import org.springframework.data.core.TypeInformation;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Definition for an Atlas Search Index (Search Index or Vector Index).
 *
 * @author Marcin Grzejszczak
 * @author Mark Paluch
 * @since 4.5
 */
public interface SearchIndexDefinition {

	/**
	 * @return the name of the index.
	 */
	String getName();

	/**
	 * @return the type of the index. Typically, {@code search} or {@code vectorSearch}.
	 */
	String getType();

	/**
	 * Returns the index document for this index without any potential entity context resolving field name mappings. The
	 * resulting document contains the index name, type and {@link #getDefinition(TypeInformation, MappingContext)
	 * definition}.
	 *
	 * @return never {@literal null}.
	 */
	default Document getRawIndexDocument() {
		return getIndexDocument(null, null);
	}

	/**
	 * Returns the index document for this index in the context of a potential entity to resolve field name mappings. The
	 * resulting document contains the index name, type and {@link #getDefinition(TypeInformation, MappingContext)
	 * definition}.
	 *
	 * @param entity can be {@literal null}.
	 * @param mappingContext
	 * @return never {@literal null}.
	 */
	default Document getIndexDocument(@Nullable TypeInformation<?> entity,
			@Nullable MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		Document document = new Document();
		document.put("name", getName());
		document.put("type", getType());
		document.put("definition", getDefinition(entity, mappingContext));

		return document;
	}

	/**
	 * Returns the actual index definition for this index in the context of a potential entity to resolve field name
	 * mappings. Entity and context can be {@literal null} to create a generic index definition without applying field
	 * name mapping.
	 *
	 * @param entity can be {@literal null}.
	 * @param mappingContext can be {@literal null}.
	 * @return never {@literal null}.
	 */
	Document getDefinition(@Nullable TypeInformation<?> entity,
			@Nullable MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext);

}
