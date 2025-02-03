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

import java.util.function.Supplier;

import org.bson.Document;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Index information for a MongoDB Search Index.
 * 
 * @author Christoph Strobl
 */
public class SearchIndexInfo {

	private final @Nullable Object id;
	private final SearchIndexStatus status;
	private final Lazy<SearchIndexDefinition> indexDefinition;

	SearchIndexInfo(@Nullable Object id, SearchIndexStatus status, Supplier<SearchIndexDefinition> indexDefinition) {
		this.id = id;
		this.status = status;
		this.indexDefinition = Lazy.of(indexDefinition);
	}

	public static SearchIndexInfo parse(String source) {
		return of(Document.parse(source));
	}

	public static SearchIndexInfo of(Document indexDocument) {

		Object id = indexDocument.get("id");
		SearchIndexStatus status = SearchIndexStatus.valueOf(indexDocument.get("status", "DOES_NOT_EXIST"));

		return new SearchIndexInfo(id, status, () -> readIndexDefinition(indexDocument));
	}

	/**
	 * The id of the index. Can be {@literal null}, eg. for an index not yet created.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public Object getId() {
		return id;
	}

	/**
	 * @return the current status of the index.
	 */
	public SearchIndexStatus getStatus() {
		return status;
	}

	/**
	 * @return the current index definition.
	 */
	public SearchIndexDefinition getIndexDefinition() {
		return indexDefinition.get();
	}

	private static SearchIndexDefinition readIndexDefinition(Document document) {

		String type = document.get("type", "search");
		if (type.equals("vectorSearch")) {
			return VectorIndex.of(document);
		}

		return new SearchIndexDefinition() {

			@Override
			public String getName() {
				return document.getString("name");
			}

			@Override
			public String getType() {
				return type;
			}

			@Override
			public Document getDefinition(@Nullable TypeInformation<?> entity,
					@Nullable MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
				if (document.containsKey("latestDefinition")) {
					return document.get("latestDefinition", new Document());
				}
				return document.get("definition", new Document());
			}

			@Override
			public String toString() {
				return getDefinition(null, null).toJson();
			}
		};
	}
}
