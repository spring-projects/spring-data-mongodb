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

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonString;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.SearchIndexType;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.5
 */
public class DefaultSearchIndexOperations implements SearchIndexOperations {

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final @Nullable TypeInformation<?> entityTypeInformation;

	public DefaultSearchIndexOperations(MongoOperations mongoOperations, Class<?> type) {
		this(mongoOperations, mongoOperations.getCollectionName(type), type);
	}

	public DefaultSearchIndexOperations(MongoOperations mongoOperations, String collectionName, @Nullable Class<?> type) {

		this.collectionName = collectionName;

		if (type != null) {

			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext = mongoOperations
					.getConverter().getMappingContext();
			entityTypeInformation = mappingContext.getRequiredPersistentEntity(type).getTypeInformation();
		} else {
			entityTypeInformation = null;
		}

		this.mongoOperations = mongoOperations;
	}

	@Override
	public String createIndex(SearchIndexDefinition indexDefinition) {

		Document index = indexDefinition.getIndexDocument(entityTypeInformation,
				mongoOperations.getConverter().getMappingContext());

		mongoOperations.getCollection(collectionName)
				.createSearchIndexes(List.of(new SearchIndexModel(indexDefinition.getName(),
						index.get("definition", Document.class), SearchIndexType.of(new BsonString(indexDefinition.getType())))));

		return indexDefinition.getName();
	}

	@Override
	public void updateIndex(SearchIndexDefinition indexDefinition) {

		Document indexDocument = indexDefinition.getIndexDocument(entityTypeInformation,
				mongoOperations.getConverter().getMappingContext());

		mongoOperations.getCollection(collectionName).updateSearchIndex(indexDefinition.getName(), indexDocument);
	}

	@Override
	public boolean exists(String indexName) {
		return getSearchIndex(indexName) != null;
	}

	@Override
	public SearchIndexStatus status(String indexName) {

		Document searchIndex = getSearchIndex(indexName);
		return searchIndex != null ? SearchIndexStatus.valueOf(searchIndex.getString("status"))
				: SearchIndexStatus.DOES_NOT_EXIST;
	}

	@Override
	public void dropAllIndexes() {
		getSearchIndexes(null).forEach(indexInfo -> dropIndex(indexInfo.getString("name")));
	}

	@Override
	public void dropIndex(String indexName) {
		mongoOperations.getCollection(collectionName).dropSearchIndex(indexName);
	}

	@Nullable
	private Document getSearchIndex(String indexName) {

		List<Document> indexes = getSearchIndexes(indexName);
		return indexes.isEmpty() ? null : indexes.iterator().next();
	}

	private List<Document> getSearchIndexes(@Nullable String indexName) {

		Document filter = StringUtils.hasText(indexName) ? new Document("name", indexName) : new Document();

		return mongoOperations.getCollection(collectionName).aggregate(List.of(new Document("$listSearchIndexes", filter)))
				.into(new ArrayList<>());
	}

}
