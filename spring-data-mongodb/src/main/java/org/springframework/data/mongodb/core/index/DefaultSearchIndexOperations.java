/*
 * Copyright 2024 the original author or authors.
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

import org.bson.Document;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.SearchIndexType;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.5
 */
public class DefaultSearchIndexOperations implements SearchIndexOperations {

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final TypeInformation<?> entityTypeInformation;

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
	public String ensureIndex(SearchIndexDefinition indexDefinition) {

		if (!(indexDefinition instanceof VectorIndex vsi)) {
			throw new IllegalStateException("Index definitions must be of type VectorIndex");
		}

		Document index = indexDefinition.getIndexDocument(entityTypeInformation,
				mongoOperations.getConverter().getMappingContext());

		mongoOperations.getCollection(collectionName).createSearchIndexes(List
				.of(new SearchIndexModel(vsi.getName(), (Document) index.get("definition"), SearchIndexType.vectorSearch())));

		return vsi.getName();
	}

	@Override
	public void updateIndex(SearchIndexDefinition index) {

		if (index instanceof VectorIndex) {
			throw new UnsupportedOperationException("Vector Index definitions cannot be updated");
		}

		Document indexDocument = index.getIndexDocument(entityTypeInformation,
				mongoOperations.getConverter().getMappingContext());

		mongoOperations.getCollection(collectionName).updateSearchIndex(index.getName(), indexDocument);
	}

	@Override
	public boolean exists(String indexName) {

		List<Document> indexes = mongoOperations.getCollection(collectionName).listSearchIndexes().into(new ArrayList<>());

		for (Document index : indexes) {
			if (index.getString("name").equals(indexName)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public List<IndexInfo> getIndexInfo() {

		AggregationResults<Document> aggregate = mongoOperations.aggregate(
				Aggregation.newAggregation(context -> new Document("$listSearchIndexes", new Document())), collectionName,
				Document.class);

		ArrayList<IndexInfo> result = new ArrayList<>();
		for (Document doc : aggregate) {

			List<IndexField> indexFields = new ArrayList<>();
			String name = doc.getString("name");
			for (Object field : doc.get("latestDefinition", Document.class).get("fields", List.class)) {

				if (field instanceof Document fieldInfo) {
					indexFields.add(IndexField.vector(fieldInfo.getString("path")));
				}
			}

			result.add(new IndexInfo(indexFields, name, false, false, null, false));
		}
		return result;
	}

	@Override
	public void dropAllIndexes() {
		getIndexInfo().forEach(indexInfo -> dropIndex(indexInfo.getName()));
	}

	@Override
	public void dropIndex(String name) {
		mongoOperations.getCollection(collectionName).dropSearchIndex(name);
	}

}
