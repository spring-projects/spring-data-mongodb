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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.springframework.data.mongodb.core.DefaultIndexOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.VectorIndex.Filter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public class DefaultVectorIndexOperations extends DefaultIndexOperations implements VectorIndexOperations {

	private static final Log LOGGER = LogFactory.getLog(VectorIndexOperations.class);

	public DefaultVectorIndexOperations(MongoOperations mongoOperations, Class<?> type) {
		this(mongoOperations, mongoOperations.getCollectionName(type), type);
	}

	public DefaultVectorIndexOperations(MongoOperations mongoOperations, String collectionName, @Nullable Class<?> type) {
		super(mongoOperations, collectionName, type);
	}

	private static String getMappedPath(String path, MongoPersistentEntity<?> entity, QueryMapper mapper) {
		return mapper.getMappedFields(new Document(path, 1), entity).entrySet().iterator().next().getKey();
	}

	@Override
	public boolean exists(String indexName) {

		// https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSearchIndexes/
		AggregationResults<Document> aggregate = mongoOperations.aggregate(
				Aggregation.newAggregation(context -> new Document("$listSearchIndexes", new Document("name", indexName))),
				collectionName, Document.class);

		return aggregate.iterator().hasNext();
	}

	@Override
	public void updateIndex(VectorIndex index) {

		MongoPersistentEntity<?> entity = lookupPersistentEntity(type, collectionName);

		Document indexDocument = createIndexDocument(index, entity);

		Document cmdResult = mongoOperations.execute(db -> {

			Document command = new Document().append("updateSearchIndex", collectionName).append("name", index.getName());
			command.putAll(indexDocument);
			command.remove("type");

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Updating VectorIndex: db.runCommand(%s)".formatted(command.toJson()));
			}
			return db.runCommand(command);
		});
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
	public String ensureIndex(IndexDefinition indexDefinition) {

		if (!(indexDefinition instanceof VectorIndex vsi)) {
			return super.ensureIndex(indexDefinition);
		}

		MongoPersistentEntity<?> entity = lookupPersistentEntity(type, collectionName);

		Document index = createIndexDocument(vsi, entity);

		Document cmdResult = mongoOperations.execute(db -> {

			Document command = new Document().append("createSearchIndexes", collectionName).append("indexes", List.of(index));
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Creating VectorIndex: db.runCommand(%s)".formatted(command.toJson()));
			}
			return db.runCommand(command);
		});

		return cmdResult.get("ok").toString().equalsIgnoreCase("1.0") ? vsi.getName() : cmdResult.toJson();
	}

	@NonNull
	private Document createIndexDocument(VectorIndex vsi, MongoPersistentEntity<?> entity) {

		Document index = new Document(vsi.getIndexOptions());
		Document definition = new Document();

		List<Document> fields = new ArrayList<>(vsi.getFilters().size() + 1);

		Document vectorField = new Document("type", "vector");
		vectorField.append("path", getMappedPath(vsi.getPath(), entity, mapper));
		vectorField.append("numDimensions", vsi.getDimensions());
		vectorField.append("similarity", vsi.getSimilarity());

		fields.add(vectorField);

		for (Filter filter : vsi.getFilters()) {
			fields.add(new Document("type", "filter").append("path", getMappedPath(filter.path(), entity, mapper)));
		}

		definition.append("fields", fields);
		index.append("definition", definition);
		return index;
	}

	@Override
	public void dropIndex(String name) {

		Document command = new Document().append("dropSearchIndex", collectionName).append("name", name);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Dropping VectorIndex: db.runCommand(%s)".formatted(command.toJson()));
		}
		mongoOperations.execute(db -> db.runCommand(command));
	}
}
