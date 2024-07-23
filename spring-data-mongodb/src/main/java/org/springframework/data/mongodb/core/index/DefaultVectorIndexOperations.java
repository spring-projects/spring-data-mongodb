/*
 * Copyright 2024. the original author or authors.
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

/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.springframework.data.mongodb.core.DefaultIndexOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexField.Type;
import org.springframework.data.mongodb.core.index.VectorIndex.Filter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public class DefaultVectorIndexOperations extends DefaultIndexOperations implements VectorIndexOperations {

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
	public List<IndexInfo> getIndexInfo() {

		AggregationResults<Document> aggregate = mongoOperations.aggregate(
			Aggregation.newAggregation(context -> new Document("$listSearchIndexes", new Document())),
			collectionName, Document.class);

		String json = aggregate.getRawResults().toJson(JsonWriterSettings.builder().indent(true).build());

		/*
		{
      "id": "669e2b40c587f62c3e03ccd0",
      "name": "vector_index",
      "type": "vectorSearch",
      "status": "READY",
      "queryable": true,
      "latestVersion": 0,
      "latestDefinition": {
        "fields": [
          {
            "type": "vector",
            "path": "plot_embedding",
            "numDimensions": 1536,
            "similarity": "cosine"
          }
        ]
		 */
		ArrayList<IndexInfo> result = new ArrayList<>();
		for(Document doc : aggregate) {

			List<IndexField> indexFields = new ArrayList<>();
			String name = doc.getString("name");
			for(Object field : doc.get("latestDefinition", Document.class).get("fields", List.class)) {

				if(field instanceof Document fieldInfo) {
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

		Document cmdResult = mongoOperations.execute(db -> {

			MongoPersistentEntity<?> entity = lookupPersistentEntity(type, collectionName);

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

			Document command = new Document().append("createSearchIndexes", collectionName).append("indexes", List.of(index));

			return db.runCommand(command);
		});

		return cmdResult.get("ok").toString().equalsIgnoreCase("1.0") ? vsi.getName() : cmdResult.toJson();
	}

	@Override
	public void dropIndex(String name) {

		Document command = new Document().append("dropSearchIndex", collectionName).append("name", name);
		mongoOperations.execute(db -> db.runCommand(command));
	}
}
