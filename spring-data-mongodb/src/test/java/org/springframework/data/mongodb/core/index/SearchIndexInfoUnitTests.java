/*
 * Copyright 2025-present the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * @author Christoph Strobl
 */
class SearchIndexInfoUnitTests {

	@ParameterizedTest
	@ValueSource(strings = { """
			{
			  "id": "679b7637a580c270015ef6fb",
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
			        "similarity": "euclidean"
			      }
			    ]
			  }
			}""", """
			{
			  id: '648b4ad4d697b73bf9d2e5e1',
			  name: 'search-index',
			  status: 'PENDING',
			  queryable: false,
			  latestDefinition: {
			    mappings: { dynamic: false, fields: { text: { type: 'string' } } }
			  }
			}""", """
			{
			  name: 'search-index-not-yet-created',
			  definition: {
			    mappings: { dynamic: false, fields: { text: { type: 'string' } } }
			  }
			}""", """
			{
			  name: 'vector-index-with-filter',
			  type: "vectorSearch",
			  definition: {
			    fields: [
			      {
			        type: "vector",
			        path: "plot_embedding",
			        numDimensions: 1536,
			        similarity: "euclidean"
			      }, {
			        type: "filter",
			        path: "year"
			      }
			    ]
			  }
			}""" })
	void parsesIndexInfo(String indexInfoSource) {

		SearchIndexInfo indexInfo = SearchIndexInfo.parse(indexInfoSource);

		if (indexInfo.getId() != null) {
			assertThat(indexInfo.getId()).isInstanceOf(String.class);
		}
		assertThat(indexInfo.getStatus()).isNotNull();
		assertThat(indexInfo.getIndexDefinition()).isNotNull();
	}
}
