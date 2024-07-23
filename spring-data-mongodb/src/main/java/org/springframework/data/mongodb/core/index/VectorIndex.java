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

import java.util.Collections;
import java.util.List;

import org.bson.Document;

/**
 * @author Christoph Strobl
 * @since 2024/07
 */
public class VectorIndex implements IndexDefinition {

	String name;
	String path;
	int dimensions;
	String similarity;
	List<Filter> filters;

	public VectorIndex(String name) {
		this.name = name;
	}

	public static VectorIndex cosine(String name) {
		VectorIndex idx = new VectorIndex(name);
		idx.similarity = "cosine";
		return idx;
	}

	public VectorIndex path(String path) {
		this.path = path;
		return this;
	}

	public VectorIndex dimensions(int dimensions) {
		this.dimensions = dimensions;
		return this;
	}

	public VectorIndex similarity(String similarity) {
		this.similarity = similarity;
		return this;
	}

	@Override
	public Document getIndexKeys() {

		// List<Document> fields = new ArrayList<>(filters.size()+1);
		// fields.

		// needs to be wrapped in new Document("definition", before sending to server
		// return new Document("fields", fields);
		return new Document();
	}

	@Override
	public Document getIndexOptions() {
		return new Document("name", name).append("type", "vectorSearch");
	}

	public String getName() {
		return name;
	}

	public String getPath() {
		return path;
	}

	public int getDimensions() {
		return dimensions;
	}

	public String getSimilarity() {
		return similarity;
	}

	public List<Filter> getFilters() {
		return filters == null ? Collections.emptyList() : filters;
	}

	public record Filter(String path) {

	}
}
