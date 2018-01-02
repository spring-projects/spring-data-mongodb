/*
 * Copyright 2016-2018 the original author or authors.
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

import java.util.List;

import org.springframework.util.Assert;

/**
 * Adapter for creating synchronous {@link IndexOperations}.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface IndexOperationsAdapter extends IndexOperations {

	/**
	 * Obtain a blocking variant of {@link IndexOperations} wrapping {@link ReactiveIndexOperations}.
	 *
	 * @param reactiveIndexOperations must not be {@literal null}.
	 * @return never {@literal null}
	 */
	static IndexOperationsAdapter blocking(ReactiveIndexOperations reactiveIndexOperations) {

		Assert.notNull(reactiveIndexOperations, "ReactiveIndexOperations must not be null!");

		return new IndexOperationsAdapter() {

			@Override
			public String ensureIndex(IndexDefinition indexDefinition) {
				return reactiveIndexOperations.ensureIndex(indexDefinition).block();
			}

			@Override
			public void dropIndex(String name) {
				reactiveIndexOperations.dropIndex(name).block();
			}

			@Override
			public void dropAllIndexes() {
				reactiveIndexOperations.dropAllIndexes().block();
			}

			@Override
			public List<IndexInfo> getIndexInfo() {
				return reactiveIndexOperations.getIndexInfo().collectList().block();
			}
		};
	}
}
