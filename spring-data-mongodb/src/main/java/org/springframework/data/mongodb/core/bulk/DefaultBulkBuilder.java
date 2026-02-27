/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.bulk;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.data.mongodb.core.bulk.Bulk.BulkBuilder;
import org.springframework.data.mongodb.core.bulk.Bulk.BulkSpec;
import org.springframework.data.mongodb.core.bulk.BulkOperationContext.TypedNamespace;
import org.springframework.data.mongodb.core.mapping.CollectionName;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link BulkBuilder} that tracks the current collection (namespace) and builds a list of
 * {@link BulkOperation bulk operations} for execution. Supports bulk writes across multiple collections as in MongoDB's
 * bulk write model.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 5.1
 */
class DefaultBulkBuilder implements BulkBuilder {

	private final List<BulkOperation> bulkOperations = new ArrayList<>();

	@Override
	public BulkBuilder inCollection(String collectionName, Consumer<BulkSpec> builderCustomizer) {
		return inCollection(new TypedNamespace(null, null, CollectionName.just(collectionName)), builderCustomizer);
	}

	@Override
	public <T> BulkBuilder inCollection(Class<T> type, Consumer<BulkSpec> builderCustomizer) {

		Assert.notNull(type, "Type must not be null");

		return inCollection(new TypedNamespace(type, null, CollectionName.from(type)), builderCustomizer);
	}

	@Override
	public <T> BulkBuilder inCollection(Class<T> type, String collectionName, Consumer<BulkSpec> builderCustomizer) {

		Assert.notNull(type, "Type must not be null");

		return inCollection(new TypedNamespace(type, null, CollectionName.just(collectionName)), builderCustomizer);
	}

	private BulkBuilder inCollection(TypedNamespace namespace, Consumer<BulkSpec> builderCustomizer) {

		BulkBuilderSupport spec = new BulkBuilderSupport(bulkOperations) {
			@Override
			TypedNamespace getNamespace() {
				return namespace;
			}
		};

		builderCustomizer.accept(spec);
		return this;
	}

	@Override
	public Bulk build() {
		return () -> List.copyOf(DefaultBulkBuilder.this.bulkOperations);
	}

}
