/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.bson.Document;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.util.Assert;

import com.mongodb.reactivestreams.client.ListIndexesPublisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Default implementation of {@link IndexOperations}.
 *
 * @author Mark Paluch
 * @since 1.11
 */
public class DefaultReactiveIndexOperations implements ReactiveIndexOperations {

	private final ReactiveMongoOperations mongoOperations;
	private final String collectionName;

	/**
	 * Creates a new {@link DefaultReactiveIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 */
	public DefaultReactiveIndexOperations(ReactiveMongoOperations mongoOperations, String collectionName) {

		Assert.notNull(mongoOperations, "ReactiveMongoOperations must not be null!");
		Assert.notNull(collectionName, "Collection must not be null!");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveIndexOperations#ensureIndex(org.springframework.data.mongodb.core.index.IndexDefinition)
	 */
	public Mono<String> ensureIndex(final IndexDefinition indexDefinition) {

		return mongoOperations.execute(collectionName, (ReactiveCollectionCallback<String>) collection -> {

			Document indexOptions = indexDefinition.getIndexOptions();

			if (indexOptions != null) {
				return collection.createIndex(indexDefinition.getIndexKeys(),
						IndexConverters.indexDefinitionToIndexOptionsConverter().convert(indexDefinition));
			}

			return collection.createIndex(indexDefinition.getIndexKeys());
		}).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveIndexOperations#dropIndex(java.lang.String)
	 */
	public Mono<Void> dropIndex(final String name) {

		return mongoOperations.execute(collectionName, collection -> {

			return Mono.from(collection.dropIndex(name));
		}).flatMap(success -> Mono.<Void>empty()).next();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveIndexOperations#dropAllIndexes()
	 */
	public Mono<Void> dropAllIndexes() {
		return dropIndex("*");
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.ReactiveIndexOperations#getIndexInfo()
	 */
	public Flux<IndexInfo> getIndexInfo() {

		return mongoOperations.execute(collectionName, collection -> {

			ListIndexesPublisher<Document> indexesPublisher = collection.listIndexes(Document.class);

			return Flux.from(indexesPublisher).map(IndexConverters.documentToIndexInfoConverter()::convert);
		});
	}
}
