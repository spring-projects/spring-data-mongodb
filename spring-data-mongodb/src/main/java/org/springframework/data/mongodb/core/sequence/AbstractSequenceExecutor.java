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
package org.springframework.data.mongodb.core.sequence;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.util.Assert;

/**
 * Shares the increment itself between executors, leaving them to decide only which {@link MongoDatabase} it runs
 * against - which is what settles the transactional behaviour.
 * <p>
 * Creation and increment are separate round trips on purpose: MongoDB rejects an update that both
 * {@code $setOnInsert}s and {@code $inc}s the same field, so there is no way to say "start here, otherwise step" in one
 * statement. The first call therefore inserts and hands back the start value, and every call after it increments.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
public abstract class AbstractSequenceExecutor implements SequenceExecutor {

	@Override
	public long nextValue(SequenceDefinition definition, MongoDatabaseFactory dbFactory) {

		Assert.notNull(definition, "SequenceDefinition must not be null");
		Assert.notNull(dbFactory, "MongoDatabaseFactory must not be null");

		MongoCollection<Document> collection = getCollection(definition, dbFactory);
		Document existing = createIfAbsent(collection, definition);

		if (existing == null) {
			return definition.getStartValue();
		}

		SequenceDefinition effective = reconcile(definition, existing);
		Document updated = collection.findOneAndUpdate(filter(effective), effective.getStrategy().incrementUpdate(effective),
				new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

		if (updated == null) {
			throw new IllegalStateException("Sequence '%s' disappeared while being incremented".formatted(effective.getName()));
		}

		return effective.getStrategy().currentValue(updated);
	}

	/**
	 * The database to run against. Implementations differ here and nowhere else.
	 *
	 * @param databaseName the database named by the definition, or {@literal null} to use the factory default.
	 * @param dbFactory must not be {@literal null}.
	 * @return the database to use, never {@literal null}.
	 */
	protected abstract MongoDatabase getDatabase(@Nullable String databaseName, MongoDatabaseFactory dbFactory);

	private MongoCollection<Document> getCollection(SequenceDefinition definition, MongoDatabaseFactory dbFactory) {
		return getDatabase(definition.getDatabaseName(), dbFactory).getCollection(definition.getCollectionName());
	}

	/**
	 * Insert the sequence if it is not there yet.
	 *
	 * @return the document as it was before, or {@literal null} if this call created it.
	 */
	private @Nullable Document createIfAbsent(MongoCollection<Document> collection, SequenceDefinition definition) {

		Document initial = definition.getStrategy().initialDocument(definition);

		return collection.findOneAndUpdate(filter(definition), new Document("$setOnInsert", initial),
				new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));
	}

	private static SequenceDefinition reconcile(SequenceDefinition definition, Document stored) {

		if (!definition.getStrategy().isSelfDescribing()) {
			return definition;
		}

		return definition.getDefinitionPolicy().reconcile(definition, stored);
	}

	private static Document filter(SequenceDefinition definition) {
		return new Document(CounterSequenceStrategy.ID_FIELD, definition.getName());
	}
}
