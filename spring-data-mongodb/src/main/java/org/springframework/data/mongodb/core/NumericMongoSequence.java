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
package org.springframework.data.mongodb.core;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.NumericMongoSequence.SequenceInitializer.InitializationResult;
import org.springframework.data.mongodb.core.NumericMongoSequence.SequenceInitializer.InitializationResult.InitializationState;
import org.springframework.util.Assert;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

/**
 * @author Christoph Strobl
 * @since 5.1
 */
class NumericMongoSequence implements MongoSequence<Long> {

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final String sequenceName;
	private final String valueField;

	private final SequenceInitializer<Long> sequenceInitializer;
	private final NextValueRetriever<Long> valueRetriever;
	private final @Nullable Long startWith;
	private volatile @Nullable Long lastValue;

	NumericMongoSequence(MongoOperations mongoOperations, String collection, String sequenceName, long increment) {
		this(mongoOperations, collection, sequenceName, "count", null, increment);
	}

	NumericMongoSequence(MongoOperations mongoOperations, String collection, String sequenceName, String valueField,
			@Nullable Long startWith, long increment) {

		Assert.isTrue(increment != 0, "Increment must be non-zero");

		this.mongoOperations = mongoOperations;
		this.collectionName = collection;
		this.sequenceName = sequenceName;
		this.valueField = valueField;
		this.startWith = startWith;

		Document counterLookup = new Document("_id", sequenceName);
		this.sequenceInitializer = startWith != null ? new SequenceValueInitializer(counterLookup, startWith)
				: NoOpSequenceInitializer.INSTANCE;

		Document counterIncrement = new Document("$inc", new Document(valueField, increment));
		this.valueRetriever = new SequenceNextValueRetriever(counterLookup, counterIncrement);
	}

	@Override
	public Long nextValue() {

		Long value = doGetNextValue();
		this.lastValue = value;
		return value;
	}

	private Long doGetNextValue() {

		if (startWith != null && sequenceInitializer.getState().equals(InitializationState.PENDING)) {

			InitializationResult<Long> initializationResult = sequenceInitializer.initialize();
			if (initializationResult.state().equals(InitializationState.INITIALIZED)) {
				return initializationResult.currentValue();
			}
		}

		return valueRetriever.nextValue();
	}

	@Override
	public String toString() {

		if (lastValue == null) {
			return "%s.%s".formatted(sequenceName, valueField);
		}
		return "%s.%s=%s".formatted(sequenceName, valueField, lastValue);
	}

	interface SequenceInitializer<T> {

		InitializationResult<T> initialize();

		InitializationState getState();

		record InitializationResult<T>(InitializationState state, @Nullable T currentValue) {

			enum InitializationState {
				PENDING, EXISTS, INITIALIZED;
			}
		}
	}

	interface NextValueRetriever<T> {
		T nextValue();
	}

	static class NoOpSequenceInitializer implements SequenceInitializer<Long> {

		static final NoOpSequenceInitializer INSTANCE = new NoOpSequenceInitializer();

		private static final InitializationResult<Long> DEFAULT_STATE = new InitializationResult<>(
				InitializationState.EXISTS, null);

		@Override
		public InitializationResult<Long> initialize() {
			return DEFAULT_STATE;
		}

		@Override
		public InitializationState getState() {
			return DEFAULT_STATE.state();
		}
	}

	private class SequenceNextValueRetriever implements NextValueRetriever<Long> {

		private static final FindOneAndUpdateOptions UPDATE_OPTIONS = new FindOneAndUpdateOptions().upsert(true)
				.returnDocument(ReturnDocument.AFTER);

		private final Document counterLookup;
		private final Document counterIncrement;

		public SequenceNextValueRetriever(Document counterLookup, Document counterIncrement) {
			this.counterLookup = counterLookup;
			this.counterIncrement = counterIncrement;
		}

		@Override
		public Long nextValue() {
			return mongoOperations
					.execute(collectionName,
							collection -> collection.findOneAndUpdate(counterLookup, counterIncrement, UPDATE_OPTIONS))
					.getLong(valueField);
		}
	}

	private class SequenceValueInitializer implements SequenceInitializer<Long> {

		public static final InitializationResult<Long> SEQUENCE_EXISTS = new InitializationResult<>(
				InitializationState.EXISTS, null);

		private final Document counterLookup;
		private final @Nullable Long startWith;
		private final Object lock = new Object();

		volatile InitializationState state;

		public SequenceValueInitializer(Document counterLookup, @Nullable Long startWith) {

			this.counterLookup = counterLookup;
			this.startWith = startWith;
			this.state = InitializationState.PENDING;
		}

		@Override
		public InitializationResult<Long> initialize() {

			if (state == InitializationState.EXISTS) {
				return SEQUENCE_EXISTS;
			}

			synchronized (lock) {

				if (state == InitializationState.INITIALIZED) {
					state = InitializationState.EXISTS;
				}

				if (state != InitializationState.PENDING) {
					return new InitializationResult<>(state, null);
				}

				Document update = new Document("$setOnInsert", new Document(valueField, startWith));

				UpdateResult result = mongoOperations.execute(collectionName,
						collection -> collection.updateOne(counterLookup, update, new UpdateOptions().upsert(true)));

				this.state = result.getMatchedCount() == 0 ? InitializationState.INITIALIZED : InitializationState.EXISTS;

				return state.equals(InitializationState.INITIALIZED)
						? new InitializationResult<>(InitializationState.INITIALIZED, startWith)
						: SEQUENCE_EXISTS;
			}
		}

		public InitializationState getState() {
			return state;
		}
	}
}
