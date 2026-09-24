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

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.util.Assert;

/**
 * Collects the settings shared by every sequence obtained from a {@link MongoSequences} instance, so they do not have
 * to be repeated on each {@link SequenceDefinition}.
 *
 * @author Jeongkyun An
 * @since 5.2
 * @see MongoSequences#builder(MongoDatabaseFactory)
 */
public class MongoSequencesBuilder {

	private final MongoDatabaseFactory dbFactory;

	private @Nullable String defaultDatabase;
	private String defaultCollection = SequenceDefinition.DEFAULT_COLLECTION_NAME;
	private SequenceExecutor executor = new DefaultSequenceExecutor();
	private SequenceStrategy strategy = new CounterSequenceStrategy();
	private DefinitionPolicy definitionPolicy = DefinitionPolicy.validate();

	MongoSequencesBuilder(MongoDatabaseFactory dbFactory) {
		this.dbFactory = dbFactory;
	}

	/**
	 * The database sequences live in. Defaults to the one the factory is bound to.
	 *
	 * @param databaseName must not be {@literal null} or empty.
	 */
	public MongoSequencesBuilder defaultDatabase(String databaseName) {

		Assert.hasText(databaseName, "Database name must not be null or empty");

		this.defaultDatabase = databaseName;
		return this;
	}

	/**
	 * The collection sequences live in. Defaults to {@literal sequences}.
	 *
	 * @param collectionName must not be {@literal null} or empty.
	 */
	public MongoSequencesBuilder defaultCollection(String collectionName) {

		Assert.hasText(collectionName, "Collection name must not be null or empty");

		this.defaultCollection = collectionName;
		return this;
	}

	/**
	 * How increments relate to an ongoing transaction. Defaults to {@link DefaultSequenceExecutor}.
	 *
	 * @param executor must not be {@literal null}.
	 */
	public MongoSequencesBuilder executor(SequenceExecutor executor) {

		Assert.notNull(executor, "SequenceExecutor must not be null");

		this.executor = executor;
		return this;
	}

	/**
	 * How sequences are laid out as documents. Defaults to {@link CounterSequenceStrategy}.
	 *
	 * @param strategy must not be {@literal null}.
	 */
	public MongoSequencesBuilder strategy(SequenceStrategy strategy) {

		Assert.notNull(strategy, "SequenceStrategy must not be null");

		this.strategy = strategy;
		return this;
	}

	/**
	 * What to do when a self describing sequence disagrees with the definition asked for. Defaults to
	 * {@link DefinitionPolicy#validate()}.
	 *
	 * @param definitionPolicy must not be {@literal null}.
	 */
	public MongoSequencesBuilder definitionPolicy(DefinitionPolicy definitionPolicy) {

		Assert.notNull(definitionPolicy, "DefinitionPolicy must not be null");

		this.definitionPolicy = definitionPolicy;
		return this;
	}

	/**
	 * @return new instance of {@link MongoSequences}, never {@literal null}.
	 */
	public MongoSequences build() {
		return new DefaultMongoSequences(dbFactory, executor, this::seed);
	}

	/**
	 * A builder already carrying these defaults, for a caller to depart from.
	 */
	private SequenceDefinition.Builder seed(String sequenceName) {

		SequenceDefinition.Builder builder = SequenceDefinition.builder(sequenceName).collection(defaultCollection)
				.strategy(strategy).definitionPolicy(definitionPolicy);

		if (defaultDatabase != null) {
			builder.database(defaultDatabase);
		}

		return builder;
	}
}
