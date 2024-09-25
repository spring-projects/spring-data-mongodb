/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb;

import static org.assertj.core.api.Assertions.*;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

/**
 * Unit tests for {@link DefaultMongoTransactionOptionsResolver}.
 *
 * @author Yan Kardziyaka
 * @author Christoph Strobl
 */
class DefaultMongoTransactionOptionsResolverUnitTests {

	@ParameterizedTest
	@ValueSource(strings = { "mongo:maxCommitTime=-PT5S", "mongo:readConcern=invalidValue",
			"mongo:readPreference=invalidValue", "mongo:writeConcern=invalidValue", "mongo:invalidPreference=jedi",
			"mongo:readConcern", "mongo:readConcern:local", "mongo:readConcern=" })
	void shouldThrowExceptionOnInvalidAttribute(String label) {

		TransactionAttribute attribute = transactionAttribute(label);

		assertThatThrownBy(() -> DefaultMongoTransactionOptionsResolver.INSTANCE.resolve(attribute)) //
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-1628
	public void shouldReturnEmptyOptionsIfNotTransactionAttribute() {

		DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
		assertThat(DefaultMongoTransactionOptionsResolver.INSTANCE.resolve(definition))
				.isSameAs(MongoTransactionOptions.NONE);
	}

	@Test // GH-1628
	public void shouldReturnEmptyOptionsIfNoLabelsProvided() {

		TransactionAttribute attribute = new DefaultTransactionAttribute();

		assertThat(DefaultMongoTransactionOptionsResolver.INSTANCE.resolve(attribute))
				.isSameAs(MongoTransactionOptions.NONE);
	}

	@Test // GH-1628
	public void shouldIgnoreNonMongoOptions() {

		TransactionAttribute attribute = transactionAttribute("jpa:ignore");

		assertThat(DefaultMongoTransactionOptionsResolver.INSTANCE.resolve(attribute))
				.isSameAs(MongoTransactionOptions.NONE);
	}

	@Test // GH-1628
	public void shouldReturnMergedOptionsIfLabelsContainMaxCommitTime() {

		TransactionAttribute attribute = transactionAttribute("mongo:maxCommitTime=PT5S");

		assertThat(DefaultMongoTransactionOptionsResolver.INSTANCE.resolve(attribute))
				.returns(5L, from(options -> options.getMaxCommitTime().toSeconds())) //
				.returns(null, from(MongoTransactionOptions::getReadConcern)) //
				.returns(null, from(MongoTransactionOptions::getReadPreference)) //
				.returns(null, from(MongoTransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnReadConcernWhenPresent() {

		TransactionAttribute attribute = transactionAttribute("mongo:readConcern=majority");

		assertThat(DefaultMongoTransactionOptionsResolver.INSTANCE.resolve(attribute))
				.returns(null, from(TransactionMetadata::getMaxCommitTime)) //
				.returns(ReadConcern.MAJORITY, from(MongoTransactionOptions::getReadConcern)) //
				.returns(null, from(MongoTransactionOptions::getReadPreference)) //
				.returns(null, from(MongoTransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnMergedOptionsIfLabelsContainReadPreference() {

		TransactionAttribute attribute = transactionAttribute("mongo:readPreference=primaryPreferred");

		assertThat(DefaultMongoTransactionOptionsResolver.INSTANCE.resolve(attribute))
				.returns(null, from(TransactionMetadata::getMaxCommitTime)) //
				.returns(null, from(MongoTransactionOptions::getReadConcern)) //
				.returns(ReadPreference.primaryPreferred(), from(MongoTransactionOptions::getReadPreference)) //
				.returns(null, from(MongoTransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnMergedOptionsIfLabelsContainWriteConcern() {

		TransactionAttribute attribute = transactionAttribute("mongo:writeConcern=w3");

		assertThat(DefaultMongoTransactionOptionsResolver.INSTANCE.resolve(attribute))
				.returns(null, from(TransactionMetadata::getMaxCommitTime)) //
				.returns(null, from(MongoTransactionOptions::getReadConcern)) //
				.returns(null, from(MongoTransactionOptions::getReadPreference)) //
				.returns(WriteConcern.W3, from(MongoTransactionOptions::getWriteConcern));

	}

	private static TransactionAttribute transactionAttribute(String... labels) {

		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of(labels));
		return attribute;
	}
}
