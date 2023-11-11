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

import static java.util.UUID.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;

/**
 * @author Yan Kardziyaka
 */
class MongoTransactionUtilsUnitTests {

	@Test // GH-1628
	public void shouldThrowIllegalArgumentExceptionIfLabelsContainInvalidMaxCommitTime() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of("mongo:maxCommitTime=-PT5S"));

		assertThatThrownBy(() -> MongoTransactionUtils.extractOptions(attribute, fallbackOptions)) //
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-1628
	public void shouldThrowIllegalArgumentExceptionIfLabelsContainInvalidReadConcern() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of("mongo:readConcern=invalidValue"));

		assertThatThrownBy(() -> MongoTransactionUtils.extractOptions(attribute, fallbackOptions)) //
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-1628
	public void shouldThrowIllegalArgumentExceptionIfLabelsContainInvalidReadPreference() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of("mongo:readPreference=invalidValue"));

		assertThatThrownBy(() -> MongoTransactionUtils.extractOptions(attribute, fallbackOptions)) //
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-1628
	public void shouldThrowIllegalArgumentExceptionIfLabelsContainInvalidWriteConcern() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of("mongo:writeConcern=invalidValue"));

		assertThatThrownBy(() -> MongoTransactionUtils.extractOptions(attribute, fallbackOptions)) //
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-1628
	public void shouldReturnFallbackOptionsIfNotTransactionAttribute() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionDefinition definition = new DefaultTransactionDefinition();

		TransactionOptions result = MongoTransactionUtils.extractOptions(definition, fallbackOptions);

		assertThat(result).isSameAs(fallbackOptions);
	}

	@Test // GH-1628
	public void shouldReturnFallbackOptionsIfNoLabelsProvided() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		TransactionAttribute attribute = new DefaultTransactionAttribute();

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, fallbackOptions);

		assertThat(result).isSameAs(fallbackOptions);
	}

	@Test // GH-1628
	public void shouldReturnFallbackOptionsIfLabelsDoesNotContainValidOptions() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		Set<String> labels = Set.of("mongo:readConcern", "writeConcern", "readPreference=SECONDARY",
				"mongo:maxCommitTime PT5M", randomUUID().toString());
		attribute.setLabels(labels);

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, fallbackOptions);

		assertThat(result).isSameAs(fallbackOptions);
	}

	@Test // GH-1628
	public void shouldReturnMergedOptionsIfLabelsContainMaxCommitTime() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of("mongo:maxCommitTime=PT5S"));

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, fallbackOptions);

		assertThat(result).isNotSameAs(fallbackOptions) //
				.returns(5L, from(options -> options.getMaxCommitTime(TimeUnit.SECONDS))) //
				.returns(ReadConcern.AVAILABLE, from(TransactionOptions::getReadConcern)) //
				.returns(ReadPreference.secondaryPreferred(), from(TransactionOptions::getReadPreference)) //
				.returns(WriteConcern.UNACKNOWLEDGED, from(TransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnMergedOptionsIfLabelsContainReadConcern() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of("mongo:readConcern=majority"));

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, fallbackOptions);

		assertThat(result).isNotSameAs(fallbackOptions) //
				.returns(1L, from(options -> options.getMaxCommitTime(TimeUnit.MINUTES))) //
				.returns(ReadConcern.MAJORITY, from(TransactionOptions::getReadConcern)) //
				.returns(ReadPreference.secondaryPreferred(), from(TransactionOptions::getReadPreference)) //
				.returns(WriteConcern.UNACKNOWLEDGED, from(TransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnMergedOptionsIfLabelsContainReadPreference() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of("mongo:readPreference=primaryPreferred"));

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, fallbackOptions);

		assertThat(result).isNotSameAs(fallbackOptions) //
				.returns(1L, from(options -> options.getMaxCommitTime(TimeUnit.MINUTES))) //
				.returns(ReadConcern.AVAILABLE, from(TransactionOptions::getReadConcern)) //
				.returns(ReadPreference.primaryPreferred(), from(TransactionOptions::getReadPreference)) //
				.returns(WriteConcern.UNACKNOWLEDGED, from(TransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnMergedOptionsIfLabelsContainWriteConcern() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		attribute.setLabels(Set.of("mongo:writeConcern=w3"));

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, fallbackOptions);

		assertThat(result).isNotSameAs(fallbackOptions) //
				.returns(1L, from(options -> options.getMaxCommitTime(TimeUnit.MINUTES))) //
				.returns(ReadConcern.AVAILABLE, from(TransactionOptions::getReadConcern)) //
				.returns(ReadPreference.secondaryPreferred(), from(TransactionOptions::getReadPreference)) //
				.returns(WriteConcern.W3, from(TransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnNewOptionsIfLabelsContainAllOptions() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		Set<String> labels = Set.of("mongo:maxCommitTime=PT5S", "mongo:readConcern=majority",
				"mongo:readPreference=primaryPreferred", "mongo:writeConcern=w3");
		attribute.setLabels(labels);

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, fallbackOptions);

		assertThat(result).isNotSameAs(fallbackOptions) //
				.returns(5L, from(options -> options.getMaxCommitTime(TimeUnit.SECONDS))) //
				.returns(ReadConcern.MAJORITY, from(TransactionOptions::getReadConcern)) //
				.returns(ReadPreference.primaryPreferred(), from(TransactionOptions::getReadPreference)) //
				.returns(WriteConcern.W3, from(TransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnMergedOptionsIfLabelsContainOptionsMixedWithOrdinaryStrings() {
		TransactionOptions fallbackOptions = getTransactionOptions();
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		Set<String> labels = Set.of("mongo:maxCommitTime=PT5S", "mongo:nonExistentOption=value", "label",
				"mongo:writeConcern=w3");
		attribute.setLabels(labels);

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, fallbackOptions);

		assertThat(result).isNotSameAs(fallbackOptions) //
				.returns(5L, from(options -> options.getMaxCommitTime(TimeUnit.SECONDS))) //
				.returns(ReadConcern.AVAILABLE, from(TransactionOptions::getReadConcern)) //
				.returns(ReadPreference.secondaryPreferred(), from(TransactionOptions::getReadPreference)) //
				.returns(WriteConcern.W3, from(TransactionOptions::getWriteConcern));
	}

	@Test // GH-1628
	public void shouldReturnNewOptionsIFallbackIsNull() {
		DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
		Set<String> labels = Set.of("mongo:maxCommitTime=PT5S", "mongo:writeConcern=w3");
		attribute.setLabels(labels);

		TransactionOptions result = MongoTransactionUtils.extractOptions(attribute, null);

		assertThat(result).returns(5L, from(options -> options.getMaxCommitTime(TimeUnit.SECONDS))) //
				.returns(null, from(TransactionOptions::getReadConcern)) //
				.returns(null, from(TransactionOptions::getReadPreference)) //
				.returns(WriteConcern.W3, from(TransactionOptions::getWriteConcern));
	}

	private TransactionOptions getTransactionOptions() {
		return TransactionOptions.builder() //
				.maxCommitTime(1L, TimeUnit.MINUTES) //
				.readConcern(ReadConcern.AVAILABLE) //
				.readPreference(ReadPreference.secondaryPreferred()) //
				.writeConcern(WriteConcern.UNACKNOWLEDGED).build();
	}
}
