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

import static org.assertj.core.api.Assertions.*;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * Integration tests for {@link MongoSequences}.
 *
 * @author Jeongkyun An
 */
class MongoSequencesIntegrationTests {

	@Template
	static MongoTestTemplate template;

	private static final String DEFAULT_COLLECTION = "sequences";

	@BeforeEach
	void setUp() {
		template.dropCollection(DEFAULT_COLLECTION);
	}

	@Test // GH-4823
	void shouldGenerateSequentialValues() {

		MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq = factory.numericSequence("test-seq");

		assertThat(seq.nextValue()).isEqualTo(1L);
		assertThat(seq.nextValue()).isEqualTo(2L);
		assertThat(seq.nextValue()).isEqualTo(3L);
		assertThat(seq.nextValue()).isEqualTo(4L);
		assertThat(seq.nextValue()).isEqualTo(5L);
	}

	@Test // GH-4823
	void shouldReturnSequenceName() {

		MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq = factory.numericSequence("my-sequence");

		assertThat(seq.getName()).isEqualTo("my-sequence");
	}

	@Test // GH-4823
	void shouldRespectCustomStartValue() {

		MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq = factory.numericSequence("custom-seq", it -> it.startWith(1000));

		assertThat(seq.nextValue()).isEqualTo(1000L);
		assertThat(seq.nextValue()).isEqualTo(1001L);
		assertThat(seq.nextValue()).isEqualTo(1002L);
	}

	@Test // GH-4823
	void shouldRespectCustomIncrement() {

		MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq = factory.numericSequence("increment-seq", it -> it.startWith(10).increment(5));

		assertThat(seq.nextValue()).isEqualTo(10L);
		assertThat(seq.nextValue()).isEqualTo(15L);
		assertThat(seq.nextValue()).isEqualTo(20L);
		assertThat(seq.nextValue()).isEqualTo(25L);
	}

	@Test // GH-4823
	void shouldMaintainIndependentSequences() {

		MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq1 = factory.numericSequence("seq-1");
		MongoSequence<Long> seq2 = factory.numericSequence("seq-2");

		assertThat(seq1.nextValue()).isEqualTo(1L);
		assertThat(seq2.nextValue()).isEqualTo(1L);
		assertThat(seq1.nextValue()).isEqualTo(2L);
		assertThat(seq2.nextValue()).isEqualTo(2L);
		assertThat(seq1.nextValue()).isEqualTo(3L);
		assertThat(seq2.nextValue()).isEqualTo(3L);
	}

	@Test // GH-4823
	void shouldPersistAcrossFactoryInstances() {

		MongoSequences factory1 = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq1 = factory1.numericSequence("persistent-seq");

		assertThat(seq1.nextValue()).isEqualTo(1L);
		assertThat(seq1.nextValue()).isEqualTo(2L);

		// Create new factory and sequence with same name
		MongoSequences factory2 = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq2 = factory2.numericSequence("persistent-seq");

		// Should continue from where seq1 left off
		assertThat(seq2.nextValue()).isEqualTo(3L);
		assertThat(seq2.nextValue()).isEqualTo(4L);
	}

	@Test // GH-4823
	void shouldUseCustomCollection() {

		String customCollection = "my-counters";
		template.dropCollection(customCollection);

		MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq = factory.numericSequence("test-seq", it -> it.collection(customCollection));

		long value = seq.nextValue();

		assertThat(value).isEqualTo(1L);

		// Verify document exists in custom collection
		assertThat(template.collectionExists(customCollection)).isTrue();
		Document doc = template.findById("test-seq", Document.class, customCollection);
		assertThat(doc).isNotNull();
		assertThat(doc.get("value")).isEqualTo(1L);
	}

	@Test // GH-4823
	void shouldStoreDocumentWithCorrectStructure() {

		MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq = factory.numericSequence("structure-test");

		seq.nextValue();
		seq.nextValue();
		seq.nextValue();

		Document doc = template.findById("structure-test", Document.class, DEFAULT_COLLECTION);

		assertThat(doc).isNotNull();
		assertThat(doc.get("_id")).isEqualTo("structure-test");
		assertThat(doc.get("value")).isEqualTo(3L);
		assertThat(doc.keySet()).containsExactlyInAnyOrder("_id", "value");
	}

	@Test // GH-4823
	void shouldHandleConcurrentAccessFromSameSequenceInstance() throws Exception {

		MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
		MongoSequence<Long> seq = factory.numericSequence("concurrent-seq");

		int threadCount = 10;
		int iterationsPerThread = 100;
		Set<Long> values = ConcurrentHashMap.newKeySet();
		CountDownLatch latch = new CountDownLatch(threadCount);

		for (int i = 0; i < threadCount; i++) {
			new Thread(() -> {
				for (int j = 0; j < iterationsPerThread; j++) {
					values.add(seq.nextValue());
				}
				latch.countDown();
			}).start();
		}

		latch.await();

		// Should have exactly 1000 unique values (1 to 1000)
		assertThat(values).hasSize(1000);
		assertThat(values).containsExactlyInAnyOrder(LongStream.rangeClosed(1, 1000).boxed().toArray(Long[]::new));
	}

	@Test // GH-4823
	void shouldHandleConcurrentAccessFromMultipleSequenceInstances() throws Exception {

		int threadCount = 10;
		int iterationsPerThread = 50;
		Set<Long> values = ConcurrentHashMap.newKeySet();
		CountDownLatch latch = new CountDownLatch(threadCount);

		for (int i = 0; i < threadCount; i++) {
			new Thread(() -> {
				// Each thread creates its own factory and sequence instance
				MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
				MongoSequence<Long> seq = factory.numericSequence("multi-instance-seq");

				for (int j = 0; j < iterationsPerThread; j++) {
					values.add(seq.nextValue());
				}
				latch.countDown();
			}).start();
		}

		latch.await();

		// Should have exactly 500 unique values (1 to 500)
		assertThat(values).hasSize(500);
		assertThat(values).containsExactlyInAnyOrder(LongStream.rangeClosed(1, 500).boxed().toArray(Long[]::new));
	}

	@Test // GH-4823
	void shouldHandleHighConcurrencyWithCustomConfiguration() throws Exception {

		int threadCount = 20;
		int iterationsPerThread = 25;
		Set<Long> values = ConcurrentHashMap.newKeySet();
		CountDownLatch latch = new CountDownLatch(threadCount);

		for (int i = 0; i < threadCount; i++) {
			new Thread(() -> {
				MongoSequences factory = MongoSequences.create(template.getMongoDatabaseFactory());
				MongoSequence<Long> seq = factory.numericSequence("high-concurrency-seq", it -> it.startWith(100).increment(2));

				for (int j = 0; j < iterationsPerThread; j++) {
					values.add(seq.nextValue());
				}
				latch.countDown();
			}).start();
		}

		latch.await();

		// Should have exactly 500 unique values
		assertThat(values).hasSize(500);

		// All values should be even numbers starting from 100
		assertThat(values).allMatch(v -> v >= 100 && v % 2 == 0);

		// Should contain values from 100 to 1098 (100 + 499*2)
		Long minValue = values.stream().min(Long::compare).orElse(0L);
		Long maxValue = values.stream().max(Long::compare).orElse(0L);

		assertThat(minValue).isEqualTo(100L);
		assertThat(maxValue).isEqualTo(1098L);
	}

	@Test // GH-4823
	void shouldStoreHowASelfDescribingSequenceCounts() {

		MongoSequences factory = selfDescribing(DefinitionPolicy.validate());
		MongoSequence<Long> seq = factory.numericSequence("described", it -> it.increment(5));

		assertThat(seq.nextValue()).isEqualTo(1L);
		assertThat(seq.nextValue()).isEqualTo(6L);

		Document stored = template.execute(DEFAULT_COLLECTION,
				collection -> collection.find(new Document("_id", "described")).first());

		assertThat(stored).containsEntry("startValue", 1L).containsEntry("increment", 5L);
	}

	@Test // GH-4823
	void shouldRefuseToCountASelfDescribingSequenceADifferentWay() {

		MongoSequences factory = selfDescribing(DefinitionPolicy.validate());
		factory.numericSequence("contested", it -> it.increment(5)).nextValue();

		MongoSequence<Long> disagreeing = factory.numericSequence("contested", it -> it.increment(1));

		assertThatExceptionOfType(SequenceDefinitionMismatchException.class).isThrownBy(disagreeing::nextValue);
	}

	@Test // GH-4823
	void serverWinsShouldAdoptTheStoredIncrement() {

		MongoSequences factory = selfDescribing(DefinitionPolicy.serverWins());
		factory.numericSequence("adopted", it -> it.increment(5)).nextValue();

		MongoSequence<Long> disagreeing = factory.numericSequence("adopted", it -> it.increment(1));

		assertThat(disagreeing.nextValue()).isEqualTo(6L);
	}

	private static MongoSequences selfDescribing(DefinitionPolicy policy) {

		return MongoSequences.builder(template.getMongoDatabaseFactory())
				.strategy(new SelfDescribingSequenceStrategy()).definitionPolicy(policy).build();
	}
}
