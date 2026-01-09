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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * @author Christoph Strobl
 */
public class MongoSequenceTests {

	public static final String COLLECTION_NAME = "__sequences";
	public static final String SEQUENCE_NAME = "sequence-1";

	@Template //
	static MongoTestTemplate template;
	MongoSequences sequences;

	@BeforeEach
	public void beforeEach() {

		template.flush(COLLECTION_NAME);

		sequences = new MongoSequences(template);
	}

	@Test // GH-4823
	void zeroIncrementErrors() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> new NumericMongoSequence(template, COLLECTION_NAME, "zero-counter", 0));
	}

	@Test // GH-4823
	void newSequence() {

		MongoSequence<Long> sequence = sequences.numericSequence(SEQUENCE_NAME, cfg -> cfg.incrementBy(2L));

		assertThat(sequence.nextValue()).isEqualTo(2L);
		assertThat(sequence.nextValue()).isEqualTo(4L);
		assertThat(sequence.nextValue()).isEqualTo(6L);
	}

	@Test // GH-4823
	void sequenceToString() {

		MongoSequence<Long> sequence = sequences.numericSequence(SEQUENCE_NAME, cfg -> cfg.incrementBy(2L));

		assertThat(sequence.toString()).isEqualTo("sequence-1.counter");

		sequence.nextValue();
		assertThat(sequence.toString()).isEqualTo("sequence-1.counter=2");

		sequence.nextValue();
		assertThat(sequence.toString()).isEqualTo("sequence-1.counter=4");
	}

	@Test // GH-4823
	void newSequenceStartingWithValue() {

		MongoSequence<Long> sequence = sequences.numericSequence(SEQUENCE_NAME, cfg -> cfg.startWith(0L).incrementBy(2L));

		assertThat(sequence.nextValue()).isEqualTo(0L);
		assertThat(sequence.nextValue()).isEqualTo(2L);
		assertThat(sequence.nextValue()).isEqualTo(4L);
	}

	@Test // GH-4823
	void resumeExistingSequence() {

		preInitializeSequence(SEQUENCE_NAME, "counter", 100L);

		MongoSequence<Long> sequence = sequences.numericSequence(SEQUENCE_NAME, cfg -> cfg.incrementBy(2L));

		assertThat(sequence.nextValue()).isEqualTo(102L);
		assertThat(sequence.nextValue()).isEqualTo(104L);
		assertThat(sequence.nextValue()).isEqualTo(106L);
	}

	@Test // GH-4823
	void resumeExistingCounterStartingWith() {

		preInitializeSequence(SEQUENCE_NAME, "counter", 10L);

		MongoSequence<Long> sequence = sequences.numericSequence(SEQUENCE_NAME, cfg -> cfg.startWith(10L).incrementBy(2L));

		assertThat(sequence.nextValue()).isEqualTo(12L);
		assertThat(sequence.nextValue()).isEqualTo(14L);
		assertThat(sequence.nextValue()).isEqualTo(16L);
	}

	@Test // GH-4823
	void concurrentSequenceInitializationStartingWith() throws InterruptedException {

		int threadCount = 10;
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch finishLatch = new CountDownLatch(threadCount);
		List<Long> values = Collections.synchronizedList(new ArrayList<>());
		Set<Long> uniqueInitialValues = ConcurrentHashMap.newKeySet();

		MongoSequence<Long> sequence = sequences.numericSequence("concurrent-init", cfg -> cfg.startWith(100L));

		for (int i = 0; i < threadCount; i++) {
			executor.submit(() -> {
				try {
					startLatch.await();
					Long value = sequence.nextValue();
					values.add(value);
					if (value == 100L) {
						uniqueInitialValues.add(value);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} finally {
					finishLatch.countDown();
				}
			});
		}

		startLatch.countDown();
		finishLatch.await(5, TimeUnit.SECONDS);
		executor.shutdown();

		// Only one thread should get the initial value (100)
		assertThat(uniqueInitialValues).hasSize(1);
		assertThat(uniqueInitialValues).contains(100L);

		// All values should be unique and sequential
		assertThat(values).hasSize(threadCount);
		assertThat(values).containsExactlyInAnyOrderElementsOf(
				IntStream.range(0, threadCount).mapToLong(i -> 100L + i).boxed().collect(Collectors.toList()));
	}

	@Test // GH-4823
	void concurrentSequenceAccessAfterInitialization() throws InterruptedException {

		int threadCount = 10;
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch finishLatch = new CountDownLatch(threadCount);
		List<Long> values = Collections.synchronizedList(new ArrayList<>());

		MongoSequence<Long> sequence = sequences.numericSequence("concurrent-after-init", cfg -> cfg.startWith(50L));

		assertThat(sequence.nextValue()).isEqualTo(50L);

		for (int i = 0; i < threadCount; i++) {
			executor.submit(() -> {
				try {
					startLatch.await();
					values.add(sequence.nextValue());
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} finally {
					finishLatch.countDown();
				}
			});
		}

		startLatch.countDown();
		finishLatch.await(5, TimeUnit.SECONDS);
		executor.shutdown();

		// All values should be unique and sequential starting from 51
		assertThat(values).hasSize(threadCount);
		assertThat(values).containsExactlyInAnyOrderElementsOf(
				IntStream.range(1, threadCount + 1).mapToLong(i -> 50L + i).boxed().collect(Collectors.toList()));
	}

	@Test // GH-4823
	void sequenceWithNegativeIncrement() {

		MongoSequence<Long> sequence = sequences.numericSequence("negative-counter", cfg -> cfg.incrementBy(-2L));

		assertThat(sequence.nextValue()).isEqualTo(-2L);
		assertThat(sequence.nextValue()).isEqualTo(-4L);
		assertThat(sequence.nextValue()).isEqualTo(-6L);
	}

	@Test // GH-4823
	void sequenceWithNegativeIncrementStartingAt() {

		MongoSequence<Long> sequence = sequences.numericSequence("negative-counter",
				cfg -> cfg.startWith(10L).incrementBy(-3L));

		assertThat(sequence.nextValue()).isEqualTo(10L);
		assertThat(sequence.nextValue()).isEqualTo(7L);
		assertThat(sequence.nextValue()).isEqualTo(4L);
		assertThat(sequence.nextValue()).isEqualTo(1L);
	}

	@Test // GH-4823
	void sequenceWithCustomFieldName() {

		MongoSequence<Long> sequence = sequences.numericSequence("custom-field-counter",
				cfg -> cfg.inCollection(COLLECTION_NAME).counterName("sequence").incrementBy(3L));

		assertThat(sequence.nextValue()).isEqualTo(3L);
		assertThat(sequence.nextValue()).isEqualTo(6L);
		assertThat(sequence.nextValue()).isEqualTo(9L);

		Document doc = template.execute(COLLECTION_NAME,
				collection -> collection.find(new Document("_id", "custom-field-counter")).first());

		assertThat(doc).containsEntry("sequence", 9L);
	}

	@Test // GH-4823
	void sequenceWithCustomFieldNameAndStartValue() {

		MongoSequence<Long> sequence = sequences.numericSequence("custom-field-counter",
				cfg -> cfg.inCollection(COLLECTION_NAME).counterName("seq").startWith(20L).incrementBy(2L));

		assertThat(sequence.nextValue()).isEqualTo(20L);
		assertThat(sequence.nextValue()).isEqualTo(22L);
		assertThat(sequence.nextValue()).isEqualTo(24L);

		Document doc = template.execute(COLLECTION_NAME,
				collection -> collection.find(new Document("_id", "custom-field-counter")).first());

		assertThat(doc).containsEntry("seq", 24L);
	}

	@Test // GH-4823
	void startCounterWithinExistingDocumentNotHavingCounterField() {

		template.execute(COLLECTION_NAME, collection -> {
			collection.insertOne(new Document("_id", "different-field-counter").append("some-field", 50L));
			return null;
		});

		MongoSequence<Long> sequence = sequences.numericSequence("different-field-counter",
				cfg -> cfg.inCollection(COLLECTION_NAME).incrementBy(2L));

		assertThat(sequence.nextValue()).isEqualTo(2L);
		assertThat(sequence.nextValue()).isEqualTo(4L);

		Document doc = template.execute(COLLECTION_NAME,
				collection -> collection.find(new Document("_id", "different-field-counter")).first());
		assertThat(doc).containsEntry("some-field", 50L).containsEntry("counter", 4L);
	}

	@Test // GH-4823
	void multipleCountersInOneDocument() {

		MongoSequence<Long> f_seq = sequences.numericSequence("s_1", cfg -> cfg.counterName("f-counter").incrementBy(6L));
		MongoSequence<Long> d_seq = sequences.numericSequence("s_1", cfg -> cfg.counterName("d-counter").incrementBy(4L));
		MongoSequence<Long> t_seq = sequences.numericSequence("s_1", cfg -> cfg.counterName("t-counter").incrementBy(20L));

		assertThat(f_seq.nextValue()).isEqualTo(6L);
		assertThat(d_seq.nextValue()).isEqualTo(4L);
		assertThat(t_seq.nextValue()).isEqualTo(20L);

		assertThat(t_seq.nextValue()).isEqualTo(40L);
		assertThat(d_seq.nextValue()).isEqualTo(8L);
		assertThat(f_seq.nextValue()).isEqualTo(12L);
	}

	private static void preInitializeSequence(String name, String field, Long value) {

		template.execute(COLLECTION_NAME, collection -> {
			collection.insertOne(new Document("_id", name).append(field, value));
			return null;
		});
	}
}
