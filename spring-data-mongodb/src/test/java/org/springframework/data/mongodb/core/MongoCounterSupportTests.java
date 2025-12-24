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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * Tests for {@link MongoCounterSupport}.
 *
 * @author Jeongkyun An
 */
class MongoCounterSupportTests {

	@Template
	static MongoTestTemplate template;

	private static final String COUNTER_COLLECTION = "counters";

	private MongoCounterSupport counterSupport;

	@BeforeEach
	void setUp() {
		template.dropCollection(COUNTER_COLLECTION);
		counterSupport = new MongoCounterSupport(template);
	}

	@Test // GH-4823
	void shouldIncrementCounterAtomically() {

		long value1 = counterSupport.getNextSequenceValue("test-counter", COUNTER_COLLECTION);
		long value2 = counterSupport.getNextSequenceValue("test-counter", COUNTER_COLLECTION);
		long value3 = counterSupport.getNextSequenceValue("test-counter", COUNTER_COLLECTION);
		long value4 = counterSupport.getNextSequenceValue("test-counter", COUNTER_COLLECTION);
		long value5 = counterSupport.getNextSequenceValue("test-counter", COUNTER_COLLECTION);

		assertThat(value1).isEqualTo(1L);
		assertThat(value2).isEqualTo(2L);
		assertThat(value3).isEqualTo(3L);
		assertThat(value4).isEqualTo(4L);
		assertThat(value5).isEqualTo(5L);
	}

	@Test // GH-4823
	void shouldCreateCounterDocumentOnFirstCall() {

		long firstValue = counterSupport.getNextSequenceValue("new-counter", COUNTER_COLLECTION);

		assertThat(firstValue).isEqualTo(1L);

		long secondValue = counterSupport.getNextSequenceValue("new-counter", COUNTER_COLLECTION);

		assertThat(secondValue).isEqualTo(2L);
	}

	@Test // GH-4823
	void shouldMaintainIndependentCounters() {

		long counter1Value1 = counterSupport.getNextSequenceValue("counter-1", COUNTER_COLLECTION);
		long counter2Value1 = counterSupport.getNextSequenceValue("counter-2", COUNTER_COLLECTION);
		long counter1Value2 = counterSupport.getNextSequenceValue("counter-1", COUNTER_COLLECTION);
		long counter2Value2 = counterSupport.getNextSequenceValue("counter-2", COUNTER_COLLECTION);

		assertThat(counter1Value1).isEqualTo(1L);
		assertThat(counter2Value1).isEqualTo(1L);
		assertThat(counter1Value2).isEqualTo(2L);
		assertThat(counter2Value2).isEqualTo(2L);
	}

}
