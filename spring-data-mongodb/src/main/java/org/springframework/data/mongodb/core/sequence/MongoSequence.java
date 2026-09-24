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

/**
 * Represents a MongoDB-backed sequence that provides monotonically increasing values using atomic operations.
 * <p>
 * Sequences are thread-safe and can be used across multiple application instances. Each call to {@link #nextValue()}
 * returns a unique value, guaranteed by MongoDB's atomic findAndModify operation.
 * <p>
 * Sequences are obtained from {@link MongoSequences}:
 *
 * <pre class="code">
 * MongoSequences sequences = MongoSequences.create(dbFactory);
 * MongoSequence&lt;Long&gt; orderIds = sequences.numericSequence("orders");
 * long nextId = orderIds.nextValue(); // 1, 2, 3 ...
 * </pre>
 *
 * @param <T> the type of sequence value.
 * @author Jeongkyun An
 * @since 5.2
 * @see MongoSequences
 * @see <a href="https://github.com/spring-projects/spring-data-mongodb/issues/4823">GH-4823</a>
 */
public interface MongoSequence<T extends Number> {

	/**
	 * Get the next value in the sequence atomically.
	 * <p>
	 * This operation is thread-safe and uses MongoDB's atomic findAndModify to ensure each value is returned exactly
	 * once, even under high concurrency or across multiple application instances.
	 *
	 * @return the next sequence value, never {@literal null}
	 */
	T nextValue();

	/**
	 * Get the name of this sequence.
	 *
	 * @return sequence name, never {@literal null}
	 */
	String getName();
}
