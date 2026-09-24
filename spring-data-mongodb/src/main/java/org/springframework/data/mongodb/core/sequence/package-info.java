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
/**
 * MongoDB backed sequences, for the monotonically increasing numbers MongoDB has no native type for.
 *
 * <h2>Usage</h2>
 *
 * <pre class="code">
 * MongoSequences sequences = MongoSequences.create(dbFactory);
 *
 * long nextOrderId = sequences.numericSequence("orders").nextValue(); // 1, 2, 3 ...
 * long nextInvoiceId = sequences.numericSequence("invoices", it -&gt; it.startWith(1000).increment(10)).nextValue();
 * </pre>
 *
 * Settings given to {@link org.springframework.data.mongodb.core.sequence.MongoSequences#builder} apply to every
 * sequence obtained from that instance:
 *
 * <pre class="code">
 * MongoSequences sequences = MongoSequences.builder(dbFactory)
 * 		.defaultCollection("__sequences")
 * 		.executor(new SessionBoundSequenceExecutor())
 * 		.strategy(new CounterSequenceStrategy())
 * 		.build();
 * </pre>
 *
 * <h2>Concurrency</h2>
 * <p>
 * Every value comes from an atomic update, so a sequence is safe to share between threads and between application
 * instances. Sequence objects hold no state and are cheap to obtain.
 *
 * <h2>Transactions</h2>
 * <p>
 * {@link org.springframework.data.mongodb.core.sequence.SequenceExecutor} decides whether an increment joins an
 * ongoing transaction. Staying outside it, the default, leaves a gap when a transaction rolls back but never hands the
 * same value to two callers. Joining it keeps the sequence contiguous but puts rolled back values back into
 * circulation.
 *
 * <h2>Document layout</h2>
 * <p>
 * {@link org.springframework.data.mongodb.core.sequence.CounterSequenceStrategy}, the default, stores only the current
 * value:
 *
 * <pre>
 * { "_id": "orders", "value": NumberLong(123) }
 * </pre>
 *
 * {@link org.springframework.data.mongodb.core.sequence.SelfDescribingSequenceStrategy} also writes the counting rules,
 * so clients configured differently can be told they disagree rather than quietly counting their own way:
 *
 * <pre>
 * { "_id": "orders", "value": NumberLong(123), "startValue": NumberLong(1), "increment": NumberLong(1) }
 * </pre>
 *
 * What happens on disagreement is up to
 * {@link org.springframework.data.mongodb.core.sequence.DefinitionPolicy}, which by default refuses the increment.
 * <p>
 * Sequences live in a collection named {@literal sequences} unless
 * {@link org.springframework.data.mongodb.core.sequence.SequenceDefinition.Builder#collection(String)} says otherwise.
 *
 * @author Jeongkyun An
 * @since 5.2
 * @see org.springframework.data.mongodb.core.sequence.MongoSequences
 * @see org.springframework.data.mongodb.core.sequence.SequenceDefinition
 */
package org.springframework.data.mongodb.core.sequence;
