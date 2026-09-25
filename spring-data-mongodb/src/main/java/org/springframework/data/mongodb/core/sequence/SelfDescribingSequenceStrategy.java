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

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * Writes the counting rules into the document next to the value, as in
 * {@code { _id: "orders", value: 41, startValue: 1, increment: 1 }}.
 * <p>
 * The point is that the sequence keeps behaving the same whoever increments it, since a client that disagrees can be
 * told so rather than quietly counting differently. What happens on disagreement is up to the {@link DefinitionPolicy}.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
public class SelfDescribingSequenceStrategy extends CounterSequenceStrategy {

	static final String START_VALUE_FIELD = "startValue";
	static final String INCREMENT_FIELD = "increment";

	@Override
	public Document initialDocument(SequenceDefinition definition) {

		Document document = super.initialDocument(definition);
		document.put(START_VALUE_FIELD, definition.getStartValue());
		document.put(INCREMENT_FIELD, definition.getIncrement());

		return document;
	}

	@Override
	public boolean isSelfDescribing() {
		return true;
	}

	/**
	 * Rebuild a definition from what the document says, falling back to the local one for anything it does not carry.
	 *
	 * @param definition the definition held by the client, must not be {@literal null}.
	 * @param stored the stored sequence document, must not be {@literal null}.
	 * @return the stored definition, never {@literal null}.
	 */
	static SequenceDefinition readDefinition(SequenceDefinition definition, Document stored) {

		Assert.notNull(definition, "SequenceDefinition must not be null");
		Assert.notNull(stored, "Document must not be null");

		SequenceDefinition.Builder builder = SequenceDefinition.builder(definition);

		if (stored.get(START_VALUE_FIELD) instanceof Number startValue) {
			builder.startWith(startValue.longValue());
		}

		if (stored.get(INCREMENT_FIELD) instanceof Number increment) {
			builder.increment(increment.longValue());
		}

		return builder.build();
	}
}
