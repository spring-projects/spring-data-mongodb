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
 * Stores nothing but the current value, as in {@code { _id: "orders", value: 41 }}.
 * <p>
 * The increment and start value stay on the client, which keeps the document small and the update a plain
 * {@code $inc}. Two clients configured differently will therefore count differently against the same sequence; use
 * {@link SelfDescribingSequenceStrategy} if that matters.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
public class CounterSequenceStrategy implements SequenceStrategy {

	static final String VALUE_FIELD = "value";
	static final String ID_FIELD = "_id";

	@Override
	public Document initialDocument(SequenceDefinition definition) {

		Assert.notNull(definition, "SequenceDefinition must not be null");

		Document document = new Document(VALUE_FIELD, definition.getStartValue());
		document.putAll(definition.getAttributes());

		return document;
	}

	@Override
	public Document incrementUpdate(SequenceDefinition definition) {

		Assert.notNull(definition, "SequenceDefinition must not be null");

		return new Document("$inc", new Document(VALUE_FIELD, definition.getIncrement()));
	}

	@Override
	public long currentValue(Document document) {

		Assert.notNull(document, "Document must not be null");

		Object value = document.get(VALUE_FIELD);

		if (!(value instanceof Number number)) {
			throw new IllegalStateException(
					"Sequence document %s does not hold a numeric '%s'".formatted(document.get(ID_FIELD), VALUE_FIELD));
		}

		return number.longValue();
	}
}
