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

/**
 * Controls how a sequence is laid out as a document.
 * <p>
 * A counter layout keeps only the current value, so the increment and start value live in the
 * {@link SequenceDefinition} and are supplied by the client on every call. A self describing layout writes those
 * settings into the document as well, which lets a sequence be used consistently by clients that do not share the same
 * configuration - at the cost of having to agree on what the document says. {@link DefinitionPolicy} decides who wins
 * when they disagree.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
public interface SequenceStrategy {

	/**
	 * The document to insert when the sequence is used for the first time.
	 *
	 * @param definition the sequence being initialized, must not be {@literal null}.
	 * @return the initial document, never {@literal null}.
	 */
	Document initialDocument(SequenceDefinition definition);

	/**
	 * The update to apply on every call to obtain the next value.
	 *
	 * @param definition the sequence being incremented, must not be {@literal null}.
	 * @return the update document, never {@literal null}.
	 */
	Document incrementUpdate(SequenceDefinition definition);

	/**
	 * Read the current value from a stored sequence document.
	 *
	 * @param document the stored document, must not be {@literal null}.
	 * @return the current value.
	 */
	long currentValue(Document document);

	/**
	 * Whether the settings a client holds are also written to the document, and therefore have to be reconciled with it.
	 * Only self describing layouts need a {@link DefinitionPolicy}.
	 *
	 * @return {@literal true} if the layout stores its own definition.
	 */
	default boolean isSelfDescribing() {
		return false;
	}
}
