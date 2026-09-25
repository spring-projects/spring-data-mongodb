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
 * Decides what happens when a self describing sequence document disagrees with the {@link SequenceDefinition} the
 * client holds, for instance because someone else created the sequence with a different increment.
 * <p>
 * The default is {@link #validate()}, which refuses to hand out a value it cannot vouch for. The other two carry on
 * instead, and differ in which side is taken as the truth.
 *
 * @author Jeongkyun An
 * @since 5.2
 * @see SequenceStrategy#isSelfDescribing()
 */
@FunctionalInterface
public interface DefinitionPolicy {

	/**
	 * Reconcile the definition held by the client with the one stored alongside the sequence.
	 *
	 * @param definition the definition the client asked for, must not be {@literal null}.
	 * @param stored the document currently holding the sequence, must not be {@literal null}.
	 * @return the definition to increment with, never {@literal null}.
	 * @throws SequenceDefinitionMismatchException if the two cannot be reconciled.
	 */
	SequenceDefinition reconcile(SequenceDefinition definition, Document stored);

	/**
	 * Check the stored shape on every increment and fail if it does not match. This is the default because a sequence
	 * that silently changes its increment is worse than one that stops working.
	 *
	 * @return a policy rejecting any mismatch, never {@literal null}.
	 */
	static DefinitionPolicy validate() {
		return (definition, stored) -> {

			SequenceDefinition storedDefinition = SelfDescribingSequenceStrategy.readDefinition(definition, stored);

			if (!storedDefinition.describesSameSequenceAs(definition)) {
				throw new SequenceDefinitionMismatchException(definition, storedDefinition);
			}

			return definition;
		};
	}

	/**
	 * Take the stored definition, so a sequence behaves the same no matter who increments it.
	 *
	 * @return a policy preferring the stored definition, never {@literal null}.
	 */
	static DefinitionPolicy serverWins() {
		return SelfDescribingSequenceStrategy::readDefinition;
	}

	/**
	 * Keep the definition the client asked for and leave the document as it is. The stored settings become a record of
	 * how the sequence started rather than how it currently behaves.
	 *
	 * @return a policy preferring the local definition, never {@literal null}.
	 */
	static DefinitionPolicy clientWins() {
		return (definition, stored) -> definition;
	}
}
