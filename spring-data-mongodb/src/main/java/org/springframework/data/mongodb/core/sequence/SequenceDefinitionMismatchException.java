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

import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * Thrown when a self describing sequence is incremented with a definition that does not match the one stored with it,
 * and the {@link DefinitionPolicy} is not willing to pick a side.
 *
 * @author Jeongkyun An
 * @since 5.2
 * @see DefinitionPolicy#validate()
 */
public class SequenceDefinitionMismatchException extends InvalidDataAccessApiUsageException {

	private static final long serialVersionUID = 1L;

	private final transient SequenceDefinition requested;
	private final transient SequenceDefinition stored;

	/**
	 * @param requested the definition the client asked for, must not be {@literal null}.
	 * @param stored the definition read back from the sequence document, must not be {@literal null}.
	 */
	public SequenceDefinitionMismatchException(SequenceDefinition requested, SequenceDefinition stored) {

		super("Sequence '%s' is stored as %s but was requested as %s".formatted(requested.getName(), stored, requested));

		this.requested = requested;
		this.stored = stored;
	}

	/**
	 * @return the definition the client asked for, never {@literal null}.
	 */
	public SequenceDefinition getRequested() {
		return requested;
	}

	/**
	 * @return the definition read back from the sequence document, never {@literal null}.
	 */
	public SequenceDefinition getStored() {
		return stored;
	}
}
