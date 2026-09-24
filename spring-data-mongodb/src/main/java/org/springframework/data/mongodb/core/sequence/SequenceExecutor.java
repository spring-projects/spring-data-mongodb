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

import org.springframework.data.mongodb.MongoDatabaseFactory;

/**
 * Obtains the next value of a sequence, and decides how that relates to an ongoing transaction.
 * <p>
 * Running outside the transaction, as {@link DefaultSequenceExecutor} does, means a rolled back transaction leaves a
 * hole in the sequence but never hands the same value to two callers. Running inside it, as
 * {@link SessionBoundSequenceExecutor} does, keeps the sequence unbroken but puts values back into circulation on
 * rollback. Which one is right depends on whether the numbers are merely unique or are expected to be contiguous.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
public interface SequenceExecutor {

	/**
	 * Obtain the next value, creating the sequence if this is its first use.
	 *
	 * @param definition the sequence to increment, must not be {@literal null}.
	 * @param dbFactory used to reach the database, must not be {@literal null}.
	 * @return the next value.
	 */
	long nextValue(SequenceDefinition definition, MongoDatabaseFactory dbFactory);
}
