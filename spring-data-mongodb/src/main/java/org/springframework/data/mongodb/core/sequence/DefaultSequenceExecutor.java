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

import com.mongodb.client.MongoDatabase;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.SessionSynchronization;

/**
 * Increments outside of any ongoing transaction, so a value that has been handed out stays handed out even if the
 * transaction that asked for it rolls back. The sequence then has gaps, which is the usual trade for identifiers that
 * only have to be unique.
 * <p>
 * This is the default. Use {@link SessionBoundSequenceExecutor} when the numbers have to be contiguous.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
public class DefaultSequenceExecutor extends AbstractSequenceExecutor {

	@Override
	protected MongoDatabase getDatabase(@Nullable String databaseName, MongoDatabaseFactory dbFactory) {
		return MongoDatabaseUtils.getDatabase(databaseName, dbFactory, SessionSynchronization.NEVER);
	}
}
