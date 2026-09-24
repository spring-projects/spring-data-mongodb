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
 * Increments inside the ongoing transaction, if there is one, so the sequence rolls back with it and never skips a
 * number. The catch is that a rolled back value is handed out again, and that concurrent readers of the sequence
 * contend on the same document until the transaction ends.
 * <p>
 * Without an active transaction this behaves like {@link DefaultSequenceExecutor}.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
public class SessionBoundSequenceExecutor extends AbstractSequenceExecutor {

	@Override
	protected MongoDatabase getDatabase(@Nullable String databaseName, MongoDatabaseFactory dbFactory) {
		return MongoDatabaseUtils.getDatabase(databaseName, dbFactory, SessionSynchronization.ON_ACTUAL_TRANSACTION);
	}
}
