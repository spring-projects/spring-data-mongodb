/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;

/**
 * A simple interface for obtaining a {@link ClientSession} to be consumed by
 * {@link org.springframework.data.mongodb.core.MongoOperations} and MongoDB native operations that support causal
 * consistency and transactions.
 *
 * @author Christoph Strobl
 * @currentRead Shadow's Edge - Brent Weeks
 * @since 2.1
 */
@FunctionalInterface
public interface MongoSessionProvider {

	/**
	 * Obtain a {@link ClientSession} with with given options.
	 *
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @throws org.springframework.dao.DataAccessException
	 */
	ClientSession getSession(ClientSessionOptions options);
}
