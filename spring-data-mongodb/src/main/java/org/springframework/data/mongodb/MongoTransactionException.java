/*
 * Copyright 2018-2025 the original author or authors.
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
package org.springframework.data.mongodb;

import org.jspecify.annotations.Nullable;

/**
 * A specific {@link ClientSessionException} related to issues with a transaction such as aborted or non existing
 * transactions.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class MongoTransactionException extends ClientSessionException {

	/**
	 * Constructor for {@link MongoTransactionException}.
	 *
	 * @param msg the detail message. Must not be {@literal null}.
	 */
	public MongoTransactionException(String msg) {
		super(msg);
	}

	/**
	 * Constructor for {@link ClientSessionException}.
	 *
	 * @param msg the detail message. Can be {@literal null}.
	 * @param cause the root cause. Can be {@literal null}.
	 */
	public MongoTransactionException(@Nullable String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}
}
