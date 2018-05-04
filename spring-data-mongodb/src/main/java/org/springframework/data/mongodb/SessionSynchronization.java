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

/**
 * {@link SessionSynchronization} is used along with {@link org.springframework.data.mongodb.core.MongoTemplate} to
 * define in which type of transactions to participate if any.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public enum SessionSynchronization {

	/**
	 * Synchronize with any transaction even with empty transactions and initiate a MongoDB transaction when doing so by
	 * registering a MongoDB specific {@link org.springframework.transaction.support.ResourceHolderSynchronization}.
	 */
	ALWAYS,

	/**
	 * Synchronize with native MongoDB transactions initiated via {@link MongoTransactionManager}.
	 */
	ON_ACTUAL_TRANSACTION;
}
