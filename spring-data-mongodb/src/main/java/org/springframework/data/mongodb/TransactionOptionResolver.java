/*
 * Copyright 2024 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionDefinition;

/**
 * Interface that defines a resolver for {@link TransactionMetadata} based on a {@link TransactionDefinition}.
 * Transaction metadata is used to enrich the MongoDB transaction with additional information.
 *
 * @author Christoph Strobl
 * @since 4.3
 */
interface TransactionOptionResolver<T extends TransactionMetadata> {

	/**
	 * Resolves the transaction metadata from a given {@link TransactionDefinition}.
	 *
	 * @param definition the {@link TransactionDefinition}.
	 * @return the resolved {@link TransactionMetadata} or {@literal null} if the resolver cannot resolve any metadata.
	 */
	@Nullable
	T resolve(TransactionDefinition definition);
}
