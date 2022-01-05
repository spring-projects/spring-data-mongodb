/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.IndexOperationsProvider;

/**
 * {@link IndexOperationsProvider} to obtain {@link IndexOperations} from a given {@link MongoDatabaseFactory}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class DefaultIndexOperationsProvider implements IndexOperationsProvider {

	private final MongoDatabaseFactory mongoDbFactory;
	private final QueryMapper mapper;

	/**
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param mapper must not be {@literal null}.
	 */
	DefaultIndexOperationsProvider(MongoDatabaseFactory mongoDbFactory, QueryMapper mapper) {

		this.mongoDbFactory = mongoDbFactory;
		this.mapper = mapper;
	}

	@Override
	public IndexOperations indexOps(String collectionName, Class<?> type) {
		return new DefaultIndexOperations(mongoDbFactory, collectionName, mapper, type);
	}
}
