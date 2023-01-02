/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.ReferenceCollection;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.MongoCollection;

/**
 * {@link ReferenceLoader} implementation using a {@link MongoDatabaseFactory} to obtain raw {@link Document documents}
 * for linked entities via a {@link ReferenceLoader.DocumentReferenceQuery}.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public class MongoDatabaseFactoryReferenceLoader implements ReferenceLoader {

	private static final Log LOGGER = LogFactory.getLog(MongoDatabaseFactoryReferenceLoader.class);

	private final MongoDatabaseFactory mongoDbFactory;

	/**
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public MongoDatabaseFactoryReferenceLoader(MongoDatabaseFactory mongoDbFactory) {

		Assert.notNull(mongoDbFactory, "MongoDbFactory translator must not be null");

		this.mongoDbFactory = mongoDbFactory;
	}

	@Override
	public Iterable<Document> fetchMany(DocumentReferenceQuery referenceQuery, ReferenceCollection context) {

		MongoCollection<Document> collection = getCollection(context);

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(String.format("Bulk fetching %s from %s.%s", referenceQuery,
					StringUtils.hasText(context.getDatabase()) ? context.getDatabase()
							: collection.getNamespace().getDatabaseName(),
					context.getCollection()));
		}

		return referenceQuery.apply(collection);
	}

	/**
	 * Obtain the {@link MongoCollection} for a given {@link ReferenceCollection} from the underlying
	 * {@link MongoDatabaseFactory}.
	 *
	 * @param context must not be {@literal null}.
	 * @return the {@link MongoCollection} targeted by the {@link ReferenceCollection}.
	 */
	protected MongoCollection<Document> getCollection(ReferenceCollection context) {

		return MongoDatabaseUtils.getDatabase(context.getDatabase(), mongoDbFactory).getCollection(context.getCollection(),
				Document.class);
	}
}
