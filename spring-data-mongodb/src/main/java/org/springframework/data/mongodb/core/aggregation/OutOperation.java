/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import org.springframework.lang.Contract;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Encapsulates the {@code $out}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#out(String)} instead of creating instances of this
 * class directly.
 *
 * @author Nikolay Bogdanov
 * @author Christoph Strobl
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/out/">MongoDB Aggregation Framework:
 *      $out</a>
 */
public class OutOperation implements AggregationOperation {

	private final @Nullable String databaseName;
	private final String collectionName;

	/**
	 * @param outCollectionName Collection name to export the results. Must not be {@literal null}.
	 */
	public OutOperation(String outCollectionName) {
		this(null, outCollectionName);
	}

	/**
	 * @param databaseName Optional database name the target collection is located in. Can be {@literal null}.
	 * @param collectionName Collection name to export the results. Must not be {@literal null}. Can be {@literal null}.
	 * @since 2.2
	 */
	private OutOperation(@Nullable String databaseName, String collectionName) {

		Assert.notNull(collectionName, "Collection name must not be null");

		this.databaseName = databaseName;
		this.collectionName = collectionName;
	}

	/**
	 * Optionally specify the database of the target collection. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @param database can be {@literal null}. Defaulted to aggregation target database.
	 * @return new instance of {@link OutOperation}.
	 * @since 2.2
	 */
	@Contract("_ -> new")
	public OutOperation in(@Nullable String database) {
		return new OutOperation(database, collectionName);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		if (!StringUtils.hasText(databaseName)) {
			return new Document(getOperator(), collectionName);
		}

		return new Document(getOperator(), new Document("db", databaseName).append("coll", collectionName));
	}

	@Override
	public String getOperator() {
		return "$out";
	}
}
