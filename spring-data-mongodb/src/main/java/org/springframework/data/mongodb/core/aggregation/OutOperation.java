/*
 * Copyright 2016-2023 the original author or authors.
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
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
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
	private final @Nullable Document uniqueKey;
	private final @Nullable OutMode mode;

	/**
	 * @param outCollectionName Collection name to export the results. Must not be {@literal null}.
	 */
	public OutOperation(String outCollectionName) {
		this(null, outCollectionName, null, null);
	}

	/**
	 * @param databaseName Optional database name the target collection is located in. Can be {@literal null}.
	 * @param collectionName Collection name to export the results. Must not be {@literal null}. Can be {@literal null}.
	 * @param uniqueKey Optional unique key spec identify a document in the to collection for replacement or merge.
	 * @param mode The mode for merging the aggregation pipeline output with the target collection. Can be
	 *          {@literal null}. {@literal null}.
	 * @since 2.2
	 */
	private OutOperation(@Nullable String databaseName, String collectionName, @Nullable Document uniqueKey,
			@Nullable OutMode mode) {

		Assert.notNull(collectionName, "Collection name must not be null");

		this.databaseName = databaseName;
		this.collectionName = collectionName;
		this.uniqueKey = uniqueKey;
		this.mode = mode;
	}

	/**
	 * Optionally specify the database of the target collection. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @param database can be {@literal null}. Defaulted to aggregation target database.
	 * @return new instance of {@link OutOperation}.
	 * @since 2.2
	 */
	public OutOperation in(@Nullable String database) {
		return new OutOperation(database, collectionName, uniqueKey, mode);
	}

	/**
	 * Optionally specify the field that uniquely identifies a document in the target collection. <br />
	 * For convenience the given {@literal key} can either be a single field name or the Json representation of a key
	 * {@link Document}.
	 *
	 * <pre class="code">
	 *
	 * // {
	 * //    "field-1" : 1
	 * // }
	 * .uniqueKey("field-1")
	 *
	 * // {
	 * //    "field-1" : 1,
	 * //    "field-2" : 1
	 * // }
	 * .uniqueKey("{ 'field-1' : 1, 'field-2' : 1}")
	 * </pre>
	 *
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @param key can be {@literal null}. Server uses {@literal _id} when {@literal null}.
	 * @return new instance of {@link OutOperation}.
	 * @since 2.2
	 */
	public OutOperation uniqueKey(@Nullable String key) {

		Document uniqueKey = key == null ? null : BsonUtils.toDocumentOrElse(key, it -> new Document(it, 1));
		return new OutOperation(databaseName, collectionName, uniqueKey, mode);
	}

	/**
	 * Optionally specify the fields that uniquely identifies a document in the target collection. <br />
	 *
	 * <pre class="code">
	 *
	 * // {
	 * //    "field-1" : 1
	 * //    "field-2" : 1
	 * // }
	 * .uniqueKeyOf(Arrays.asList("field-1", "field-2"))
	 * </pre>
	 *
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link OutOperation}.
	 * @since 2.2
	 */
	public OutOperation uniqueKeyOf(Iterable<String> fields) {

		Assert.notNull(fields, "Fields must not be null");

		Document uniqueKey = new Document();
		fields.forEach(it -> uniqueKey.append(it, 1));

		return new OutOperation(databaseName, collectionName, uniqueKey, mode);
	}

	/**
	 * Specify how to merge the aggregation output with the target collection. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @param mode must not be {@literal null}.
	 * @return new instance of {@link OutOperation}.
	 * @since 2.2
	 */
	public OutOperation mode(OutMode mode) {

		Assert.notNull(mode, "Mode must not be null");
		return new OutOperation(databaseName, collectionName, uniqueKey, mode);
	}

	/**
	 * Replace the target collection. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @return new instance of {@link OutOperation}.
	 * @see OutMode#REPLACE_COLLECTION
	 * @since 2.2
	 */
	public OutOperation replaceCollection() {
		return mode(OutMode.REPLACE_COLLECTION);
	}

	/**
	 * Replace/Upsert documents in the target collection. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @return new instance of {@link OutOperation}.
	 * @see OutMode#REPLACE
	 * @since 2.2
	 */
	public OutOperation replaceDocuments() {
		return mode(OutMode.REPLACE);
	}

	/**
	 * Insert documents to the target collection. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.2 or later.
	 *
	 * @return new instance of {@link OutOperation}.
	 * @see OutMode#INSERT
	 * @since 2.2
	 */
	public OutOperation insertDocuments() {
		return mode(OutMode.INSERT);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		if (!requiresMongoDb42Format()) {
			return new Document("$out", collectionName);
		}

		Assert.state(mode != null, "Mode must not be null");

		Document $out = new Document("to", collectionName) //
				.append("mode", mode.getMongoMode());

		if (StringUtils.hasText(databaseName)) {
			$out.append("db", databaseName);
		}

		if (uniqueKey != null) {
			$out.append("uniqueKey", uniqueKey);
		}

		return new Document(getOperator(), $out);
	}

	@Override
	public String getOperator() {
		return "$out";
	}

	private boolean requiresMongoDb42Format() {
		return StringUtils.hasText(databaseName) || mode != null || uniqueKey != null;
	}

	/**
	 * The mode for merging the aggregation pipeline output.
	 *
	 * @author Christoph Strobl
	 * @since 2.2
	 */
	public enum OutMode {

		/**
		 * Write documents to the target collection. Errors if a document same uniqueKey already exists.
		 */
		INSERT("insertDocuments"),

		/**
		 * Update on any document in the target collection with the same uniqueKey.
		 */
		REPLACE("replaceDocuments"),

		/**
		 * Replaces the to collection with the output from the aggregation pipeline. Cannot be in a different database.
		 */
		REPLACE_COLLECTION("replaceCollection");

		private final String mode;

		OutMode(String mode) {
			this.mode = mode;
		}

		public String getMongoMode() {
			return mode;
		}
	}
}
