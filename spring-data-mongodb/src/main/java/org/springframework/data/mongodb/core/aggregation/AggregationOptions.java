/*
 * Copyright 2014-2023 the original author or authors.
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

import java.time.Duration;
import java.util.Optional;

import org.bson.Document;
import org.springframework.data.mongodb.core.ReadConcernAware;
import org.springframework.data.mongodb.core.ReadPreferenceAware;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;

/**
 * Holds a set of configurable aggregation options that can be used within an aggregation pipeline. A list of support
 * aggregation options can be found in the
 * <a href="https://docs.mongodb.org/manual/reference/command/aggregate/#aggregate">MongoDB reference documentation</a>.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Yadhukrishna S Pai
 * @author Soumya Prakash Behera
 * @see Aggregation#withOptions(AggregationOptions)
 * @see TypedAggregation#withOptions(AggregationOptions)
 * @since 1.6
 */
public class AggregationOptions implements ReadConcernAware, ReadPreferenceAware {

	private static final String BATCH_SIZE = "batchSize";
	private static final String CURSOR = "cursor";
	private static final String EXPLAIN = "explain";
	private static final String ALLOW_DISK_USE = "allowDiskUse";
	private static final String COLLATION = "collation";
	private static final String COMMENT = "comment";
	private static final String MAX_TIME = "maxTimeMS";
	private static final String HINT = "hint";

	private final boolean allowDiskUse;
	private final boolean explain;
	private final Optional<Document> cursor;
	private final Optional<Collation> collation;
	private final Optional<String> comment;
	private final Optional<Object> hint;

	private Optional<ReadConcern> readConcern;

	private Optional<ReadPreference> readPreference;
	private Duration maxTime = Duration.ZERO;
	private ResultOptions resultOptions = ResultOptions.READ;
	private DomainTypeMapping domainTypeMapping = DomainTypeMapping.RELAXED;

	/**
	 * Creates a new {@link AggregationOptions}.
	 *
	 * @param allowDiskUse whether to off-load intensive sort-operations to disk.
	 * @param explain whether to get the execution plan for the aggregation instead of the actual results.
	 * @param cursor can be {@literal null}, used to pass additional options to the aggregation.
	 */
	public AggregationOptions(boolean allowDiskUse, boolean explain, @Nullable Document cursor) {
		this(allowDiskUse, explain, cursor, null);
	}

	/**
	 * Creates a new {@link AggregationOptions}.
	 *
	 * @param allowDiskUse whether to off-load intensive sort-operations to disk.
	 * @param explain whether to get the execution plan for the aggregation instead of the actual results.
	 * @param cursor can be {@literal null}, used to pass additional options (such as {@code batchSize}) to the
	 *          aggregation.
	 * @param collation collation for string comparison. Can be {@literal null}.
	 * @since 2.0
	 */
	public AggregationOptions(boolean allowDiskUse, boolean explain, @Nullable Document cursor,
			@Nullable Collation collation) {
		this(allowDiskUse, explain, cursor, collation, null, null);
	}

	/**
	 * Creates a new {@link AggregationOptions}.
	 *
	 * @param allowDiskUse whether to off-load intensive sort-operations to disk.
	 * @param explain whether to get the execution plan for the aggregation instead of the actual results.
	 * @param cursor can be {@literal null}, used to pass additional options (such as {@code batchSize}) to the
	 *          aggregation.
	 * @param collation collation for string comparison. Can be {@literal null}.
	 * @param comment execution comment. Can be {@literal null}.
	 * @since 2.2
	 */
	public AggregationOptions(boolean allowDiskUse, boolean explain, @Nullable Document cursor,
			@Nullable Collation collation, @Nullable String comment) {
		this(allowDiskUse, explain, cursor, collation, comment, null);
	}

	/**
	 * Creates a new {@link AggregationOptions}.
	 *
	 * @param allowDiskUse whether to off-load intensive sort-operations to disk.
	 * @param explain whether to get the execution plan for the aggregation instead of the actual results.
	 * @param cursor can be {@literal null}, used to pass additional options (such as {@code batchSize}) to the
	 *          aggregation.
	 * @param collation collation for string comparison. Can be {@literal null}.
	 * @param comment execution comment. Can be {@literal null}.
	 * @param hint can be {@literal null}, used to provide an index that would be forcibly used by query optimizer.
	 * @since 3.1
	 */
	private AggregationOptions(boolean allowDiskUse, boolean explain, @Nullable Document cursor,
			@Nullable Collation collation, @Nullable String comment, @Nullable Object hint) {

		this.allowDiskUse = allowDiskUse;
		this.explain = explain;
		this.cursor = Optional.ofNullable(cursor);
		this.collation = Optional.ofNullable(collation);
		this.comment = Optional.ofNullable(comment);
		this.hint = Optional.ofNullable(hint);
		this.readConcern = Optional.empty();
		this.readPreference = Optional.empty();
	}

	/**
	 * Creates a new {@link AggregationOptions}.
	 *
	 * @param allowDiskUse whether to off-load intensive sort-operations to disk.
	 * @param explain whether to get the execution plan for the aggregation instead of the actual results.
	 * @param cursorBatchSize initial cursor batch size.
	 * @since 2.0
	 */
	public AggregationOptions(boolean allowDiskUse, boolean explain, int cursorBatchSize) {
		this(allowDiskUse, explain, createCursor(cursorBatchSize), null);
	}

	/**
	 * Creates new {@link AggregationOptions} given {@link Document} containing aggregation options.
	 *
	 * @param document must not be {@literal null}.
	 * @return the {@link AggregationOptions}.
	 * @since 2.0
	 */
	public static AggregationOptions fromDocument(Document document) {

		Assert.notNull(document, "Document must not be null");

		boolean allowDiskUse = document.getBoolean(ALLOW_DISK_USE, false);
		boolean explain = document.getBoolean(EXPLAIN, false);
		Document cursor = document.get(CURSOR, Document.class);
		Collation collation = document.containsKey(COLLATION) ? Collation.from(document.get(COLLATION, Document.class))
				: null;
		String comment = document.getString(COMMENT);
		Document hint = document.get(HINT, Document.class);

		AggregationOptions options = new AggregationOptions(allowDiskUse, explain, cursor, collation, comment, hint);
		if (document.containsKey(MAX_TIME)) {
			options.maxTime = Duration.ofMillis(document.getLong(MAX_TIME));
		}
		return options;
	}

	/**
	 * Obtain a new {@link Builder} for constructing {@link AggregationOptions}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Enables writing to temporary files. When set to true, aggregation stages can write data to the _tmp subdirectory in
	 * the dbPath directory.
	 *
	 * @return {@literal true} if enabled.
	 */
	public boolean isAllowDiskUse() {
		return allowDiskUse;
	}

	/**
	 * Specifies to return the information on the processing of the pipeline.
	 *
	 * @return {@literal true} if enabled.
	 */
	public boolean isExplain() {
		return explain;
	}

	/**
	 * The initial cursor batch size, if available, otherwise {@literal null}.
	 *
	 * @return the batch size or {@literal null}.
	 * @since 2.0
	 */
	@Nullable
	public Integer getCursorBatchSize() {

		if (cursor.filter(val -> val.containsKey(BATCH_SIZE)).isPresent()) {
			return cursor.get().get(BATCH_SIZE, Integer.class);
		}

		return null;
	}

	/**
	 * Specify a document that contains options that control the creation of the cursor object.
	 *
	 * @return never {@literal null}.
	 */
	public Optional<Document> getCursor() {
		return cursor;
	}

	/**
	 * Get collation settings for string comparison.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	public Optional<Collation> getCollation() {
		return collation;
	}

	/**
	 * Get the comment for the aggregation.
	 *
	 * @return never {@literal null}.
	 * @since 2.2
	 */
	public Optional<String> getComment() {
		return comment;
	}

	/**
	 * Get the hint used to fulfill the aggregation.
	 *
	 * @return never {@literal null}.
	 * @since 3.1
	 * @deprecated since 4.1, use {@link #getHintObject()} instead.
	 */
	public Optional<Document> getHint() {
		return hint.map(it -> {
			if (it instanceof Document doc) {
				return doc;
			}
			if (it instanceof String hintString) {
				if (BsonUtils.isJsonDocument(hintString)) {
					return BsonUtils.parse(hintString, null);
				}
			}
			throw new IllegalStateException("Unable to read hint of type %s".formatted(it.getClass()));
		});
	}

	/**
	 * Get the hint used to fulfill the aggregation.
	 *
	 * @return never {@literal null}.
	 * @since 4.1
	 */
	public Optional<Object> getHintObject() {
		return hint;
	}

	@Override
	public boolean hasReadConcern() {
		return readConcern.isPresent();
	}

	@Override
	public ReadConcern getReadConcern() {
		return readConcern.orElse(null);
	}

	@Override
	public boolean hasReadPreference() {
		return readPreference.isPresent();
	}

	@Override
	public ReadPreference getReadPreference() {
		return readPreference.orElse(null);
	}

	/**
	 * @return the time limit for processing. {@link Duration#ZERO} is used for the default unbounded behavior.
	 * @since 3.0
	 */
	public Duration getMaxTime() {
		return maxTime;
	}

	/**
	 * @return {@literal true} to skip results when running an aggregation. Useful in combination with {@code $merge} or
	 *         {@code $out}.
	 * @since 3.0.2
	 */
	public boolean isSkipResults() {
		return ResultOptions.SKIP.equals(resultOptions);
	}

	/**
	 * @return the domain type mapping strategy do apply. Never {@literal null}.
	 * @since 3.2
	 */
	public DomainTypeMapping getDomainTypeMapping() {
		return domainTypeMapping;
	}

	/**
	 * Returns a new potentially adjusted copy for the given {@code aggregationCommandObject} with the configuration
	 * applied.
	 *
	 * @param command the aggregation command.
	 * @return
	 */
	Document applyAndReturnPotentiallyChangedCommand(Document command) {

		Document result = new Document(command);

		if (allowDiskUse && !result.containsKey(ALLOW_DISK_USE)) {
			result.put(ALLOW_DISK_USE, allowDiskUse);
		}

		if (explain && !result.containsKey(EXPLAIN)) {
			result.put(EXPLAIN, explain);
		}

		if (result.containsKey(HINT)) {
			hint.ifPresent(val -> result.append(HINT, val));
		}

		if (!result.containsKey(CURSOR)) {
			cursor.ifPresent(val -> result.put(CURSOR, val));
		}

		if (!result.containsKey(COLLATION)) {
			collation.map(Collation::toDocument).ifPresent(val -> result.append(COLLATION, val));
		}

		if (hasExecutionTimeLimit() && !result.containsKey(MAX_TIME)) {
			result.append(MAX_TIME, maxTime.toMillis());
		}

		return result;
	}

	/**
	 * Returns a {@link Document} representation of this {@link AggregationOptions}.
	 *
	 * @return never {@literal null}.
	 */
	public Document toDocument() {

		Document document = new Document();
		document.put(ALLOW_DISK_USE, allowDiskUse);
		document.put(EXPLAIN, explain);

		cursor.ifPresent(val -> document.put(CURSOR, val));
		collation.ifPresent(val -> document.append(COLLATION, val.toDocument()));
		comment.ifPresent(val -> document.append(COMMENT, val));
		hint.ifPresent(val -> document.append(HINT, val));

		if (hasExecutionTimeLimit()) {
			document.append(MAX_TIME, maxTime.toMillis());
		}

		return document;
	}

	/**
	 * @return {@literal true} if {@link #maxTime} is set to a positive value.
	 * @since 3.0
	 */
	public boolean hasExecutionTimeLimit() {
		return !maxTime.isZero() && !maxTime.isNegative();
	}

	@Override
	public String toString() {
		return toDocument().toJson();
	}

	static Document createCursor(int cursorBatchSize) {
		return new Document("batchSize", cursorBatchSize);
	}

	/**
	 * A Builder for {@link AggregationOptions}.
	 *
	 * @author Thomas Darimont
	 * @author Mark Paluch
	 */
	public static class Builder {

		private boolean allowDiskUse;
		private boolean explain;
		private @Nullable Document cursor;
		private @Nullable Collation collation;
		private @Nullable String comment;
		private @Nullable Object hint;
		private @Nullable ReadConcern readConcern;
		private @Nullable ReadPreference readPreference;
		private @Nullable Duration maxTime;
		private @Nullable ResultOptions resultOptions;
		private @Nullable DomainTypeMapping domainTypeMapping;

		/**
		 * Defines whether to off-load intensive sort-operations to disk.
		 *
		 * @param allowDiskUse use {@literal true} to allow disk use during the aggregation.
		 * @return this.
		 */
		public Builder allowDiskUse(boolean allowDiskUse) {

			this.allowDiskUse = allowDiskUse;
			return this;
		}

		/**
		 * Defines whether to get the execution plan for the aggregation instead of the actual results.
		 *
		 * @param explain use {@literal true} to enable explain feature.
		 * @return this.
		 */
		public Builder explain(boolean explain) {

			this.explain = explain;
			return this;
		}

		/**
		 * Additional options to the aggregation.
		 *
		 * @param cursor must not be {@literal null}.
		 * @return this.
		 */
		public Builder cursor(Document cursor) {

			this.cursor = cursor;
			return this;
		}

		/**
		 * Define the initial cursor batch size.
		 *
		 * @param batchSize use a positive int.
		 * @return this.
		 * @since 2.0
		 */
		public Builder cursorBatchSize(int batchSize) {

			this.cursor = createCursor(batchSize);
			return this;
		}

		/**
		 * Define collation settings for string comparison.
		 *
		 * @param collation can be {@literal null}.
		 * @return this.
		 * @since 2.0
		 */
		public Builder collation(@Nullable Collation collation) {

			this.collation = collation;
			return this;
		}

		/**
		 * Define a comment to describe the execution.
		 *
		 * @param comment can be {@literal null}.
		 * @return this.
		 * @since 2.2
		 */
		public Builder comment(@Nullable String comment) {

			this.comment = comment;
			return this;
		}

		/**
		 * Define a hint that is used by query optimizer to to fulfill the aggregation.
		 *
		 * @param hint can be {@literal null}.
		 * @return this.
		 * @since 3.1
		 */
		public Builder hint(@Nullable Document hint) {

			this.hint = hint;
			return this;
		}

		/**
		 * Define a hint that is used by query optimizer to to fulfill the aggregation.
		 *
		 * @param indexName can be {@literal null}.
		 * @return this.
		 * @since 4.1
		 */
		public Builder hint(@Nullable String indexName) {

			this.hint = indexName;
			return this;
		}

		/**
		 * Define a {@link ReadConcern} to apply to the aggregation.
		 *
		 * @param readConcern can be {@literal null}.
		 * @return this.
		 * @since 4.1
		 */
		public Builder readConcern(@Nullable ReadConcern readConcern) {

			this.readConcern = readConcern;
			return this;
		}

		/**
		 * Define a {@link ReadPreference} to apply to the aggregation.
		 *
		 * @param readPreference can be {@literal null}.
		 * @return this.
		 * @since 4.1
		 */
		public Builder readPreference(@Nullable ReadPreference readPreference) {

			this.readPreference = readPreference;
			return this;
		}

		/**
		 * Set the time limit for processing.
		 *
		 * @param maxTime {@link Duration#ZERO} is used for the default unbounded behavior. {@link Duration#isNegative()
		 *          Negative} values will be ignored.
		 * @return this.
		 * @since 3.0
		 */
		public Builder maxTime(@Nullable Duration maxTime) {

			this.maxTime = maxTime;
			return this;
		}

		/**
		 * Run the aggregation, but do NOT read the aggregation result from the store. <br />
		 * If the expected result of the aggregation is rather large, eg. when using an {@literal $out} operation, this
		 * option allows to execute the aggregation without having the cursor return the operation result.
		 *
		 * @return this.
		 * @since 3.0.2
		 */
		public Builder skipOutput() {

			this.resultOptions = ResultOptions.SKIP;
			return this;
		}

		/**
		 * Apply a strict domain type mapping considering {@link org.springframework.data.mongodb.core.mapping.Field}
		 * annotations throwing errors for non-existent, but referenced fields.
		 *
		 * @return this.
		 * @since 3.2
		 */
		public Builder strictMapping() {

			this.domainTypeMapping = DomainTypeMapping.STRICT;
			return this;
		}

		/**
		 * Apply a relaxed domain type mapping considering {@link org.springframework.data.mongodb.core.mapping.Field}
		 * annotations using the user provided name if a referenced field does not exist.
		 *
		 * @return this.
		 * @since 3.2
		 */
		public Builder relaxedMapping() {

			this.domainTypeMapping = DomainTypeMapping.RELAXED;
			return this;
		}

		/**
		 * Apply no domain type mapping at all taking the pipeline as-is.
		 *
		 * @return this.
		 * @since 3.2
		 */
		public Builder noMapping() {

			this.domainTypeMapping = DomainTypeMapping.NONE;
			return this;
		}

		/**
		 * Returns a new {@link AggregationOptions} instance with the given configuration.
		 *
		 * @return new instance of {@link AggregationOptions}.
		 */
		public AggregationOptions build() {

			AggregationOptions options = new AggregationOptions(allowDiskUse, explain, cursor, collation, comment, hint);
			if (maxTime != null) {
				options.maxTime = maxTime;
			}
			if (resultOptions != null) {
				options.resultOptions = resultOptions;
			}
			if (domainTypeMapping != null) {
				options.domainTypeMapping = domainTypeMapping;
			}
			if (readConcern != null) {
				options.readConcern = Optional.of(readConcern);
			}
			if (readPreference != null) {
				options.readPreference = Optional.of(readPreference);
			}

			return options;
		}
	}

	/**
	 * @since 3.0
	 */
	private enum ResultOptions {

		/**
		 * Just do it!, and do not read the operation result.
		 */
		SKIP,
		/**
		 * Read the aggregation result from the cursor.
		 */
		READ
	}

	/**
	 * Aggregation pipeline Domain type mappings supported by the mapping layer.
	 *
	 * @since 3.2
	 */
	public enum DomainTypeMapping {

		/**
		 * Mapping throws errors for non-existent, but referenced fields.
		 */
		STRICT,

		/**
		 * Fields that do not exist in the model are treated as-is.
		 */
		RELAXED,

		/**
		 * Do not attempt to map fields against the model and treat the entire pipeline as-is.
		 */
		NONE
	}
}
