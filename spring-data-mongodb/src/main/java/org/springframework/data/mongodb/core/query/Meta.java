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
package org.springframework.data.mongodb.core.query;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Meta-data for {@link Query} instances.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 * @since 1.6
 */
public class Meta {

	private enum MetaKey {
		MAX_TIME_MS("$maxTimeMS"), MAX_SCAN("$maxScan"), COMMENT("$comment"), SNAPSHOT("$snapshot");

		private final String key;

		MetaKey(String key) {
			this.key = key;
		}
	}

	private Map<String, Object> values = Collections.emptyMap();
	private Set<CursorOption> flags = Collections.emptySet();
	private Integer cursorBatchSize;
	private Boolean allowDiskUse;

	public Meta() {}

	/**
	 * Copy a {@link Meta} object.
	 *
	 * @since 2.2
	 * @param source
	 */
	Meta(Meta source) {

		this.values = new LinkedHashMap<>(source.values);
		this.flags = new LinkedHashSet<>(source.flags);
		this.cursorBatchSize = source.cursorBatchSize;
		this.allowDiskUse = source.allowDiskUse;
	}

	/**
	 * Return whether the maximum time limit for processing operations is set.
	 *
	 * @return {@code true} if set; {@code false} otherwise.
	 * @since 4.0.6
	 */
	public boolean hasMaxTime() {

		Long maxTimeMsec = getMaxTimeMsec();

		return maxTimeMsec != null && maxTimeMsec > 0;
	}

	/**
	 * @return {@literal null} if not set.
	 */
	@Nullable
	public Long getMaxTimeMsec() {
		return getValue(MetaKey.MAX_TIME_MS.key);
	}

	/**
	 * Returns the required maximum time limit in milliseconds or throws {@link IllegalStateException} if the maximum time
	 * limit is not set.
	 *
	 * @return the maximum time limit in milliseconds for processing operations.
	 * @throws IllegalStateException if the maximum time limit is not set
	 * @see #hasMaxTime()
	 * @since 4.0.6
	 */
	public Long getRequiredMaxTimeMsec() {

		Long maxTimeMsec = getMaxTimeMsec();

		if (maxTimeMsec == null) {
			throw new IllegalStateException("Maximum time limit in milliseconds not set");
		}

		return maxTimeMsec;
	}

	/**
	 * Set the maximum time limit in milliseconds for processing operations.
	 *
	 * @param maxTimeMsec
	 */
	public void setMaxTimeMsec(long maxTimeMsec) {
		setMaxTime(Duration.ofMillis(maxTimeMsec));
	}

	/**
	 * Set the maximum time limit for processing operations.
	 *
	 * @param timeout must not be {@literal null}.
	 * @since 2.1
	 */
	public void setMaxTime(Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		setValue(MetaKey.MAX_TIME_MS.key, timeout.toMillis());
	}

	/**
	 * Return whether the comment is set.
	 *
	 * @return {@code true} if set; {@code false} otherwise.
	 * @since 4.0.6
	 */
	public boolean hasComment() {
		return StringUtils.hasText(getComment());
	}

	/**
	 * @return {@literal null} if not set.
	 */
	@Nullable
	public String getComment() {
		return getValue(MetaKey.COMMENT.key);
	}

	/**
	 * Returns the required comment or throws {@link IllegalStateException} if the comment is not set.
	 *
	 * @return the comment.
	 * @throws IllegalStateException if the comment is not set
	 * @see #hasComment()
	 * @since 4.0.6
	 */
	public String getRequiredComment() {

		String comment = getComment();

		if (comment == null) {
			throw new IllegalStateException("Comment not set");
		}

		return comment;
	}

	/**
	 * Add a comment to the query that is propagated to the profile log.
	 *
	 * @param comment
	 */
	public void setComment(String comment) {
		setValue(MetaKey.COMMENT.key, comment);
	}

	/**
	 * @return {@literal null} if not set.
	 * @since 2.1
	 */
	@Nullable
	public Integer getCursorBatchSize() {
		return cursorBatchSize;
	}

	/**
	 * Apply the batch size (number of documents to return in each response) for a query. <br />
	 * Use {@literal 0 (zero)} for no limit. A <strong>negative limit</strong> closes the cursor after returning a single
	 * batch indicating to the server that the client will not ask for a subsequent one.
	 *
	 * @param cursorBatchSize The number of documents to return per batch.
	 * @since 2.1
	 */
	public void setCursorBatchSize(int cursorBatchSize) {
		this.cursorBatchSize = cursorBatchSize;
	}

	/**
	 * Add {@link CursorOption} influencing behavior of the {@link com.mongodb.client.FindIterable}.
	 *
	 * @param option must not be {@literal null}.
	 * @return
	 * @since 1.10
	 */
	public boolean addFlag(CursorOption option) {

		Assert.notNull(option, "CursorOption must not be null");

		if (this.flags == Collections.EMPTY_SET) {
			this.flags = new LinkedHashSet<>(2);
		}

		return this.flags.add(option);
	}

	/**
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public Set<CursorOption> getFlags() {
		return flags;
	}

	/**
	 * When set to {@literal true}, aggregation stages can write data to disk.
	 *
	 * @return {@literal null} if not set.
	 * @since 3.0
	 */
	@Nullable
	public Boolean getAllowDiskUse() {
		return allowDiskUse;
	}

	/**
	 * Enables writing to temporary files for aggregation stages and queries. When set to {@literal true}, aggregation
	 * stages can write data to the {@code _tmp} subdirectory in the {@code dbPath} directory.
	 * <p>
	 * Starting in MongoDB 4.2, the profiler log messages and diagnostic log messages includes a {@code usedDisk}
	 * indicator if any aggregation stage wrote data to temporary files due to memory restrictions.
	 *
	 * @param allowDiskUse use {@literal null} for server defaults.
	 * @since 3.0
	 */
	public void setAllowDiskUse(@Nullable Boolean allowDiskUse) {
		this.allowDiskUse = allowDiskUse;
	}

	/**
	 * @return
	 */
	public boolean hasValues() {
		return !this.values.isEmpty() || !this.flags.isEmpty() || this.cursorBatchSize != null || this.allowDiskUse != null;
	}

	/**
	 * Get {@link Iterable} of set meta values.
	 *
	 * @return
	 */
	public Iterable<Entry<String, Object>> values() {
		return Collections.unmodifiableSet(this.values.entrySet());
	}

	/**
	 * Sets or removes the value in case of {@literal null} or empty {@link String}.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @param value
	 */
	void setValue(String key, @Nullable Object value) {

		Assert.hasText(key, "Meta key must not be 'null' or blank");

		if (values == Collections.EMPTY_MAP) {
			values = new LinkedHashMap<>(2);
		}

		if (value == null || (value instanceof String stringValue && !StringUtils.hasText(stringValue))) {
			this.values.remove(key);
		}
		this.values.put(key, value);
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private <T> T getValue(String key) {
		return (T) this.values.get(key);
	}

	private <T> T getValue(String key, T defaultValue) {

		T value = getValue(key);
		return value != null ? value : defaultValue;
	}

	@Override
	public int hashCode() {

		int hash = ObjectUtils.nullSafeHashCode(this.values);
		hash += ObjectUtils.nullSafeHashCode(this.flags);
		return hash;
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Meta other)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(this.values, other.values)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(this.flags, other.flags);
	}

	/**
	 * {@link CursorOption} represents {@code OP_QUERY} wire protocol flags to change the behavior of queries.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	public enum CursorOption {

		/** Prevents the server from timing out idle cursors. */
		NO_TIMEOUT,

		/**
		 * Sets the cursor to return all data returned by the query at once rather than splitting the results into batches.
		 */
		EXHAUST,

		/**
		 * Allows querying of a replica.
		 *
		 * @since 3.0.2
		 */
		SECONDARY_READS,

		/**
		 * Sets the cursor to return partial data from a query against a sharded cluster in which some shards do not respond
		 * rather than throwing an error.
		 */
		PARTIAL
	}
}
