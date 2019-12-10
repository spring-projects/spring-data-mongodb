/*
 * Copyright 2014-2019 the original author or authors.
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
import java.util.concurrent.TimeUnit;

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

		private String key;

		MetaKey(String key) {
			this.key = key;
		}
	}

	private final Map<String, Object> values = new LinkedHashMap<>(2);
	private final Set<CursorOption> flags = new LinkedHashSet<>();
	private Integer cursorBatchSize;

	public Meta() {}

	/**
	 * Copy a {@link Meta} object.
	 *
	 * @since 2.2
	 * @param source
	 */
	Meta(Meta source) {
		this.values.putAll(source.values);
		this.flags.addAll(source.flags);
		this.cursorBatchSize = source.cursorBatchSize;
	}

	/**
	 * @return {@literal null} if not set.
	 */
	@Nullable
	public Long getMaxTimeMsec() {
		return getValue(MetaKey.MAX_TIME_MS.key);
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
	 * @param timeout
	 * @param timeUnit
	 * @deprecated since 2.1. Use {@link #setMaxTime(Duration)} instead.
	 */
	@Deprecated
	public void setMaxTime(long timeout, @Nullable TimeUnit timeUnit) {
		setValue(MetaKey.MAX_TIME_MS.key, (timeUnit != null ? timeUnit : TimeUnit.MILLISECONDS).toMillis(timeout));
	}

	/**
	 * Set the maximum time limit for processing operations.
	 *
	 * @param timeout must not be {@literal null}.
	 * @since 2.1
	 */
	public void setMaxTime(Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null!");
		setValue(MetaKey.MAX_TIME_MS.key, timeout.toMillis());
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
	 */
	@Nullable
	public String getComment() {
		return getValue(MetaKey.COMMENT.key);
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

		Assert.notNull(option, "CursorOption must not be null!");
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
	 * @return
	 */
	public boolean hasValues() {
		return !this.values.isEmpty() || !this.flags.isEmpty() || this.cursorBatchSize != null;
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

		Assert.hasText(key, "Meta key must not be 'null' or blank.");

		if (value == null || (value instanceof String && !StringUtils.hasText((String) value))) {
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int hash = ObjectUtils.nullSafeHashCode(this.values);
		hash += ObjectUtils.nullSafeHashCode(this.flags);
		return hash;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Meta)) {
			return false;
		}

		Meta other = (Meta) obj;
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

		/** Allows querying of a replica slave. */
		SLAVE_OK,

		/**
		 * Sets the cursor to return partial data from a query against a sharded cluster in which some shards do not respond
		 * rather than throwing an error.
		 */
		PARTIAL
	}
}
