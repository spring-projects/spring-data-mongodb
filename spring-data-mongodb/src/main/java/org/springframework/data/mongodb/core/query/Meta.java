/*
 * Copyright 2014-2016 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

	private final Map<String, Object> values = new LinkedHashMap<String, Object>(2);
	private final Set<CursorOption> flags = new LinkedHashSet<CursorOption>();

	/**
	 * @return {@literal null} if not set.
	 */
	public Long getMaxTimeMsec() {
		return getValue(MetaKey.MAX_TIME_MS.key);
	}

	/**
	 * Set the maximum time limit in milliseconds for processing operations.
	 * 
	 * @param maxTimeMsec
	 */
	public void setMaxTimeMsec(long maxTimeMsec) {
		setMaxTime(maxTimeMsec, TimeUnit.MILLISECONDS);
	}

	/**
	 * Set the maximum time limit for processing operations.
	 * 
	 * @param timeout
	 * @param timeUnit
	 */
	public void setMaxTime(long timeout, TimeUnit timeUnit) {
		setValue(MetaKey.MAX_TIME_MS.key, (timeUnit != null ? timeUnit : TimeUnit.MILLISECONDS).toMillis(timeout));
	}

	/**
	 * @return {@literal null} if not set.
	 */
	public Long getMaxScan() {
		return getValue(MetaKey.MAX_SCAN.key);
	}

	/**
	 * Only scan the specified number of documents.
	 * 
	 * @param maxScan
	 */
	public void setMaxScan(long maxScan) {
		setValue(MetaKey.MAX_SCAN.key, maxScan);
	}

	/**
	 * Add a comment to the query.
	 * 
	 * @param comment
	 */
	public void setComment(String comment) {
		setValue(MetaKey.COMMENT.key, comment);
	}

	/**
	 * @return {@literal null} if not set.
	 */
	public String getComment() {
		return getValue(MetaKey.COMMENT.key);
	}

	/**
	 * Using snapshot prevents the cursor from returning a document more than once.
	 * 
	 * @param useSnapshot
	 */
	public void setSnapshot(boolean useSnapshot) {
		setValue(MetaKey.SNAPSHOT.key, useSnapshot);
	}

	/**
	 * @return {@literal null} if not set.
	 */
	public boolean getSnapshot() {
		return getValue(MetaKey.SNAPSHOT.key, false);
	}

	/**
	 * Add {@link CursorOption} influencing behavior of the {@link com.mongodb.DBCursor}.
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
		return !this.values.isEmpty() || !this.flags.isEmpty();
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
	private void setValue(String key, Object value) {

		Assert.hasText(key, "Meta key must not be 'null' or blank.");

		if (value == null || (value instanceof String && !StringUtils.hasText((String) value))) {
			this.values.remove(key);
		}
		this.values.put(key, value);
	}

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
