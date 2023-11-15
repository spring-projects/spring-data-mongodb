/*
 * Copyright 2018-2023 the original author or authors.
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

import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

/**
 * Options applicable to MongoDB <a href="https://docs.mongodb.com/manual/changeStreams/">Change Streams</a>. Intended
 * to be used along with {@link org.springframework.data.mongodb.core.messaging.ChangeStreamRequest} in a sync world as
 * well {@link ReactiveMongoOperations} if you prefer it that way.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Myroslav Kosinskyi
 * @since 2.1
 */
public class ChangeStreamOptions {

	private @Nullable Object filter;
	private @Nullable BsonValue resumeToken;
	private @Nullable FullDocument fullDocumentLookup;
	private @Nullable FullDocumentBeforeChange fullDocumentBeforeChangeLookup;
	private @Nullable Collation collation;
	private @Nullable Object resumeTimestamp;
	private Resume resume = Resume.UNDEFINED;

	protected ChangeStreamOptions() {}

	/**
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Object> getFilter() {
		return Optional.ofNullable(filter);
	}

	/**
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<BsonValue> getResumeToken() {
		return Optional.ofNullable(resumeToken);
	}

	/**
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<FullDocument> getFullDocumentLookup() {
		return Optional.ofNullable(fullDocumentLookup);
	}

	/**
	 * @return {@link Optional#empty()} if not set.
	 * @since 4.0
	 */
	public Optional<FullDocumentBeforeChange> getFullDocumentBeforeChangeLookup() {
		return Optional.ofNullable(fullDocumentBeforeChangeLookup);
	}

	/**
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Collation> getCollation() {
		return Optional.ofNullable(collation);
	}

	/**
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Instant> getResumeTimestamp() {
		return Optional.ofNullable(resumeTimestamp).map(timestamp -> asTimestampOfType(timestamp, Instant.class));
	}

	/**
	 * @return {@link Optional#empty()} if not set.
	 * @since 2.2
	 */
	public Optional<BsonTimestamp> getResumeBsonTimestamp() {
		return Optional.ofNullable(resumeTimestamp).map(timestamp -> asTimestampOfType(timestamp, BsonTimestamp.class));
	}

	/**
	 * @return {@literal true} if the change stream should be started after the {@link #getResumeToken() token}.
	 * @since 2.2
	 */
	public boolean isStartAfter() {
		return Resume.START_AFTER.equals(resume);
	}

	/**
	 * @return {@literal true} if the change stream should be resumed after the {@link #getResumeToken() token}.
	 * @since 2.2
	 */
	public boolean isResumeAfter() {
		return Resume.RESUME_AFTER.equals(resume);
	}

	/**
	 * @return empty {@link ChangeStreamOptions}.
	 */
	public static ChangeStreamOptions empty() {
		return ChangeStreamOptions.builder().build();
	}

	/**
	 * Obtain a shiny new {@link ChangeStreamOptionsBuilder} and start defining options in this fancy fluent way. Just
	 * don't forget to call {@link ChangeStreamOptionsBuilder#build() build()} when done.
	 *
	 * @return new instance of {@link ChangeStreamOptionsBuilder}.
	 */
	public static ChangeStreamOptionsBuilder builder() {
		return new ChangeStreamOptionsBuilder();
	}

	private static <T> T asTimestampOfType(Object timestamp, Class<T> targetType) {
		return targetType.cast(doGetTimestamp(timestamp, targetType));
	}

	private static <T> Object doGetTimestamp(Object timestamp, Class<T> targetType) {

		if (ClassUtils.isAssignableValue(targetType, timestamp)) {
			return timestamp;
		}

		if (timestamp instanceof Instant instant) {
			return new BsonTimestamp((int) instant.getEpochSecond(), 0);
		}

		if (timestamp instanceof BsonTimestamp bsonTimestamp) {
			return Instant.ofEpochSecond(bsonTimestamp.getTime());
		}

		throw new IllegalArgumentException(
				"o_O that should actually not happen; The timestamp should be an Instant or a BsonTimestamp but was "
						+ ObjectUtils.nullSafeClassName(timestamp));
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		ChangeStreamOptions that = (ChangeStreamOptions) o;

		if (!ObjectUtils.nullSafeEquals(this.filter, that.filter)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.resumeToken, that.resumeToken)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.fullDocumentLookup, that.fullDocumentLookup)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.fullDocumentBeforeChangeLookup, that.fullDocumentBeforeChangeLookup)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.collation, that.collation)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.resumeTimestamp, that.resumeTimestamp)) {
			return false;
		}
		return resume == that.resume;
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(filter);
		result = 31 * result + ObjectUtils.nullSafeHashCode(resumeToken);
		result = 31 * result + ObjectUtils.nullSafeHashCode(fullDocumentLookup);
		result = 31 * result + ObjectUtils.nullSafeHashCode(fullDocumentBeforeChangeLookup);
		result = 31 * result + ObjectUtils.nullSafeHashCode(collation);
		result = 31 * result + ObjectUtils.nullSafeHashCode(resumeTimestamp);
		result = 31 * result + ObjectUtils.nullSafeHashCode(resume);
		return result;
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.2
	 */
	enum Resume {

		UNDEFINED,

		/**
		 * @see com.mongodb.client.ChangeStreamIterable#startAfter(BsonDocument)
		 */
		START_AFTER,

		/**
		 * @see com.mongodb.client.ChangeStreamIterable#resumeAfter(BsonDocument)
		 */
		RESUME_AFTER
	}

	/**
	 * Builder for creating {@link ChangeStreamOptions}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class ChangeStreamOptionsBuilder {

		private @Nullable Object filter;
		private @Nullable BsonValue resumeToken;
		private @Nullable FullDocument fullDocumentLookup;
		private @Nullable FullDocumentBeforeChange fullDocumentBeforeChangeLookup;
		private @Nullable Collation collation;
		private @Nullable Object resumeTimestamp;
		private Resume resume = Resume.UNDEFINED;

		private ChangeStreamOptionsBuilder() {}

		/**
		 * Set the collation to use.
		 *
		 * @param collation must not be {@literal null} nor {@literal empty}.
		 * @return this.
		 */
		public ChangeStreamOptionsBuilder collation(Collation collation) {

			Assert.notNull(collation, "Collation must not be null nor empty");

			this.collation = collation;
			return this;
		}

		/**
		 * Set the filter to apply.
		 * <br />
		 * Fields on aggregation expression root level are prefixed to map to fields contained in
		 * {@link ChangeStreamDocument#getFullDocument() fullDocument}. However {@literal operationType}, {@literal ns},
		 * {@literal documentKey} and {@literal fullDocument} are reserved words that will be omitted, and therefore taken
		 * as given, during the mapping procedure. You may want to have a look at the
		 * <a href="https://docs.mongodb.com/manual/reference/change-events/">structure of Change Events</a>.
		 * <br />
		 * Use {@link org.springframework.data.mongodb.core.aggregation.TypedAggregation} to ensure filter expressions are
		 * mapped to domain type fields.
		 *
		 * @param filter the {@link Aggregation Aggregation pipeline} to apply for filtering events. Must not be
		 *          {@literal null}.
		 * @return this.
		 */
		public ChangeStreamOptionsBuilder filter(Aggregation filter) {

			Assert.notNull(filter, "Filter must not be null");

			this.filter = filter;
			return this;
		}

		/**
		 * Set the plain filter chain to apply.
		 *
		 * @param filter must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 */
		public ChangeStreamOptionsBuilder filter(Document... filter) {

			Assert.noNullElements(filter, "Filter must not contain null values");

			this.filter = Arrays.asList(filter);
			return this;
		}

		/**
		 * Set the resume token (typically a {@link org.bson.BsonDocument} containing a {@link org.bson.BsonBinary binary
		 * token}) after which to start with listening.
		 *
		 * @param resumeToken must not be {@literal null}.
		 * @return this.
		 */
		public ChangeStreamOptionsBuilder resumeToken(BsonValue resumeToken) {

			Assert.notNull(resumeToken, "ResumeToken must not be null");

			this.resumeToken = resumeToken;

			if (this.resume == Resume.UNDEFINED) {
				this.resume = Resume.RESUME_AFTER;
			}

			return this;
		}

		/**
		 * Set the {@link FullDocument} lookup to {@link FullDocument#UPDATE_LOOKUP}.
		 *
		 * @return this.
		 * @see #fullDocumentLookup(FullDocument)
		 */
		public ChangeStreamOptionsBuilder returnFullDocumentOnUpdate() {
			return fullDocumentLookup(FullDocument.UPDATE_LOOKUP);
		}

		/**
		 * Set the {@link FullDocument} lookup to use.
		 *
		 * @param lookup must not be {@literal null}.
		 * @return this.
		 */
		public ChangeStreamOptionsBuilder fullDocumentLookup(FullDocument lookup) {

			Assert.notNull(lookup, "Lookup must not be null");

			this.fullDocumentLookup = lookup;
			return this;
		}

		/**
		 * Set the {@link FullDocumentBeforeChange} lookup to use.
		 *
		 * @param lookup must not be {@literal null}.
		 * @return this.
		 * @since 4.0
		 */
		public ChangeStreamOptionsBuilder fullDocumentBeforeChangeLookup(FullDocumentBeforeChange lookup) {

			Assert.notNull(lookup, "Lookup must not be null");

			this.fullDocumentBeforeChangeLookup = lookup;
			return this;
		}

		/**
		 * Return the full document before being changed if it is available.
		 *
		 * @return this.
		 * @since 4.0
		 * @see #fullDocumentBeforeChangeLookup(FullDocumentBeforeChange) 
		 */
		public ChangeStreamOptionsBuilder returnFullDocumentBeforeChange() {
			return fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.WHEN_AVAILABLE);
		}

		/**
		 * Set the cluster time to resume from.
		 *
		 * @param resumeTimestamp must not be {@literal null}.
		 * @return this.
		 */
		public ChangeStreamOptionsBuilder resumeAt(Instant resumeTimestamp) {

			Assert.notNull(resumeTimestamp, "ResumeTimestamp must not be null");

			this.resumeTimestamp = resumeTimestamp;
			return this;
		}

		/**
		 * Set the cluster time to resume from.
		 *
		 * @param resumeTimestamp must not be {@literal null}.
		 * @return this.
		 * @since 2.2
		 */
		public ChangeStreamOptionsBuilder resumeAt(BsonTimestamp resumeTimestamp) {

			Assert.notNull(resumeTimestamp, "ResumeTimestamp must not be null");

			this.resumeTimestamp = resumeTimestamp;
			return this;
		}

		/**
		 * Set the resume token after which to continue emitting notifications.
		 *
		 * @param resumeToken must not be {@literal null}.
		 * @return this.
		 * @since 2.2
		 */
		public ChangeStreamOptionsBuilder resumeAfter(BsonValue resumeToken) {

			resumeToken(resumeToken);
			this.resume = Resume.RESUME_AFTER;

			return this;
		}

		/**
		 * Set the resume token after which to start emitting notifications.
		 *
		 * @param resumeToken must not be {@literal null}.
		 * @return this.
		 * @since 2.2
		 */
		public ChangeStreamOptionsBuilder startAfter(BsonValue resumeToken) {

			resumeToken(resumeToken);
			this.resume = Resume.START_AFTER;

			return this;
		}

		/**
		 * @return the built {@link ChangeStreamOptions}
		 */
		public ChangeStreamOptions build() {

			ChangeStreamOptions options = new ChangeStreamOptions();

			options.filter = this.filter;
			options.resumeToken = this.resumeToken;
			options.fullDocumentLookup = this.fullDocumentLookup;
			options.fullDocumentBeforeChangeLookup = this.fullDocumentBeforeChangeLookup;
			options.collation = this.collation;
			options.resumeTimestamp = this.resumeTimestamp;
			options.resume = this.resume;

			return options;
		}
	}
}
