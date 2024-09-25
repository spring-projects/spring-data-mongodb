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

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.Function;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

/**
 * Trivial implementation of {@link MongoTransactionOptions}.
 *
 * @author Christoph Strobl
 * @since 4.3
 */
class SimpleMongoTransactionOptions implements MongoTransactionOptions {

	static final Set<String> KNOWN_KEYS = Arrays.stream(OptionKey.values()).map(OptionKey::getKey)
			.collect(Collectors.toSet());

	private final Duration maxCommitTime;
	private final ReadConcern readConcern;
	private final ReadPreference readPreference;
	private final WriteConcern writeConcern;

	static SimpleMongoTransactionOptions of(Map<String, String> options) {
		return new SimpleMongoTransactionOptions(options);
	}

	private SimpleMongoTransactionOptions(Map<String, String> options) {

		this.maxCommitTime = doGetMaxCommitTime(options);
		this.readConcern = doGetReadConcern(options);
		this.readPreference = doGetReadPreference(options);
		this.writeConcern = doGetWriteConcern(options);
	}

	@Nullable
	@Override
	public Duration getMaxCommitTime() {
		return maxCommitTime;
	}

	@Nullable
	@Override
	public ReadConcern getReadConcern() {
		return readConcern;
	}

	@Nullable
	@Override
	public ReadPreference getReadPreference() {
		return readPreference;
	}

	@Nullable
	@Override
	public WriteConcern getWriteConcern() {
		return writeConcern;
	}

	@Override
	public String toString() {

		return "DefaultMongoTransactionOptions{" + "maxCommitTime=" + maxCommitTime + ", readConcern=" + readConcern
				+ ", readPreference=" + readPreference + ", writeConcern=" + writeConcern + '}';
	}

	@Nullable
	private static Duration doGetMaxCommitTime(Map<String, String> options) {

		return getValue(options, OptionKey.MAX_COMMIT_TIME, value -> {

			Duration timeout = Duration.parse(value);
			Assert.isTrue(!timeout.isNegative(), "%s cannot be negative".formatted(OptionKey.MAX_COMMIT_TIME));
			return timeout;
		});
	}

	@Nullable
	private static ReadConcern doGetReadConcern(Map<String, String> options) {
		return getValue(options, OptionKey.READ_CONCERN, value -> new ReadConcern(ReadConcernLevel.fromString(value)));
	}

	@Nullable
	private static ReadPreference doGetReadPreference(Map<String, String> options) {
		return getValue(options, OptionKey.READ_PREFERENCE, ReadPreference::valueOf);
	}

	@Nullable
	private static WriteConcern doGetWriteConcern(Map<String, String> options) {

		return getValue(options, OptionKey.WRITE_CONCERN, value -> {

			WriteConcern writeConcern = WriteConcern.valueOf(value);
			if (writeConcern == null) {
				throw new IllegalArgumentException("'%s' is not a valid WriteConcern".formatted(options.get("writeConcern")));
			}
			return writeConcern;
		});
	}

	@Nullable
	private static <T> T getValue(Map<String, String> options, OptionKey key, Function<String, T> convertFunction) {

		String value = options.get(key.getKey());
		return value != null ? convertFunction.apply(value) : null;
	}

	enum OptionKey {

		MAX_COMMIT_TIME("maxCommitTime"), READ_CONCERN("readConcern"), READ_PREFERENCE("readPreference"), WRITE_CONCERN(
				"writeConcern");

		final String key;

		OptionKey(String key) {
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		@Override
		public String toString() {
			return getKey();
		}
	}

}
