/*
 * Copyright 2024-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.lang.Nullable;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;

/**
 * Unit tests for {@link MongoTransactionOptions}.
 *
 * @author Christoph Strobl
 */
class MongoTransactionOptionsUnitTests {

	private static final TransactionOptions NATIVE_OPTIONS = TransactionOptions.builder() //
			.maxCommitTime(1L, TimeUnit.SECONDS) //
			.readConcern(ReadConcern.SNAPSHOT) //
			.readPreference(ReadPreference.secondaryPreferred()) //
			.writeConcern(WriteConcern.W3) //
			.build();

	@Test // GH-1628
	void wrapsNativeDriverTransactionOptions() {

		assertThat(MongoTransactionOptions.of(NATIVE_OPTIONS))
				.returns(NATIVE_OPTIONS.getMaxCommitTime(TimeUnit.SECONDS), options -> options.getMaxCommitTime().toSeconds())
				.returns(NATIVE_OPTIONS.getReadConcern(), MongoTransactionOptions::getReadConcern)
				.returns(NATIVE_OPTIONS.getReadPreference(), MongoTransactionOptions::getReadPreference)
				.returns(NATIVE_OPTIONS.getWriteConcern(), MongoTransactionOptions::getWriteConcern)
				.returns(NATIVE_OPTIONS, MongoTransactionOptions::toDriverOptions);
	}

	@Test // GH-1628
	void mergeNoneWithDefaultsUsesDefaults() {

		assertThat(MongoTransactionOptions.NONE.mergeWith(MongoTransactionOptions.of(NATIVE_OPTIONS)))
				.returns(NATIVE_OPTIONS.getMaxCommitTime(TimeUnit.SECONDS), options -> options.getMaxCommitTime().toSeconds())
				.returns(NATIVE_OPTIONS.getReadConcern(), MongoTransactionOptions::getReadConcern)
				.returns(NATIVE_OPTIONS.getReadPreference(), MongoTransactionOptions::getReadPreference)
				.returns(NATIVE_OPTIONS.getWriteConcern(), MongoTransactionOptions::getWriteConcern)
				.returns(NATIVE_OPTIONS, MongoTransactionOptions::toDriverOptions);
	}

	@Test // GH-1628
	void mergeExistingOptionsWithNoneUsesOptions() {

		MongoTransactionOptions source = MongoTransactionOptions.of(NATIVE_OPTIONS);
		assertThat(source.mergeWith(MongoTransactionOptions.NONE)).isSameAs(source);
	}

	@Test // GH-1628
	void mergeExistingOptionsWithUsesFirstNonNullValue() {

		MongoTransactionOptions source = MongoTransactionOptions
				.of(TransactionOptions.builder().writeConcern(WriteConcern.UNACKNOWLEDGED).build());

		assertThat(source.mergeWith(MongoTransactionOptions.of(NATIVE_OPTIONS)))
				.returns(NATIVE_OPTIONS.getMaxCommitTime(TimeUnit.SECONDS), options -> options.getMaxCommitTime().toSeconds())
				.returns(NATIVE_OPTIONS.getReadConcern(), MongoTransactionOptions::getReadConcern)
				.returns(NATIVE_OPTIONS.getReadPreference(), MongoTransactionOptions::getReadPreference)
				.returns(source.getWriteConcern(), MongoTransactionOptions::getWriteConcern);
	}

	@Test // GH-1628
	void testEquals() {

		assertThat(MongoTransactionOptions.NONE) //
				.isSameAs(MongoTransactionOptions.NONE) //
				.isNotEqualTo(new MongoTransactionOptions() {
					@Nullable
					@Override
					public Duration getMaxCommitTime() {
						return null;
					}

					@Nullable
					@Override
					public ReadConcern getReadConcern() {
						return null;
					}

					@Nullable
					@Override
					public ReadPreference getReadPreference() {
						return null;
					}

					@Nullable
					@Override
					public WriteConcern getWriteConcern() {
						return null;
					}
				});
	}
}
