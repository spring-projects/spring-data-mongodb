/*
 * Copyright 2023 the original author or authors.
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
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.interceptor.TransactionAttribute;

import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;

/**
 * Helper class for translating @Transactional labels into Mongo-specific {@link TransactionOptions}.
 *
 * @author Yan Kardziyaka
 */
public final class MongoTransactionUtils {
	private static final Log LOGGER = LogFactory.getLog(MongoTransactionUtils.class);

	private static final String MAX_COMMIT_TIME = "mongo:maxCommitTime";

	private static final String READ_CONCERN_OPTION = "mongo:readConcern";

	private static final String READ_PREFERENCE_OPTION = "mongo:readPreference";

	private static final String WRITE_CONCERN_OPTION = "mongo:writeConcern";

	private MongoTransactionUtils() {}

	@Nullable
	public static TransactionOptions extractOptions(TransactionDefinition transactionDefinition,
			@Nullable TransactionOptions fallbackOptions) {
		if (transactionDefinition instanceof TransactionAttribute transactionAttribute) {
			TransactionOptions.Builder builder = null;
			for (String label : transactionAttribute.getLabels()) {
				String[] tokens = label.split("=", 2);
				builder = tokens.length == 2 ? enhanceWithProperty(builder, tokens[0], tokens[1]) : builder;
			}
			if (builder == null) {
				return fallbackOptions;
			}
			TransactionOptions options = builder.build();
			return fallbackOptions == null ? options : TransactionOptions.merge(options, fallbackOptions);
		} else {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("%s cannot be casted to %s. Transaction labels won't be evaluated as options".formatted(
						TransactionDefinition.class.getName(), TransactionAttribute.class.getName()));
			}
			return fallbackOptions;
		}
	}

	@Nullable
	private static TransactionOptions.Builder enhanceWithProperty(@Nullable TransactionOptions.Builder builder,
			String key, String value) {
		return switch (key) {
			case MAX_COMMIT_TIME -> nullSafe(builder).maxCommitTime(Duration.parse(value).toMillis(), TimeUnit.MILLISECONDS);
			case READ_CONCERN_OPTION -> nullSafe(builder).readConcern(new ReadConcern(ReadConcernLevel.fromString(value)));
			case READ_PREFERENCE_OPTION -> nullSafe(builder).readPreference(ReadPreference.valueOf(value));
			case WRITE_CONCERN_OPTION -> nullSafe(builder).writeConcern(getWriteConcern(value));
			default -> builder;
		};
	}

	private static TransactionOptions.Builder nullSafe(@Nullable TransactionOptions.Builder builder) {
		return builder == null ? TransactionOptions.builder() : builder;
	}

	private static WriteConcern getWriteConcern(String writeConcernAsString) {
		WriteConcern writeConcern = WriteConcern.valueOf(writeConcernAsString);
		if (writeConcern == null) {
			throw new IllegalArgumentException("'%s' is not a valid WriteConcern".formatted(writeConcernAsString));
		}
		return writeConcern;
	}

}
