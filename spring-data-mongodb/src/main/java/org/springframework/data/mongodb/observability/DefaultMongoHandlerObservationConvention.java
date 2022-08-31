/*
 * Copyright 2013-2022 the original author or authors.
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
package org.springframework.data.mongodb.observability;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;

import org.springframework.data.mongodb.observability.MongoObservation.HighCardinalityCommandKeyNames;
import org.springframework.data.mongodb.observability.MongoObservation.LowCardinalityCommandKeyNames;

import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.event.CommandStartedEvent;

/**
 * Default {@link MongoHandlerObservationConvention} implementation.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class DefaultMongoHandlerObservationConvention implements MongoHandlerObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(MongoHandlerContext context) {

		KeyValues keyValues = KeyValues.empty();

		if (context.getCollectionName() != null) {
			keyValues = keyValues
					.and(LowCardinalityCommandKeyNames.MONGODB_COLLECTION.withValue(context.getCollectionName()));
		}

		KeyValue connectionTag = connectionTag(context.getCommandStartedEvent());
		if (connectionTag != null) {
			keyValues = keyValues.and(connectionTag);
		}

		return keyValues;
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(MongoHandlerContext context) {

		return KeyValues.of(
				HighCardinalityCommandKeyNames.MONGODB_COMMAND.withValue(context.getCommandStartedEvent().getCommandName()));
	}

	/**
	 * Extract connection details for a MongoDB connection into a {@link KeyValue}.
	 *
	 * @param event
	 * @return
	 */
	private static KeyValue connectionTag(CommandStartedEvent event) {

		ConnectionDescription connectionDescription = event.getConnectionDescription();

		if (connectionDescription != null) {

			ConnectionId connectionId = connectionDescription.getConnectionId();
			if (connectionId != null) {
				return LowCardinalityCommandKeyNames.MONGODB_CLUSTER_ID
						.withValue(connectionId.getServerId().getClusterId().getValue());
			}
		}

		return null;
	}
}
