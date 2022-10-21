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

import java.net.InetSocketAddress;

import org.springframework.data.mongodb.observability.MongoObservation.HighCardinalityCommandKeyNames;
import org.springframework.data.mongodb.observability.MongoObservation.LowCardinalityCommandKeyNames;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.event.CommandStartedEvent;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;

/**
 * Default {@link MongoHandlerObservationConvention} implementation.
 *
 * @author Greg Turnquist
 * @since 4
 */
class DefaultMongoHandlerObservationConvention implements MongoHandlerObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(MongoHandlerContext context) {

		KeyValues keyValues = KeyValues.of(LowCardinalityCommandKeyNames.DB_SYSTEM.withValue("mongodb"));

		ConnectionString connectionString = context.getConnectionString();
		if (connectionString != null) {

			keyValues = keyValues
					.and(LowCardinalityCommandKeyNames.DB_CONNECTION_STRING.withValue(connectionString.getConnectionString()));

			String user = connectionString.getUsername();

			if (!ObjectUtils.isEmpty(user)) {
				keyValues = keyValues.and(LowCardinalityCommandKeyNames.DB_USER.withValue(user));
			}

		}

		if (!ObjectUtils.isEmpty(context.getDatabaseName())) {
			keyValues = keyValues.and(LowCardinalityCommandKeyNames.DB_NAME.withValue(context.getDatabaseName()));
		}

		if (!ObjectUtils.isEmpty(context.getCollectionName())) {
			keyValues = keyValues
					.and(LowCardinalityCommandKeyNames.MONGODB_COLLECTION.withValue(context.getCollectionName()));
		}

		ServerAddress serverAddress = context.getCommandStartedEvent().getConnectionDescription().getServerAddress();

		if (serverAddress != null) {

			keyValues = keyValues.and(LowCardinalityCommandKeyNames.NET_TRANSPORT.withValue("IP.TCP"),
					LowCardinalityCommandKeyNames.NET_PEER_ADDR.withValue(serverAddress.getHost()));

			InetSocketAddress socketAddress = serverAddress.getSocketAddress();

			if (socketAddress != null) {

				keyValues = keyValues.and(LowCardinalityCommandKeyNames.NET_PEER_NAME.withValue(socketAddress.getHostName()),
						LowCardinalityCommandKeyNames.NET_PEER_PORT.withValue("" + socketAddress.getPort()));
			}

		}

		KeyValue connectionTag = connectionTag(context.getCommandStartedEvent());
		if (connectionTag != null) {
			keyValues = keyValues.and(connectionTag);
		}

		return keyValues;
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(MongoHandlerContext context) {

		return KeyValues.of(HighCardinalityCommandKeyNames.MONGODB_COMMAND.withValue(context.getCommandName()));
	}

	@Override
	public String getContextualName(MongoHandlerContext context) {
		return context.getContextualName();
	}

	/**
	 * Extract connection details for a MongoDB connection into a {@link KeyValue}.
	 *
	 * @param event
	 * @return
	 */
	@Nullable
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
