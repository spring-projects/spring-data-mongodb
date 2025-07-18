/*
 * Copyright 2022-2025 the original author or authors.
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

import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.event.CommandStartedEvent;
import io.micrometer.common.KeyValues;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import static org.springframework.data.mongodb.observability.MongoObservation.LowCardinalityCommandKeyNames.*;

/**
 * Default {@link MongoHandlerObservationConvention} implementation.
 *
 * @author Greg Turnquist
 * @author Mark Paluch
 * @author Michal Domagala
 * @since 4.0
 */
class DefaultMongoHandlerObservationConvention implements MongoHandlerObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(MongoHandlerContext context) {

		if (context.getCommandStartedEvent() == null) {
			throw new IllegalStateException("not command started event present");
		}

		ConnectionString connectionString = context.getConnectionString();
		String connectionStringValue = connectionString != null ? connectionString.getConnectionString() : null;
		String username = connectionString != null ? connectionString.getUsername() : null;

		String transport = null,  peerName = null, peerPort =null,  clusterId = null;
		ConnectionDescription connectionDescription = context.getCommandStartedEvent().getConnectionDescription();
		if (connectionDescription != null) {
			ServerAddress serverAddress = connectionDescription.getServerAddress();

			if (serverAddress != null) {
				transport = "IP.TCP";
				peerName = serverAddress.getHost();
				peerPort = String.valueOf(serverAddress.getPort());
			}

			ConnectionId connectionId = connectionDescription.getConnectionId();
			if (connectionId != null) {
				clusterId = connectionId.getServerId().getClusterId().getValue();
			}
		}

		return KeyValues.of(
				DB_SYSTEM.withValue("mongodb"),
				MONGODB_COMMAND.withValue(context.getCommandName()),
				DB_CONNECTION_STRING.withOptionalValue(connectionStringValue),
				DB_USER.withOptionalValue(username),
				DB_NAME.withOptionalValue(context.getDatabaseName()),
				MONGODB_COLLECTION.withOptionalValue(context.getCollectionName()),
				NET_TRANSPORT.withOptionalValue(transport),
				NET_PEER_NAME.withOptionalValue(peerName),
				NET_PEER_PORT.withOptionalValue(peerPort),
				MONGODB_CLUSTER_ID.withOptionalValue(clusterId)
		);
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(MongoHandlerContext context) {
		return KeyValues.empty();
	}

	@Override
	public String getContextualName(MongoHandlerContext context) {

		String collectionName = context.getCollectionName();
		CommandStartedEvent commandStartedEvent = context.getCommandStartedEvent();

		Assert.notNull(commandStartedEvent, "CommandStartedEvent must not be null");

		if (ObjectUtils.isEmpty(collectionName)) {
			return commandStartedEvent.getCommandName();
		}

		return collectionName + "." + commandStartedEvent.getCommandName();
	}

}
