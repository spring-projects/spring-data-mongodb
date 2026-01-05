/*
 * Copyright 2022-present the original author or authors.
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

import static org.springframework.data.mongodb.observability.MongoKeyName.MongoKeyValue;
import static org.springframework.data.mongodb.observability.MongoKeyName.just;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.docs.ObservationDocumentation;

import org.jspecify.annotations.Nullable;
import org.springframework.util.StringUtils;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.event.CommandEvent;

/**
 * A MongoDB-based {@link io.micrometer.observation.Observation}.
 *
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 * @since 4.0
 */
enum MongoObservation implements ObservationDocumentation {

	/**
	 * Timer created around a MongoDB command execution.
	 */
	MONGODB_COMMAND_OBSERVATION {

		@Override
		public String getName() {
			return "spring.data.mongodb.command";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return LowCardinality.getKeyNames();
		}

		@Override
		public KeyName[] getHighCardinalityKeyNames() {
			return new KeyName[0];
		}

	};

	/**
	 * Contributors for low cardinality key names.
	 */
	static class LowCardinality {

		static MongoKeyValue DB_SYSTEM = just("db.system", "mongodb");
		static MongoKeyName<MongoHandlerContext> MONGODB_COMMAND = MongoKeyName.requiredString("db.operation",
				MongoHandlerContext::getCommandName);

		static MongoKeyName<MongoHandlerContext> DB_NAME = MongoKeyName.requiredString("db.name",
				MongoHandlerContext::getDatabaseName);

		static MongoKeyName<MongoHandlerContext> MONGODB_COLLECTION = MongoKeyName.requiredString("db.mongodb.collection",
				MongoHandlerContext::getCollectionName);

		/**
		 * MongoDB cluster identifier.
		 */
		static MongoKeyName<ConnectionDescription> MONGODB_CLUSTER_ID = MongoKeyName.required(
				"spring.data.mongodb.cluster_id", it -> it.getConnectionId().getServerId().getClusterId().getValue(),
				StringUtils::hasText);

		static MongoKeyValue NET_TRANSPORT_TCP_IP = just("net.transport", "IP.TCP");
		static MongoKeyName<ServerAddress> NET_PEER_NAME = MongoKeyName.required("net.peer.name", ServerAddress::getHost);
		static MongoKeyName<ServerAddress> NET_PEER_PORT = MongoKeyName.required("net.peer.port", ServerAddress::getPort);

		/**
		 * Observe low cardinality key values for the given {@link MongoHandlerContext}.
		 *
		 * @param context the context to contribute from, can be {@literal null} if no context is available.
		 * @return the key value contributor providing low cardinality key names.
		 */
		static Observer observe(@Nullable MongoHandlerContext context) {

			return Observer.fromContext(context, it -> {

				it.contribute(DB_SYSTEM).contribute(MONGODB_COMMAND, DB_NAME, MONGODB_COLLECTION);

				it.nested(MongoHandlerContext::getCommandStartedEvent) //
						.nested(CommandEvent::getConnectionDescription).contribute(MONGODB_CLUSTER_ID) //
						.nested(ConnectionDescription::getServerAddress) //
						.contribute(NET_TRANSPORT_TCP_IP).contribute(NET_PEER_NAME, NET_PEER_PORT);
			});
		}

		/**
		 * Returns the key names for low cardinality keys.
		 *
		 * @return the key names for low cardinality keys.
		 */
		static KeyName[] getKeyNames() {
			return observe(null).toKeyNames();
		}
	}
}
