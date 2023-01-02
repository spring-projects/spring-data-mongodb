/*
 * Copyright 2022-2023 the original author or authors.
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

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.docs.ObservationDocumentation;

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
			return LowCardinalityCommandKeyNames.values();
		}

		@Override
		public KeyName[] getHighCardinalityKeyNames() {
			return new KeyName[0];
		}

	};

	/**
	 * Enums related to low cardinality key names for MongoDB commands.
	 */
	enum LowCardinalityCommandKeyNames implements KeyName {

		/**
		 * MongoDB database system.
		 */
		DB_SYSTEM {
			@Override
			public String asString() {
				return "db.system";
			}
		},

		/**
		 * MongoDB connection string.
		 */
		DB_CONNECTION_STRING {
			@Override
			public String asString() {
				return "db.connection_string";
			}
		},

		/**
		 * Network transport.
		 */
		NET_TRANSPORT {
			@Override
			public String asString() {
				return "net.transport";
			}
		},

		/**
		 * Name of the database host.
		 */
		NET_PEER_NAME {
			@Override
			public String asString() {
				return "net.peer.name";
			}
		},

		/**
		 * Logical remote port number.
		 */
		NET_PEER_PORT {
			@Override
			public String asString() {
				return "net.peer.port";
			}
		},

		/**
		 * Mongo peer address.
		 */
		NET_SOCK_PEER_ADDR {
			@Override
			public String asString() {
				return "net.sock.peer.addr";
			}
		},

		/**
		 * Mongo peer port.
		 */
		NET_SOCK_PEER_PORT {
			@Override
			public String asString() {
				return "net.sock.peer.port";
			}
		},

		/**
		 * MongoDB user.
		 */
		DB_USER {
			@Override
			public String asString() {
				return "db.user";
			}
		},

		/**
		 * MongoDB database name.
		 */
		DB_NAME {
			@Override
			public String asString() {
				return "db.name";
			}
		},

		/**
		 * MongoDB collection name.
		 */
		MONGODB_COLLECTION {
			@Override
			public String asString() {
				return "db.mongodb.collection";
			}
		},

		/**
		 * MongoDB cluster identifier.
		 */
		MONGODB_CLUSTER_ID {
			@Override
			public String asString() {
				return "spring.data.mongodb.cluster_id";
			}
		},

		/**
		 * MongoDB command value.
		 */
		MONGODB_COMMAND {
			@Override
			public String asString() {
				return "db.operation";
			}
		}
	}

}
