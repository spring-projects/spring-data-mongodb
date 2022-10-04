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

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * A MongoDB-based {@link io.micrometer.observation.Observation}.
 *
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 * @since 4.0.0
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
			return HighCardinalityCommandKeyNames.values();
		}

		@Override
		public String getPrefix() {
			return "spring.data.mongodb";
		}
	};

	/**
	 * Enums related to low cardinality key names for MongoDB commands.
	 */
	enum LowCardinalityCommandKeyNames implements KeyName {

		/**
		 * MongoDB collection name.
		 */
		MONGODB_COLLECTION {
			@Override
			public String asString() {
				return "spring.data.mongodb.collection";
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
		}
	}

	/**
	 * Enums related to high cardinality key names for MongoDB commands.
	 */
	enum HighCardinalityCommandKeyNames implements KeyName {

		/**
		 * MongoDB command value.
		 */
		MONGODB_COMMAND {
			@Override
			public String asString() {
				return "spring.data.mongodb.command";
			}
		}
	}
}
