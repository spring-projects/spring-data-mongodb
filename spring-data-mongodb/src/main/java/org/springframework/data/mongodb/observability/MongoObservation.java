/*
 * Copyright 2013-2021 the original author or authors.
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

import io.micrometer.api.instrument.docs.DocumentedObservation;
import io.micrometer.api.instrument.docs.TagKey;

/**
 * @author Marcin Grzejszczak
 * @since 1.0.0
 */
enum MongoObservation implements DocumentedObservation {

	/**
	 * Timer created around a MongoDB command execution.
	 */
	MONGODB_COMMAND {
		@Override public String getName() {
			return "mongodb.command";
		}

		@Override public TagKey[] getLowCardinalityTagKeys() {
			return LowCardinalityCommandTags.values();
		}

		@Override public TagKey[] getHighCardinalityTagKeys() {
			return HighCardinalityCommandTags.values();
		}

		@Override public String getPrefix() {
			return "mongodb";
		}
	};

	/**
	 * Enums related to low cardinality tags for MongoDB commands.
	 */
	enum LowCardinalityCommandTags implements TagKey {
		/**
		 * MongoDB collection name.
		 */
		MONGODB_COLLECTION {
			@Override public String getKey() {
				return "mongodb.collection";
			}
		},

		/**
		 * MongoDB cluster identifier.
		 */
		MONGODB_CLUSTER_ID {
			@Override public String getKey() {
				return "mongodb.cluster_id";
			}
		}
	}

	/**
	 * Enums related to high cardinality tags for MongoDB commands.
	 */
	enum HighCardinalityCommandTags implements TagKey {
		/**
		 * MongoDB command value.
		 */
		MONGODB_COMMAND {
			@Override public String getKey() {
				return "mongodb.command";
			}
		}
	}
}
