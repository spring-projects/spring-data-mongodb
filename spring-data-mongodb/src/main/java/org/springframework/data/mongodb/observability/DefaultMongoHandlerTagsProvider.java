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

import io.micrometer.common.Tag;
import io.micrometer.common.Tags;

import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.event.CommandStartedEvent;

/**
 * Default {@link MongoHandlerTagsProvider} implementation.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class DefaultMongoHandlerTagsProvider implements MongoHandlerTagsProvider {

	@Override
	public Tags getLowCardinalityTags(MongoHandlerContext context) {

		Tags tags = Tags.empty();

		if (context.getCollectionName() != null) {
			tags = tags.and(MongoObservation.LowCardinalityCommandTags.MONGODB_COLLECTION.of(context.getCollectionName()));
		}

		Tag connectionTag = connectionTag(context.getCommandStartedEvent());
		if (connectionTag != null) {
			tags = tags.and(connectionTag);
		}

		return tags;
	}

	@Override
	public Tags getHighCardinalityTags(MongoHandlerContext context) {

		return Tags.of(MongoObservation.HighCardinalityCommandTags.MONGODB_COMMAND
				.of(context.getCommandStartedEvent().getCommandName()));
	}

	/**
	 * Extract connection details for a MongoDB connection into a {@link Tag}.
	 *
	 * @param event
	 * @return
	 */
	private static Tag connectionTag(CommandStartedEvent event) {

		ConnectionDescription connectionDescription = event.getConnectionDescription();

		if (connectionDescription != null) {

			ConnectionId connectionId = connectionDescription.getConnectionId();
			if (connectionId != null) {
				return MongoObservation.LowCardinalityCommandTags.MONGODB_CLUSTER_ID
						.of(connectionId.getServerId().getClusterId().getValue());
			}
		}

		return null;
	}
}
