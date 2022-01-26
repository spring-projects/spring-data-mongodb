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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.mongodb.RequestContext;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.micrometer.api.instrument.MeterRegistry;
import io.micrometer.api.instrument.Tag;
import io.micrometer.api.instrument.Tags;
import io.micrometer.api.instrument.Timer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import org.springframework.lang.Nullable;

/**
 * Altered the Brave MongoDb instrumentation code. The code is available here:
 * https://github.com/openzipkin/brave/blob/release-5.13.0/instrumentation/mongodb/src/main/java/brave/mongodb/TraceMongoCommandListener.java
 *
 * @author OpenZipkin Brave Authors
 * @author Marcin Grzejszczak
 * @since 4.0.0
 */
public final class MicrometerMongoCommandListener implements CommandListener {

	// See https://docs.mongodb.com/manual/reference/command for the command reference
	static final Set<String> COMMANDS_WITH_COLLECTION_NAME = new LinkedHashSet<>(
			Arrays.asList("aggregate", "count", "distinct", "mapReduce", "geoSearch", "delete", "find", "findAndModify",
					"insert", "update", "collMod", "compact", "convertToCapped", "create", "createIndexes", "drop", "dropIndexes",
					"killCursors", "listIndexes", "reIndex"));
	private static final Log log = LogFactory.getLog(MicrometerMongoCommandListener.class);
	private final MeterRegistry registry;

	public MicrometerMongoCommandListener(MeterRegistry registry) {
		this.registry = registry;
	}

	private static Timer.Sample sampleFromContext(RequestContext context) {
		Timer.Sample sample = context.getOrDefault(Timer.Sample.class, null);
		if (sample != null) {
			if (log.isDebugEnabled()) {
				log.debug("Found a sample in mongo context [" + sample + "]");
			}
			return sample;
		}
		if (log.isDebugEnabled()) {
			log.debug("No sample was found - will not create any child spans");
		}
		return null;
	}

	/**
	 * @return trimmed string from {@code bsonValue} or null if the trimmed string was
	 * empty or the value wasn't a string
	 */
	@Nullable static String getNonEmptyBsonString(BsonValue bsonValue) {
		if (bsonValue == null || !bsonValue.isString()) {
			return null;
		}
		String stringValue = bsonValue.asString().getValue().trim();
		return stringValue.isEmpty() ? null : stringValue;
	}

	static String getMetricName(String commandName, @Nullable String collectionName) {
		if (collectionName == null) {
			return commandName;
		}
		return commandName + " " + collectionName;
	}

	@Override public void commandStarted(CommandStartedEvent event) {
		if (log.isDebugEnabled()) {
			log.debug("Instrumenting the command started event");
		}
		String databaseName = event.getDatabaseName();
		if ("admin".equals(databaseName)) {
			return; // don't instrument commands like "endSessions"
		}
		RequestContext requestContext = event.getRequestContext();
		if (requestContext == null) {
			return;
		}
		Timer.Sample parent = sampleFromContext(requestContext);
		if (log.isDebugEnabled()) {
			log.debug("Found the following sample passed from the mongo context [" + parent + "]");
		}
		if (parent == null) {
			return;
		}
		setupObservability(event, requestContext);
	}

	private void setupObservability(CommandStartedEvent event, RequestContext requestContext) {
		String commandName = event.getCommandName();
		BsonDocument command = event.getCommand();
		String collectionName = getCollectionName(command, commandName);
		Timer.Builder timerBuilder = MongoSample.MONGODB_COMMAND.toBuilder();
		MongoHandlerContext mongoHandlerContext = new MongoHandlerContext(event) {
			@Override public String getContextualName() {
				return getMetricName(commandName, collectionName);
			}

			@Override public Tags getLowCardinalityTags() {
				Tags tags = Tags.empty();
				if (collectionName != null) {
					tags = tags.and(MongoSample.LowCardinalityCommandTags.MONGODB_COLLECTION.of(collectionName));
				}
				Tag tag = connectionTag(event);
				if (tag == null) {
					return tags;
				}
				return tags.and(tag);
			}

			@Override public Tags getHighCardinalityTags() {
				return Tags.of(MongoSample.HighCardinalityCommandTags.MONGODB_COMMAND.of(commandName));
			}
		};
		Timer.Sample child = Timer.start(this.registry, mongoHandlerContext);
		requestContext.put(Timer.Sample.class, child);
		requestContext.put(MongoHandlerContext.class, mongoHandlerContext);
		requestContext.put(Timer.Builder.class, timerBuilder);
		if (log.isDebugEnabled()) {
			log.debug("Created a child sample  [" + child + "] for mongo instrumentation and put it in mongo context");
		}
	}

	private Tag connectionTag(CommandStartedEvent event) {
		ConnectionDescription connectionDescription = event.getConnectionDescription();
		if (connectionDescription != null) {
			ConnectionId connectionId = connectionDescription.getConnectionId();
			if (connectionId != null) {
				return MongoSample.LowCardinalityCommandTags.MONGODB_CLUSTER_ID.of(connectionId.getServerId().getClusterId().getValue());
			}
		}
		return null;
	}

	@Override public void commandSucceeded(CommandSucceededEvent event) {
		RequestContext requestContext = event.getRequestContext();
		if (requestContext == null) {
			return;
		}
		Timer.Sample sample = requestContext.getOrDefault(Timer.Sample.class, null);
		if (sample == null) {
			return;
		}
		MongoHandlerContext context = requestContext.get(MongoHandlerContext.class);
		context.setCommandSucceededEvent(event);
		if (log.isDebugEnabled()) {
			log.debug("Command succeeded - will stop sample [" + sample + "]");
		}
		Timer.Builder builder = requestContext.get(Timer.Builder.class);
		sample.stop(builder);
		requestContext.delete(Timer.Sample.class);
		requestContext.delete(MongoHandlerContext.class);
	}

	@Override public void commandFailed(CommandFailedEvent event) {
		RequestContext requestContext = event.getRequestContext();
		if (requestContext == null) {
			return;
		}
		Timer.Sample sample = requestContext.getOrDefault(Timer.Sample.class, null);
		if (sample == null) {
			return;
		}
		MongoHandlerContext context = requestContext.get(MongoHandlerContext.class);
		context.setCommandFailedEvent(event);
		if (log.isDebugEnabled()) {
			log.debug("Command failed - will stop sample [" + sample + "]");
		}
		sample.error(event.getThrowable());
		Timer.Builder builder = requestContext.get(Timer.Builder.class);
		sample.stop(builder);
		requestContext.delete(Timer.Sample.class);
		requestContext.delete(MongoHandlerContext.class);
	}

	@Nullable private String getCollectionName(BsonDocument command, String commandName) {
		if (COMMANDS_WITH_COLLECTION_NAME.contains(commandName)) {
			String collectionName = getNonEmptyBsonString(command.get(commandName));
			if (collectionName != null) {
				return collectionName;
			}
		}
		// Some other commands, like getMore, have a field like {"collection":
		// collectionName}.
		return getNonEmptyBsonString(command.get("collection"));
	}

}
