/*
 * Copyright 2022 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ConnectionString;
import com.mongodb.RequestContext;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * Implement MongoDB's {@link CommandListener} using Micrometer's {@link Observation} API.
 *
 * @author OpenZipkin Brave Authors
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 * @since 4.0
 */
public class MongoObservationCommandListener implements CommandListener {

	private static final Log log = LogFactory.getLog(MongoObservationCommandListener.class);

	/**
	 * Aligns with ObservationThreadLocalAccessor.KEY.
	 */
	private static final String MICROMETER_OBSERVATION_KEY = "micrometer.observation";

	private final ObservationRegistry observationRegistry;
	private final @Nullable ConnectionString connectionString;

	private final MongoHandlerObservationConvention observationConvention = new DefaultMongoHandlerObservationConvention();

	/**
	 * Create a new {@link MongoObservationCommandListener} to record {@link Observation}s.
	 *
	 * @param observationRegistry must not be {@literal null}
	 */
	public MongoObservationCommandListener(ObservationRegistry observationRegistry) {

		Assert.notNull(observationRegistry, "ObservationRegistry must not be null");

		this.observationRegistry = observationRegistry;
		this.connectionString = null;
	}

	/**
	 * Create a new {@link MongoObservationCommandListener} to record {@link Observation}s. This constructor attaches the
	 * {@link ConnectionString} to every {@link Observation}.
	 *
	 * @param observationRegistry must not be {@literal null}
	 * @param connectionString must not be {@literal null}
	 */
	public MongoObservationCommandListener(ObservationRegistry observationRegistry, ConnectionString connectionString) {

		Assert.notNull(observationRegistry, "ObservationRegistry must not be null");
		Assert.notNull(connectionString, "ConnectionString must not be null");

		this.observationRegistry = observationRegistry;
		this.connectionString = connectionString;
	}

	@Override
	public void commandStarted(CommandStartedEvent event) {

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

		Observation parent = observationFromContext(requestContext);

		if (log.isDebugEnabled()) {
			log.debug("Found the following observation passed from the mongo context [" + parent + "]");
		}

		MongoHandlerContext observationContext = new MongoHandlerContext(connectionString, event, requestContext);
		observationContext.setRemoteServiceName("mongo");

		Observation observation = MongoObservation.MONGODB_COMMAND_OBSERVATION
				.observation(this.observationRegistry, () -> observationContext) //
				.observationConvention(this.observationConvention);

		if (parent != null) {
			observation.parentObservation(parent);
		}

		observation.start();

		requestContext.put(MICROMETER_OBSERVATION_KEY, observation);

		if (log.isDebugEnabled()) {
			log.debug(
					"Created a child observation  [" + observation + "] for Mongo instrumentation and put it in Mongo context");
		}
	}

	@Override
	public void commandSucceeded(CommandSucceededEvent event) {

		RequestContext requestContext = event.getRequestContext();

		if (requestContext == null) {
			return;
		}

		Observation observation = requestContext.getOrDefault(MICROMETER_OBSERVATION_KEY, null);
		if (observation == null) {
			return;
		}

		MongoHandlerContext context = (MongoHandlerContext) observation.getContext();
		context.setCommandSucceededEvent(event);

		if (log.isDebugEnabled()) {
			log.debug("Command succeeded - will stop observation [" + observation + "]");
		}

		observation.stop();
	}

	@Override
	public void commandFailed(CommandFailedEvent event) {

		RequestContext requestContext = event.getRequestContext();

		if (requestContext == null) {
			return;
		}

		Observation observation = requestContext.getOrDefault(MICROMETER_OBSERVATION_KEY, null);
		if (observation == null) {
			return;
		}

		MongoHandlerContext context = (MongoHandlerContext) observation.getContext();
		context.setCommandFailedEvent(event);

		if (log.isDebugEnabled()) {
			log.debug("Command failed - will stop observation [" + observation + "]");
		}

		observation.error(event.getThrowable());
		observation.stop();
	}

	/**
	 * Extract the {@link Observation} from MongoDB's {@link RequestContext}.
	 *
	 * @param context
	 * @return
	 */
	@Nullable
	private static Observation observationFromContext(RequestContext context) {

		Observation observation = context.getOrDefault(MICROMETER_OBSERVATION_KEY, null);

		if (observation != null) {

			if (log.isDebugEnabled()) {
				log.debug("Found a observation in Mongo context [" + observation + "]");
			}
			return observation;
		}

		if (log.isDebugEnabled()) {
			log.debug("No observation was found - will not create any child observations");
		}

		return null;
	}
}
