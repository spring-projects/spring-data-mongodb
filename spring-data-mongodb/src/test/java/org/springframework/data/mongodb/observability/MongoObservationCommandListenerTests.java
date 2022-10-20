/*
 * Copyright 2002-2022 the original author or authors.
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

import static io.micrometer.core.tck.MeterRegistryAssert.*;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.observability.MongoObservation.HighCardinalityCommandKeyNames;
import org.springframework.data.mongodb.observability.MongoObservation.LowCardinalityCommandKeyNames;

import com.mongodb.RequestContext;
import com.mongodb.ServerAddress;
import com.mongodb.client.SynchronousContextProvider;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * Series of test cases exercising {@link MongoObservationCommandListener}.
 *
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 */
class MongoObservationCommandListenerTests {

	ObservationRegistry observationRegistry;
	MeterRegistry meterRegistry;

	MongoObservationCommandListener listener;

	@BeforeEach
	void setup() {

		this.meterRegistry = new SimpleMeterRegistry();
		this.observationRegistry = ObservationRegistry.create();
		this.observationRegistry.observationConfig().observationHandler(new DefaultMeterObservationHandler(meterRegistry));

		this.listener = new MongoObservationCommandListener(observationRegistry);
	}

	@Test
	void commandStartedShouldNotInstrumentWhenAdminDatabase() {

		// when
		listener.commandStarted(new CommandStartedEvent(null, 0, null, "admin", "", null));

		// then
		assertThat(meterRegistry).hasNoMetrics();
	}

	@Test
	void commandStartedShouldNotInstrumentWhenNoRequestContext() {

		// when
		listener.commandStarted(new CommandStartedEvent(null, 0, null, "some name", "", null));

		// then
		assertThat(meterRegistry).hasNoMetrics();
	}

	@Test
	void commandStartedShouldNotInstrumentWhenNoParentSampleInRequestContext() {

		// when
		listener.commandStarted(new CommandStartedEvent(new MapRequestContext(), 0, null, "some name", "", null));

		// then
		assertThat(meterRegistry).hasNoMetrics();
	}

	@Test
	void successfullyCompletedCommandShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		Observation parent = Observation.start("name", observationRegistry);
		RequestContext traceRequestContext = getContext();

		// when
		listener.commandStarted(new CommandStartedEvent(traceRequestContext, 0, //
				new ConnectionDescription( //
						new ServerId( //
								new ClusterId("description"), //
								new ServerAddress("localhost", 1234))),
				"database", "insert", //
				new BsonDocument("collection", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(traceRequestContext, 0, null, "insert", null, 0));

		// then
		assertThatTimerRegisteredWithTags();
	}

	@Test
	void successfullyCompletedCommandWithCollectionHavingCommandNameShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		Observation parent = Observation.start("name", observationRegistry);
		RequestContext traceRequestContext = getContext();

		// when
		listener.commandStarted(new CommandStartedEvent(traceRequestContext, 0, //
				new ConnectionDescription( //
						new ServerId( //
								new ClusterId("description"), //
								new ServerAddress("localhost", 1234))), //
				"database", "aggregate", //
				new BsonDocument("aggregate", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(traceRequestContext, 0, null, "aggregate", null, 0));

		// then
		assertThatTimerRegisteredWithTags();
	}


	@Test
	void successfullyCompletedCommandWithoutClusterInformationShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		Observation parent = Observation.start("name", observationRegistry);
		RequestContext traceRequestContext = getContext();

		// when
		listener.commandStarted(new CommandStartedEvent(traceRequestContext, 0, null, "database", "insert",
				new BsonDocument("collection", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(traceRequestContext, 0, null, "insert", null, 0));

		// then
		assertThat(meterRegistry).hasTimerWithNameAndTags(HighCardinalityCommandKeyNames.MONGODB_COMMAND.asString(),
				KeyValues.of(LowCardinalityCommandKeyNames.MONGODB_COLLECTION.withValue("user")));
	}

	@Test
	void commandWithErrorShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		Observation parent = Observation.start("name", observationRegistry);
		RequestContext traceRequestContext = getContext();

		// when
		listener.commandStarted(new CommandStartedEvent(traceRequestContext, 0, //
				new ConnectionDescription( //
						new ServerId( //
								new ClusterId("description"), //
								new ServerAddress("localhost", 1234))), //
				"database", "insert", //
				new BsonDocument("collection", new BsonString("user"))));
		listener.commandFailed( //
				new CommandFailedEvent(traceRequestContext, 0, null, "insert", 0, new IllegalAccessException()));

		// then
		assertThatTimerRegisteredWithTags();
	}

	private RequestContext getContext() {
		return ((SynchronousContextProvider) ContextProviderFactory.create(observationRegistry)).getContext();
	}

	private void assertThatTimerRegisteredWithTags() {

		assertThat(meterRegistry) //
				.hasTimerWithNameAndTags(HighCardinalityCommandKeyNames.MONGODB_COMMAND.asString(),
						KeyValues.of(LowCardinalityCommandKeyNames.MONGODB_COLLECTION.withValue("user"))) //
				.hasTimerWithNameAndTagKeys(HighCardinalityCommandKeyNames.MONGODB_COMMAND.asString(),
						LowCardinalityCommandKeyNames.MONGODB_CLUSTER_ID.asString());
	}

}
