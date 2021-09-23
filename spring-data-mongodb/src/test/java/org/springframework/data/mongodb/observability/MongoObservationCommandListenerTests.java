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

import io.micrometer.common.Tags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.TimerObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.observability.MongoObservation.HighCardinalityCommandTags;
import org.springframework.data.mongodb.observability.MongoObservation.LowCardinalityCommandTags;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

/**
 * Series of test cases exercising {@link MongoObservationCommandListener}.
 * 
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 * @since 4.0.0
 */
class MongoObservationCommandListenerTests {

	ObservationRegistry observationRegistry;
	MeterRegistry meterRegistry;

	MongoObservationCommandListener listener;

	@BeforeEach
	void setup() {

		this.meterRegistry = new SimpleMeterRegistry();
		this.observationRegistry = ObservationRegistry.create();
		this.observationRegistry.observationConfig().observationHandler(new TimerObservationHandler(meterRegistry));

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
		listener.commandStarted(new CommandStartedEvent(new TestRequestContext(), 0, null, "some name", "", null));

		// then
		assertThat(meterRegistry).hasNoMetrics();
	}

	@Test
	void successfullyCompletedCommandShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		Observation parent = Observation.start("name", observationRegistry);
		TestRequestContext testRequestContext = TestRequestContext.withObservation(parent);

		// when
		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0, //
				new ConnectionDescription( //
						new ServerId( //
								new ClusterId("description"), //
								new ServerAddress("localhost", 1234))),
				"database", "insert", //
				new BsonDocument("collection", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(testRequestContext, 0, null, "insert", null, 0));

		// then
		assertThatTimerRegisteredWithTags();
	}

	@Test
	void successfullyCompletedCommandWithCollectionHavingCommandNameShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		Observation parent = Observation.start("name", observationRegistry);
		TestRequestContext testRequestContext = TestRequestContext.withObservation(parent);

		// when
		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0, //
				new ConnectionDescription( //
						new ServerId( //
								new ClusterId("description"), //
								new ServerAddress("localhost", 1234))), //
				"database", "aggregate", //
				new BsonDocument("aggregate", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(testRequestContext, 0, null, "aggregate", null, 0));

		// then
		assertThatTimerRegisteredWithTags();
	}

	@Test
	void successfullyCompletedCommandWithoutClusterInformationShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		Observation parent = Observation.start("name", observationRegistry);
		TestRequestContext testRequestContext = TestRequestContext.withObservation(parent);

		// when
		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0, null, "database", "insert",
				new BsonDocument("collection", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(testRequestContext, 0, null, "insert", null, 0));

		// then
		assertThat(meterRegistry).hasTimerWithNameAndTags(HighCardinalityCommandTags.MONGODB_COMMAND.getKey(),
				Tags.of(LowCardinalityCommandTags.MONGODB_COLLECTION.of("user")));
	}

	@Test
	void commandWithErrorShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		Observation parent = Observation.start("name", observationRegistry);
		TestRequestContext testRequestContext = TestRequestContext.withObservation(parent);

		// when
		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0, //
				new ConnectionDescription( //
						new ServerId( //
								new ClusterId("description"), //
								new ServerAddress("localhost", 1234))), //
				"database", "insert", //
				new BsonDocument("collection", new BsonString("user"))));
		listener.commandFailed( //
				new CommandFailedEvent(testRequestContext, 0, null, "insert", 0, new IllegalAccessException()));

		// then
		assertThatTimerRegisteredWithTags();
	}

	private void assertThatTimerRegisteredWithTags() {

		assertThat(meterRegistry) //
				.hasTimerWithNameAndTags(HighCardinalityCommandTags.MONGODB_COMMAND.getKey(),
						Tags.of(LowCardinalityCommandTags.MONGODB_COLLECTION.getKey(), "user")) //
				.hasTimerWithNameAndTagKeys(HighCardinalityCommandTags.MONGODB_COMMAND.getKey(),
						LowCardinalityCommandTags.MONGODB_CLUSTER_ID.getKey());
	}

}
