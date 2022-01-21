/*
 * Copyright 2002-2021 the original author or authors.
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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.micrometer.api.instrument.Tag;
import io.micrometer.api.instrument.Tags;
import io.micrometer.api.instrument.Timer;
import io.micrometer.api.instrument.simple.SimpleMeterRegistry;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static io.micrometer.core.tck.MeterRegistryAssert.assertThat;

class MicrometerMongoCommandListenerTests {

	SimpleMeterRegistry registry = new SimpleMeterRegistry();

	MicrometerMongoCommandListener listener = new MicrometerMongoCommandListener(registry);

	@Test void commandStartedShouldNotInstrumentWhenAdminDatabase() {
		listener.commandStarted(new CommandStartedEvent(null, 0, null, "admin", "", null));

		assertThat(registry).hasNoMetrics();
	}

	@Test void commandStartedShouldNotInstrumentWhenNoRequestContext() {
		listener.commandStarted(new CommandStartedEvent(null, 0, null, "some name", "", null));

		assertThat(registry).hasNoMetrics();
	}

	@Test void commandStartedShouldNotInstrumentWhenNoParentSampleInRequestContext() {
		listener.commandStarted(new CommandStartedEvent(new TestRequestContext(), 0, null, "some name", "", null));

		assertThat(registry).hasNoMetrics();
	}

	@Test void successfullyCompletedCommandShouldCreateTimerWhenParentSampleInRequestContext() {
		Timer.Sample parent = Timer.start(registry);
		TestRequestContext testRequestContext = TestRequestContext.withSample(parent);

		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0,
				new ConnectionDescription(new ServerId(new ClusterId("description"), new ServerAddress("localhost", 1234))),
				"database", "insert", new BsonDocument("collection", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(testRequestContext, 0, null, "insert", null, 0));

		assertThatTimerRegisteredWithTags();
	}

	@Test
	void successfullyCompletedCommandWithCollectionHavingCommandNameShouldCreateTimerWhenParentSampleInRequestContext() {
		Timer.Sample parent = Timer.start(registry);
		TestRequestContext testRequestContext = TestRequestContext.withSample(parent);

		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0,
				new ConnectionDescription(new ServerId(new ClusterId("description"), new ServerAddress("localhost", 1234))),
				"database", "aggregate", new BsonDocument("aggregate", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(testRequestContext, 0, null, "aggregate", null, 0));

		assertThatTimerRegisteredWithTags();
	}

	@Test void successfullyCompletedCommandWithoutClusterInformationShouldCreateTimerWhenParentSampleInRequestContext() {
		Timer.Sample parent = Timer.start(registry);
		TestRequestContext testRequestContext = TestRequestContext.withSample(parent);

		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0, null, "database", "insert",
				new BsonDocument("collection", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(testRequestContext, 0, null, "insert", null, 0));

		assertThat(registry).hasTimerWithNameAndTags("mongodb.command", Tags.of(Tag.of("mongodb.collection", "user")));
	}

	@Test void commandWithErrorShouldCreateTimerWhenParentSampleInRequestContext() {
		Timer.Sample parent = Timer.start(registry);
		TestRequestContext testRequestContext = TestRequestContext.withSample(parent);

		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0,
				new ConnectionDescription(new ServerId(new ClusterId("description"), new ServerAddress("localhost", 1234))),
				"database", "insert", new BsonDocument("collection", new BsonString("user")))); listener.commandFailed(
				new CommandFailedEvent(testRequestContext, 0, null, "insert", 0, new IllegalAccessException()));

		assertThatTimerRegisteredWithTags();
	}

	private void assertThatTimerRegisteredWithTags() {
		assertThat(registry).hasTimerWithNameAndTags("mongodb.command", Tags.of(Tag.of("mongodb.collection", "user")))
				.hasTimerWithNameAndTagKeys("mongodb.command", "mongodb.cluster_id");
	}

}
