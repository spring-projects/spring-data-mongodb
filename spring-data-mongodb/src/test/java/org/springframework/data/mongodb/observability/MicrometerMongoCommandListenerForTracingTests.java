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
import io.micrometer.api.instrument.observation.Observation;
import io.micrometer.api.instrument.observation.ObservationRegistry;
import io.micrometer.api.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.test.simple.SimpleTracer;
import io.micrometer.tracing.test.simple.SpanAssert;
import io.micrometer.tracing.test.simple.TracerAssert;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MicrometerMongoCommandListenerForTracingTests {

	ObservationRegistry registry = new SimpleMeterRegistry();

	MicrometerMongoCommandListener listener = new MicrometerMongoCommandListener(registry);

	SimpleTracer simpleTracer = new SimpleTracer();

	MongoTracingRecordingHandler handler = new MongoTracingRecordingHandler(simpleTracer);

	@BeforeEach void setup() {
		registry.observationConfig().observationHandler(handler);
	}

	@Test void successfullyCompletedCommandShouldCreateSpanWhenParentSampleInRequestContext() {
		TestRequestContext testRequestContext = testRequestContextWithParentObservation();

		commandStartedAndSucceeded(testRequestContext);

		assertThatMongoSpanIsClientWithTags().hasIpThatIsBlank().hasPortThatIsNotSet();
	}

	@Test
	void successfullyCompletedCommandShouldCreateSpanWithAddressInfoWhenParentSampleInRequestContextAndHandlerAddressInfoEnabled() {
		handler.setSetRemoteIpAndPortEnabled(true);
		TestRequestContext testRequestContext = testRequestContextWithParentObservation();

		commandStartedAndSucceeded(testRequestContext);

		assertThatMongoSpanIsClientWithTags().hasIpThatIsNotBlank().hasPortThatIsSet();
	}

	@Test void commandWithErrorShouldCreateTimerWhenParentSampleInRequestContext() {
		TestRequestContext testRequestContext = testRequestContextWithParentObservation();

		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0,
				new ConnectionDescription(new ServerId(new ClusterId("description"), new ServerAddress("localhost", 1234))),
				"database", "insert", new BsonDocument("collection", new BsonString("user")))); listener.commandFailed(
				new CommandFailedEvent(testRequestContext, 0, null, "insert", 0, new IllegalAccessException()));

		assertThatMongoSpanIsClientWithTags().assertThatThrowable().isInstanceOf(IllegalAccessException.class);
	}

	@NotNull private TestRequestContext testRequestContextWithParentObservation() {
		Observation parent = Observation.start("name", registry); return TestRequestContext.withObservation(parent);
	}

	private void commandStartedAndSucceeded(TestRequestContext testRequestContext) {
		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0,
				new ConnectionDescription(new ServerId(new ClusterId("description"), new ServerAddress("localhost", 1234))),
				"database", "insert", new BsonDocument("collection", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(testRequestContext, 0, null, "insert", null, 0));
	}

	private SpanAssert assertThatMongoSpanIsClientWithTags() {
		return TracerAssert.assertThat(simpleTracer).onlySpan().hasNameEqualTo("insert user")
				.hasSpanWithKindEqualTo(Span.Kind.CLIENT).hasRemoteServiceNameEqualTo("mongodb-database")
				.hasTag("mongodb.command", "insert").hasTag("mongodb.collection", "user").hasTagWithKey("mongodb.cluster_id");
	}

}
