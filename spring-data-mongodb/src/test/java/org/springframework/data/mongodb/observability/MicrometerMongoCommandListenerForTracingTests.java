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
import io.micrometer.api.instrument.Timer;
import io.micrometer.api.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerMongoCommandListenerForTracingTests {

	SimpleMeterRegistry registry = new SimpleMeterRegistry();

	MicrometerMongoCommandListener listener = new MicrometerMongoCommandListener(registry);

	SimpleTracer simpleTracer = new SimpleTracer();

	MongoTracingRecordingHandler handler = new MongoTracingRecordingHandler(simpleTracer);

	@BeforeEach
	void setup() {
		registry.config().timerRecordingHandler(handler);
	}

	@Test void successfullyCompletedCommandShouldCreateSpanWhenParentSampleInRequestContext() {
		TestRequestContext testRequestContext = testRequestContextWithParentSample();

		commandStartedAndSucceeded(testRequestContext);

		// TODO: Convert to SpanAssert
		SimpleSpan span = assertThatMongoSpanIsClientWithTags();
		assertThat(span.getIp()).isNull();
		assertThat(span.getPort()).isZero();
	}

	@Test void successfullyCompletedCommandShouldCreateSpanWithAddressInfoWhenParentSampleInRequestContextAndHandlerAddressInfoEnabled() {
		handler.setSetRemoteIpAndPortEnabled(true);
		TestRequestContext testRequestContext = testRequestContextWithParentSample();

		commandStartedAndSucceeded(testRequestContext);

		// TODO: Convert to SpanAssert
		SimpleSpan span = assertThatMongoSpanIsClientWithTags();
		assertThat(span.getIp()).isNotBlank();
		assertThat(span.getPort()).isPositive();
	}

	@Test void commandWithErrorShouldCreateTimerWhenParentSampleInRequestContext() {
		TestRequestContext testRequestContext = testRequestContextWithParentSample();

		listener.commandStarted(new CommandStartedEvent(testRequestContext, 0, new ConnectionDescription(new ServerId(new ClusterId("description"), new ServerAddress("localhost", 1234))), "database", "insert", new BsonDocument("collection", new BsonString("user"))));
		listener.commandFailed(new CommandFailedEvent(testRequestContext, 0, null, "insert", 0, new IllegalAccessException()));

		SimpleSpan simpleSpan = assertThatMongoSpanIsClientWithTags();
		// TODO: Convert to SpanAssert
		assertThat(simpleSpan.getThrowable()).isInstanceOf(IllegalAccessException.class);
	}

	@NotNull private TestRequestContext testRequestContextWithParentSample() {
		Timer.Sample parent = Timer.start(registry);
		TestRequestContext testRequestContext = TestRequestContext.withSample(parent);
		return testRequestContext;
	}

	private void commandStartedAndSucceeded(TestRequestContext testRequestContext) {
		listener.commandStarted(new CommandStartedEvent(
				testRequestContext, 0, new ConnectionDescription(new ServerId(new ClusterId("description"), new ServerAddress("localhost", 1234))), "database", "insert", new BsonDocument("collection", new BsonString("user"))));
		listener.commandSucceeded(new CommandSucceededEvent(testRequestContext, 0, null, "insert", null, 0));
	}

	// TODO: Convert to SpanAssert
	private SimpleSpan assertThatMongoSpanIsClientWithTags() {
		SimpleSpan simpleSpan = simpleTracer.onlySpan();
		assertThat(simpleSpan).isNotNull();
		assertThat(simpleSpan.getName()).isEqualTo("insert user");
		assertThat(simpleSpan.getSpanKind()).isEqualTo(Span.Kind.CLIENT);
		assertThat(simpleSpan.getRemoteServiceName()).isEqualTo("mongodb-database");
		assertThat(simpleSpan.getTags())
				.containsEntry("mongodb.command", "insert")
				.containsEntry("mongodb.collection", "user")
				.containsKey("mongodb.cluster_id");
		return simpleSpan;
	}

}
