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

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.jetbrains.annotations.NotNull;
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.test.simple.SimpleTracer;
import io.micrometer.tracing.test.simple.SpanAssert;
import io.micrometer.tracing.test.simple.TracerAssert;

/**
 * Series of test cases exercising {@link MongoObservationCommandListener} to ensure proper creation of {@link Span}s.
 *
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 */
class MongoObservationCommandListenerForTracingTests {

	SimpleTracer simpleTracer;

	MeterRegistry meterRegistry;
	ObservationRegistry observationRegistry;

	MongoObservationCommandListener listener;

	@BeforeEach
	void setup() {

		this.simpleTracer = new SimpleTracer();

		this.meterRegistry = new SimpleMeterRegistry();
		this.observationRegistry = ObservationRegistry.create();
		this.observationRegistry.observationConfig().observationHandler(new DefaultMeterObservationHandler(meterRegistry));

		this.listener = new MongoObservationCommandListener(observationRegistry);
	}

	@Test
	void successfullyCompletedCommandShouldCreateSpanWhenParentSampleInRequestContext() {

		// given
		RequestContext traceRequestContext = createTestRequestContextWithParentObservationAndStartIt();

		// when
		commandStartedAndSucceeded(traceRequestContext);

		// then
		assertThatMongoSpanIsClientWithTags().hasIpThatIsBlank().hasPortThatIsNotSet();
	}


	@Test
	void commandWithErrorShouldCreateTimerWhenParentSampleInRequestContext() {

		// given
		RequestContext traceRequestContext = createTestRequestContextWithParentObservationAndStartIt();

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
		assertThatMongoSpanIsClientWithTags().assertThatThrowable().isInstanceOf(IllegalAccessException.class);
	}

	/**
	 * Create a parent {@link Observation} then wrap it inside a {@link MapRequestContext}.
	 */
	@NotNull
	private RequestContext createTestRequestContextWithParentObservationAndStartIt() {
		return ((SynchronousContextProvider) ContextProviderFactory.create(observationRegistry)).getContext();
	}

	/**
	 * Execute MongoDB's {@link com.mongodb.event.CommandListener#commandStarted(CommandStartedEvent)} and
	 * {@link com.mongodb.event.CommandListener#commandSucceeded(CommandSucceededEvent)} operations against the
	 * {@link MapRequestContext} in order to inject some test data.
	 *
	 * @param traceRequestContext
	 */
	private void commandStartedAndSucceeded(RequestContext traceRequestContext) {

		listener.commandStarted(new CommandStartedEvent(traceRequestContext, 0, //
				new ConnectionDescription( //
						new ServerId( //
								new ClusterId("description"), //
								new ServerAddress("localhost", 1234))), //
				"database", "insert", //
				new BsonDocument("collection", new BsonString("user"))));

		listener.commandSucceeded(new CommandSucceededEvent(traceRequestContext, 0, null, "insert", null, 0));
	}

	/**
	 * Create a base MongoDB-based {@link SpanAssert} using Micrometer Tracing's fluent API. Other test methods can apply
	 * additional assertions.
	 *
	 * @return
	 */
	private SpanAssert assertThatMongoSpanIsClientWithTags() {

		return TracerAssert.assertThat(simpleTracer).onlySpan() //
				.hasNameEqualTo("insert user") //
				.hasKindEqualTo(Span.Kind.CLIENT) //
				.hasRemoteServiceNameEqualTo("mongodb-database") //
				.hasTag(HighCardinalityCommandKeyNames.MONGODB_COMMAND.asString(), "insert") //
				.hasTag(LowCardinalityCommandKeyNames.MONGODB_COLLECTION.asString(), "user") //
				.hasTagWithKey(LowCardinalityCommandKeyNames.MONGODB_CLUSTER_ID.asString());
	}
}
