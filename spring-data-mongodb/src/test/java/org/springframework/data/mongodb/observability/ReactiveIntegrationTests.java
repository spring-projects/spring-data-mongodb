/*
 * Copyright 2013-2023 the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.ReactivePersonRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

/**
 * Collection of tests that log metrics and tracing with an external tracing tool.
 *
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = TestConfig.class)
public class ReactiveIntegrationTests extends SampleTestRunner {

	@Autowired ReactivePersonRepository repository;

	ReactiveIntegrationTests() {
		super(SampleRunnerConfig.builder().build());
	}

	@Override
	protected MeterRegistry createMeterRegistry() {
		return TestConfig.METER_REGISTRY;
	}

	@Override
	protected ObservationRegistry createObservationRegistry() {
		return TestConfig.OBSERVATION_REGISTRY;
	}

	@Override
	public SampleTestRunnerConsumer yourCode() {

		return (tracer, meterRegistry) -> {

			Observation intermediate = Observation.start("intermediate", createObservationRegistry());

			repository.deleteAll() //
					.then(repository.save(new Person("Dave", "Matthews", 42))) //
					.contextWrite(Context.of(ObservationThreadLocalAccessor.KEY, intermediate)) //
					.as(StepVerifier::create).expectNextCount(1)//
					.verifyComplete();

			repository.findByLastname("Matthews") //
					.contextWrite(Context.of(ObservationThreadLocalAccessor.KEY, intermediate)) //
					.as(StepVerifier::create).assertNext(actual -> {

						assertThat(actual).extracting("firstname", "lastname").containsExactly("Dave", "Matthews");
					}).verifyComplete();

			intermediate.stop();
			System.out.println(((SimpleMeterRegistry) meterRegistry).getMetersAsString());

			assertThat(tracer.getFinishedSpans()).hasSize(5).extracting(FinishedSpan::getName).contains("person.delete",
					"person.update", "person.find");
		};
	}
}
