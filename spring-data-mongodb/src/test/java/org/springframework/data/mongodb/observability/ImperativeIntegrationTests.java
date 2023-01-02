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

import java.util.List;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.PersonRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;

/**
 * Collection of tests that log metrics and tracing with an external tracing tool.
 *
 * @author Greg Turnquist
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = TestConfig.class)
public class ImperativeIntegrationTests extends SampleTestRunner {

	@Autowired PersonRepository repository;

	ImperativeIntegrationTests() {
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

			repository.deleteAll();
			repository.save(new Person("Dave", "Matthews", 42));
			List<Person> people = repository.findByLastname("Matthews");

			assertThat(people).hasSize(1);
			assertThat(people.get(0)).extracting("firstname", "lastname").containsExactly("Dave", "Matthews");

			repository.deleteAll();

			System.out.println(((SimpleMeterRegistry) meterRegistry).getMetersAsString());

			assertThat(tracer.getFinishedSpans()).hasSize(5).extracting(FinishedSpan::getName).contains("person.delete",
					"person.update", "person.find");

			for (FinishedSpan span : tracer.getFinishedSpans()) {

				assertThat(span.getTags()).containsEntry("db.system", "mongodb").containsEntry("net.transport", "IP.TCP");

				assertThat(span.getTags()).containsKeys("db.connection_string", "db.name", "db.operation",
						"db.mongodb.collection", "net.peer.name", "net.peer.port", "net.sock.peer.addr", "net.sock.peer.port");
			}
		};
	}
}
