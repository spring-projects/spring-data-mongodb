/*
 * Copyright 2013-2022 the original author or authors.
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.TimerObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.reporter.BuildingBlocks;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.PersonRepository;
import org.springframework.data.mongodb.repository.SampleEvaluationContextExtension;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactoryBean;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.RequestContext;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.client.SynchronousContextProvider;

/**
 * Collection of tests that log metrics and tracing with an external tracing tool. Since this external tool must be up
 * and running after the test is completed, this test is ONLY run manually. Needed:
 * {@code docker run -p 9411:9411 openzipkin/zipkin} and {@code docker run -p 27017:27017 mongo:latest} (either from
 * Docker Desktop or within separate shells).
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
@Disabled("Run this manually to visually test spans in Zipkin")
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class ZipkinIntegrationTests extends SampleTestRunner {

	private static final MeterRegistry METER_REGISTRY = new SimpleMeterRegistry();
	private static final ObservationRegistry OBSERVATION_REGISTRY = ObservationRegistry.create();

	static {
		OBSERVATION_REGISTRY.observationConfig().observationHandler(new TimerObservationHandler(METER_REGISTRY));
	}

	@Autowired PersonRepository repository;

	ZipkinIntegrationTests() {
		super(SampleRunnerConfig.builder().build(), OBSERVATION_REGISTRY, METER_REGISTRY);
	}

	@Override
	public BiConsumer<BuildingBlocks, Deque<ObservationHandler>> customizeObservationHandlers() {

		return (buildingBlocks, observationHandlers) -> {
			observationHandlers.addLast(new MongoTracingObservationHandler(buildingBlocks.getTracer()));
		};
	}

	@Override
	public TracingSetup[] getTracingSetup() {
		return new TracingSetup[] { TracingSetup.ZIPKIN_BRAVE };
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
		};
	}

	@Configuration
	@EnableMongoRepositories
	static class TestConfig {

		@Bean
		MongoObservationCommandListener mongoObservationCommandListener(ObservationRegistry registry) {
			return new MongoObservationCommandListener(registry);
		}

		@Bean
		MongoDatabaseFactory mongoDatabaseFactory(MongoObservationCommandListener commandListener,
				ObservationRegistry registry) {

			ConnectionString connectionString = new ConnectionString(
					String.format("mongodb://%s:%s/?w=majority&uuidrepresentation=javaLegacy", "127.0.0.1", 27017));

			RequestContext requestContext = TestRequestContext.withObservation(Observation.start("name", registry));
			SynchronousContextProvider contextProvider = () -> requestContext;

			MongoClientSettings settings = MongoClientSettings.builder() //
					.addCommandListener(commandListener) //
					.contextProvider(contextProvider) //
					.applyConnectionString(connectionString) //
					.build();

			return new SimpleMongoClientDatabaseFactory(MongoClients.create(settings), "observable");
		}

		@Bean
		MappingMongoConverter mongoConverter(MongoDatabaseFactory factory) {

			MongoMappingContext mappingContext = new MongoMappingContext();
			mappingContext.afterPropertiesSet();

			return new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext);
		}

		@Bean
		MongoTemplate mongoTemplate(MongoDatabaseFactory mongoDatabaseFactory, MongoConverter mongoConverter) {

			MongoTemplate template = new MongoTemplate(mongoDatabaseFactory, mongoConverter);
			template.setWriteConcern(WriteConcern.JOURNALED);
			return template;
		}

		@Bean
		public PropertiesFactoryBean namedQueriesProperties() {

			PropertiesFactoryBean bean = new PropertiesFactoryBean();
			bean.setLocation(new ClassPathResource("META-INF/mongo-named-queries.properties"));
			return bean;
		}

		@Bean
		MongoRepositoryFactoryBean<PersonRepository, Person, String> repositoryFactoryBean(MongoOperations operations,
				PropertiesFactoryBean namedQueriesProperties) throws IOException {

			MongoRepositoryFactoryBean<PersonRepository, Person, String> factoryBean = new MongoRepositoryFactoryBean<>(
					PersonRepository.class);
			factoryBean.setMongoOperations(operations);
			factoryBean.setNamedQueries(new PropertiesBasedNamedQueries(namedQueriesProperties.getObject()));
			factoryBean.setCreateIndexesForQueryMethods(true);
			return factoryBean;
		}

		@Bean
		SampleEvaluationContextExtension contextExtension() {
			return new SampleEvaluationContextExtension();
		}

		@Bean
		ObservationRegistry registry() {
			return OBSERVATION_REGISTRY;
		}
	}
}
