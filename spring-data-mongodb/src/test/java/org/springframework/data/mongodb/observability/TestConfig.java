/*
 * Copyright 2022-2023 the original author or authors.
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

import java.util.Properties;

import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.PersonRepository;
import org.springframework.data.mongodb.repository.ReactivePersonRepository;
import org.springframework.data.mongodb.repository.SampleEvaluationContextExtension;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactoryBean;
import org.springframework.data.mongodb.repository.support.ReactiveMongoRepositoryFactoryBean;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.test.simple.SimpleTracer;

/**
 * @author Mark Paluch
 */
@Configuration
class TestConfig {

	static final MeterRegistry METER_REGISTRY = new SimpleMeterRegistry();
	static final ObservationRegistry OBSERVATION_REGISTRY = ObservationRegistry.create();

	static {
		OBSERVATION_REGISTRY.observationConfig().observationHandler(new DefaultMeterObservationHandler(METER_REGISTRY));
	}

	@Bean
	MongoDatabaseFactory mongoDatabaseFactory(MongoClientSettings settings) {
		return new SimpleMongoClientDatabaseFactory(MongoClients.create(settings), "observable");
	}

	@Bean
	ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory(MongoClientSettings settings) {
		return new SimpleReactiveMongoDatabaseFactory(com.mongodb.reactivestreams.client.MongoClients.create(settings),
				"observable");
	}

	@Bean
	MongoClientSettings mongoClientSettings(ObservationRegistry observationRegistry) {

		ConnectionString connectionString = new ConnectionString(
				String.format("mongodb://%s:%s/?w=majority&uuidrepresentation=javaLegacy", "127.0.0.1", 27017));

		MongoClientSettings settings = MongoClientSettings.builder() //
				.addCommandListener(new MongoObservationCommandListener(observationRegistry, connectionString)) //
				.contextProvider(ContextProviderFactory.create(observationRegistry)) //
				.applyConnectionString(connectionString) //
				.build();

		return settings;
	}

	@Bean
	MappingMongoConverter mongoConverter(MongoMappingContext mappingContext, MongoDatabaseFactory factory) {
		return new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext);
	}

	@Bean
	MongoMappingContext mappingContext() {
		return new MongoMappingContext();
	}

	@Bean
	MongoTemplate mongoTemplate(MongoDatabaseFactory mongoDatabaseFactory, MongoConverter mongoConverter) {

		MongoTemplate template = new MongoTemplate(mongoDatabaseFactory, mongoConverter);
		return template;
	}

	@Bean
	ReactiveMongoTemplate reactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory,
			MongoConverter mongoConverter) {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoDatabaseFactory, mongoConverter);
		return template;
	}

	@Bean
	public PropertiesFactoryBean namedQueriesProperties() {

		PropertiesFactoryBean bean = new PropertiesFactoryBean();
		bean.setLocation(new ClassPathResource("META-INF/mongo-named-queries.properties"));
		return bean;
	}

	@Bean
	MongoRepositoryFactoryBean<PersonRepository, Person, String> personRepositoryFactoryBean(MongoOperations operations,
			Properties namedQueriesProperties) {

		MongoRepositoryFactoryBean<PersonRepository, Person, String> factoryBean = new MongoRepositoryFactoryBean<>(
				PersonRepository.class);
		factoryBean.setNamedQueries(new PropertiesBasedNamedQueries(namedQueriesProperties));
		factoryBean.setMongoOperations(operations);
		factoryBean.setCreateIndexesForQueryMethods(true);
		return factoryBean;
	}

	@Bean
	ReactiveMongoRepositoryFactoryBean<ReactivePersonRepository, Person, String> reactivePersonRepositoryFactoryBean(
			ReactiveMongoOperations operations, Properties namedQueriesProperties) {

		ReactiveMongoRepositoryFactoryBean<ReactivePersonRepository, Person, String> factoryBean = new ReactiveMongoRepositoryFactoryBean<>(
				ReactivePersonRepository.class);
		factoryBean.setNamedQueries(new PropertiesBasedNamedQueries(namedQueriesProperties));
		factoryBean.setReactiveMongoOperations(operations);
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

	@Bean
	Tracer tracer() {
		return new SimpleTracer();
	}
}
