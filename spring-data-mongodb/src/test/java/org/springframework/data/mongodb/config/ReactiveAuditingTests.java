/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.mongodb.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.mongodb.core.AuditablePerson;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Integration test for the auditing support via {@link org.springframework.data.mongodb.core.ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class ReactiveAuditingTests {

	@Autowired ReactiveAuditablePersonRepository auditablePersonRepository;
	@Autowired AuditorAware<AuditablePerson> auditorAware;
	@Autowired MongoMappingContext context;
	@Autowired ReactiveMongoOperations operations;

	@Configuration
	@EnableMongoAuditing(auditorAwareRef = "auditorProvider")
	@EnableReactiveMongoRepositories(basePackageClasses = ReactiveAuditingTests.class, considerNestedRepositories = true)
	static class Config extends AbstractReactiveMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		public MongoClient reactiveMongoClient() {
			return MongoClients.create();
		}

		@Bean
		@SuppressWarnings("unchecked")
		public AuditorAware<AuditablePerson> auditorProvider() {
			return mock(AuditorAware.class);
		}
	}

	@Test // DATAMONGO-2139, DATAMONGO-2150
	public void auditingWorksForVersionedEntityWithWrapperVersion() {

		verifyAuditingViaVersionProperty(new VersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				auditablePersonRepository::save, //
				null, 0L, 1L);
	}

	@Test // DATAMONGO-2179
	public void auditingWorksForVersionedEntityBatchWithWrapperVersion() {

		verifyAuditingViaVersionProperty(new VersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				s -> auditablePersonRepository.saveAll(Collections.singletonList(s)).next(), //
				null, 0L, 1L);
	}

	@Test // DATAMONGO-2139, DATAMONGO-2150
	public void auditingWorksForVersionedEntityWithSimpleVersion() {

		verifyAuditingViaVersionProperty(new SimpleVersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				auditablePersonRepository::save, //
				0L, 1L, 2L);
	}

	@Test // DATAMONGO-2139, DATAMONGO-2150
	public void auditingWorksForVersionedEntityWithWrapperVersionOnTemplate() {

		verifyAuditingViaVersionProperty(new VersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				operations::save, //
				null, 0L, 1L);
	}

	@Test // DATAMONGO-2139, DATAMONGO-2150
	public void auditingWorksForVersionedEntityWithSimpleVersionOnTemplate() {
		verifyAuditingViaVersionProperty(new SimpleVersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				operations::save, //
				0L, 1L, 2L);
	}

	private <T extends AuditablePerson> void verifyAuditingViaVersionProperty(T instance,
			Function<T, Object> versionExtractor, Function<T, Object> createdDateExtractor, Function<T, Mono<T>> persister,
			Object... expectedValues) {

		AtomicReference<T> instanceHolder = new AtomicReference<>(instance);
		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(instance.getClass());

		assertThat(versionExtractor.apply(instance)).isEqualTo(expectedValues[0]);
		assertThat(createdDateExtractor.apply(instance)).isNull();
		assertThat(entity.isNew(instance)).isTrue();

		persister.apply(instanceHolder.get()) //
				.as(StepVerifier::create).consumeNextWith(actual -> {

					instanceHolder.set(actual);

					assertThat(versionExtractor.apply(actual)).isEqualTo(expectedValues[1]);
					assertThat(createdDateExtractor.apply(instance)).isNotNull();
					assertThat(entity.isNew(actual)).isFalse();
				}).verifyComplete();

		persister.apply(instanceHolder.get()) //
				.as(StepVerifier::create).consumeNextWith(actual -> {

					instanceHolder.set(actual);

					assertThat(versionExtractor.apply(actual)).isEqualTo(expectedValues[2]);
					assertThat(entity.isNew(actual)).isFalse();
				}).verifyComplete();
	}

	interface ReactiveAuditablePersonRepository extends ReactiveMongoRepository<AuditablePerson, String> {}

	static class VersionedAuditablePerson extends AuditablePerson {
		@Version Long version;
	}

	static class SimpleVersionedAuditablePerson extends AuditablePerson {
		@Version long version;
	}
}
