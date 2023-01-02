/*
 * Copyright 2018-2023 the original author or authors.
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

import org.springframework.core.ResolvableType;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mongodb.core.mapping.event.AuditingEntityCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAuditingEntityCallback;
import org.springframework.data.mongodb.test.util.ReactiveMongoClientClosingTestConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.ReactiveAuditorAware;
import org.springframework.data.mongodb.core.AuditablePerson;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration test for the auditing support via {@link org.springframework.data.mongodb.core.ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@ContextConfiguration
class ReactiveAuditingTests {

	static @Client MongoClient mongoClient;

	@Autowired ReactiveAuditablePersonRepository auditablePersonRepository;
	@Autowired MongoMappingContext context;
	@Autowired ReactiveMongoOperations operations;

	@Configuration
	@EnableReactiveMongoAuditing
	@EnableReactiveMongoRepositories(basePackageClasses = ReactiveAuditingTests.class, considerNestedRepositories = true,
			includeFilters = @Filter(type = FilterType.ASSIGNABLE_TYPE, classes = ReactiveAuditablePersonRepository.class))
	static class Config extends ReactiveMongoClientClosingTestConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		public MongoClient reactiveMongoClient() {
			return mongoClient;
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return new HashSet<>(
					Arrays.asList(AuditablePerson.class, VersionedAuditablePerson.class, SimpleVersionedAuditablePerson.class));
		}

		@Bean
		public ReactiveAuditorAware<AuditablePerson> auditorProvider() {

			AuditablePerson person = new AuditablePerson("some-person");
			person.setId("foo");

			return () -> Mono.just(person);
		}
	}

	@Test // DATAMONGO-2139, DATAMONGO-2150, DATAMONGO-2586
	void auditingWorksForVersionedEntityWithWrapperVersion() {

		verifyAuditingViaVersionProperty(new VersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				auditablePersonRepository::save, //
				null, 0L, 1L);
	}

	@Test // DATAMONGO-2179
	void auditingWorksForVersionedEntityBatchWithWrapperVersion() {

		verifyAuditingViaVersionProperty(new VersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				s -> auditablePersonRepository.saveAll(Collections.singletonList(s)).next(), //
				null, 0L, 1L);
	}

	@Test // DATAMONGO-2139, DATAMONGO-2150, DATAMONGO-2586
	void auditingWorksForVersionedEntityWithSimpleVersion() {

		verifyAuditingViaVersionProperty(new SimpleVersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				auditablePersonRepository::save, //
				0L, 1L, 2L);
	}

	@Test // DATAMONGO-2139, DATAMONGO-2150, DATAMONGO-2586
	void auditingWorksForVersionedEntityWithWrapperVersionOnTemplate() {

		verifyAuditingViaVersionProperty(new VersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				operations::save, //
				null, 0L, 1L);
	}

	@Test // DATAMONGO-2139, DATAMONGO-2150, DATAMONGO-2586
	void auditingWorksForVersionedEntityWithSimpleVersionOnTemplate() {
		verifyAuditingViaVersionProperty(new SimpleVersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				operations::save, //
				0L, 1L, 2L);
	}

	@Test // DATAMONGO-2586
	void auditingShouldOnlyRegisterReactiveAuditingCallback() {

		Object callbacks = ReflectionTestUtils.getField(operations, "entityCallbacks");
		Object callbackDiscoverer = ReflectionTestUtils.getField(callbacks, "callbackDiscoverer");
		List<EntityCallback<?>> actualCallbacks = ReflectionTestUtils.invokeMethod(callbackDiscoverer, "getEntityCallbacks",
				AuditablePerson.class, ResolvableType.forClass(EntityCallback.class));

		assertThat(actualCallbacks) //
				.hasAtLeastOneElementOfType(ReactiveAuditingEntityCallback.class) //
				.doesNotHaveAnyElementsOfTypes(AuditingEntityCallback.class);
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
