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
package org.springframework.data.mongodb.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.ResolvableType;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mongodb.core.AuditablePerson;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AuditingEntityCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAuditingEntityCallback;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoClientClosingTestConfiguration;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for auditing via Java config.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@ContextConfiguration
class AuditingViaJavaConfigRepositoriesTests {

	static @Client MongoClient mongoClient;

	@Autowired AuditablePersonRepository auditablePersonRepository;
	@Autowired AuditorAware<AuditablePerson> auditorAware;
	@Autowired MongoMappingContext context;
	@Autowired MongoOperations operations;

	AuditablePerson auditor;

	@Configuration
	@EnableMongoAuditing(auditorAwareRef = "auditorProvider")
	@EnableMongoRepositories(basePackageClasses = AuditablePersonRepository.class, considerNestedRepositories = true,
			includeFilters = @Filter(type = FilterType.ASSIGNABLE_TYPE, classes = AuditablePersonRepository.class))
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		protected String getDatabaseName() {

			return "database";
		}

		@Override
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Bean
		@SuppressWarnings("unchecked")
		public AuditorAware<AuditablePerson> auditorProvider() {
			return mock(AuditorAware.class);
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return new HashSet<>(
					Arrays.asList(AuditablePerson.class, VersionedAuditablePerson.class, SimpleVersionedAuditablePerson.class));
		}
	}

	@BeforeEach
	void setup() {
		auditablePersonRepository.deleteAll();
		this.auditor = auditablePersonRepository.save(new AuditablePerson("auditor"));
	}

	@Test // DATAMONGO-792, DATAMONGO-883
	void basicAuditing() {

		doReturn(Optional.of(this.auditor)).when(this.auditorAware).getCurrentAuditor();

		AuditablePerson savedUser = auditablePersonRepository.save(new AuditablePerson("user"));

		AuditablePerson createdBy = savedUser.getCreatedBy();

		assertThat(createdBy).isNotNull();
		assertThat(createdBy.getFirstname()).isEqualTo(this.auditor.getFirstname());
		assertThat(savedUser.getCreatedAt()).isNotNull();
	}

	@Test // DATAMONGO-843
	@SuppressWarnings("resource")
	void auditingUsesFallbackMappingContextIfNoneConfiguredWithRepositories() {
		new AnnotationConfigApplicationContext(SimpleConfigWithRepositories.class);
	}

	@Test // DATAMONGO-843
	@SuppressWarnings("resource")
	void auditingUsesFallbackMappingContextIfNoneConfigured() {
		new AnnotationConfigApplicationContext(SimpleConfig.class);
	}

	@Test // DATAMONGO-2139
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
				s -> auditablePersonRepository.saveAll(Collections.singletonList(s)).get(0), //
				null, 0L, 1L);
	}

	@Test // DATAMONGO-2139
	void auditingWorksForVersionedEntityWithSimpleVersion() {

		verifyAuditingViaVersionProperty(new SimpleVersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				auditablePersonRepository::save, //
				0L, 1L, 2L);
	}

	@Test // DATAMONGO-2139
	void auditingWorksForVersionedEntityWithWrapperVersionOnTemplate() {

		verifyAuditingViaVersionProperty(new VersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				operations::save, //
				null, 0L, 1L);
	}

	@Test // DATAMONGO-2139
	void auditingWorksForVersionedEntityWithSimpleVersionOnTemplate() {

		verifyAuditingViaVersionProperty(new SimpleVersionedAuditablePerson(), //
				it -> it.version, //
				AuditablePerson::getCreatedAt, //
				operations::save, //
				0L, 1L, 2L);
	}

	@Test // DATAMONGO-2586
	void auditingShouldOnlyRegisterImperativeAuditingCallback() {

		Object callbacks = ReflectionTestUtils.getField(operations, "entityCallbacks");
		Object callbackDiscoverer = ReflectionTestUtils.getField(callbacks, "callbackDiscoverer");
		List<EntityCallback<?>> actualCallbacks = ReflectionTestUtils.invokeMethod(callbackDiscoverer, "getEntityCallbacks",
				AuditablePerson.class, ResolvableType.forClass(EntityCallback.class));

		assertThat(actualCallbacks) //
				.hasAtLeastOneElementOfType(AuditingEntityCallback.class) //
				.doesNotHaveAnyElementsOfTypes(ReactiveAuditingEntityCallback.class);
	}

	private <T extends AuditablePerson> void verifyAuditingViaVersionProperty(T instance,
			Function<T, Object> versionExtractor, Function<T, Object> createdDateExtractor, Function<T, T> persister,
			Object... expectedValues) {

		MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(instance.getClass());

		assertThat(versionExtractor.apply(instance)).isEqualTo(expectedValues[0]);
		assertThat(createdDateExtractor.apply(instance)).isNull();
		assertThat(entity.isNew(instance)).isTrue();

		instance = persister.apply(instance);

		assertThat(versionExtractor.apply(instance)).isEqualTo(expectedValues[1]);
		assertThat(createdDateExtractor.apply(instance)).isNotNull();
		assertThat(entity.isNew(instance)).isFalse();

		instance = persister.apply(instance);

		assertThat(versionExtractor.apply(instance)).isEqualTo(expectedValues[2]);
		assertThat(entity.isNew(instance)).isFalse();
	}

	@Repository
	interface AuditablePersonRepository extends MongoRepository<AuditablePerson, String> {}

	@Configuration
	@EnableMongoRepositories
	static class SimpleConfigWithRepositories extends SimpleConfig {}

	@Configuration
	@EnableMongoAuditing
	static class SimpleConfig extends MongoClientClosingTestConfiguration {

		@Override
		public MongoClient mongoClient() {
			return MongoTestUtils.client();
		}

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.emptySet();
		}
	}

	static class VersionedAuditablePerson extends AuditablePerson {
		@Version Long version;
	}

	static class SimpleVersionedAuditablePerson extends AuditablePerson {
		@Version long version;
	}
}
