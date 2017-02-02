/*
 * Copyright 2013-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.config;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.mongodb.core.AuditablePerson;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * Integration tests for auditing via Java config.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AuditingViaJavaConfigRepositoriesTests {

	@Autowired AuditablePersonRepository auditablePersonRepository;
	@Autowired AuditorAware<AuditablePerson> auditorAware;
	AuditablePerson auditor;

	@Configuration
	@EnableMongoAuditing(auditorAwareRef = "auditorProvider")
	@EnableMongoRepositories(basePackageClasses = AuditablePersonRepository.class, considerNestedRepositories = true)
	static class Config extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		public Mongo mongo() throws Exception {
			return new MongoClient();
		}

		@Bean
		@SuppressWarnings("unchecked")
		public AuditorAware<AuditablePerson> auditorProvider() {
			return mock(AuditorAware.class);
		}
	}

	@Before
	public void setup() {
		auditablePersonRepository.deleteAll();
		this.auditor = auditablePersonRepository.save(new AuditablePerson("auditor"));
	}

	@Test // DATAMONGO-792, DATAMONGO-883
	public void basicAuditing() {

		doReturn(Optional.of(this.auditor)).when(this.auditorAware).getCurrentAuditor();

		AuditablePerson savedUser = auditablePersonRepository.save(new AuditablePerson("user"));

		AuditablePerson createdBy = savedUser.getCreatedBy();

		assertThat(createdBy, is(notNullValue()));
		assertThat(createdBy.getFirstname(), is(this.auditor.getFirstname()));
		assertThat(savedUser.getCreatedAt(), is(notNullValue()));
	}

	@Test // DATAMONGO-843
	@SuppressWarnings("resource")
	public void auditingUsesFallbackMappingContextIfNoneConfiguredWithRepositories() {
		new AnnotationConfigApplicationContext(SimpleConfigWithRepositories.class);
	}

	@Test // DATAMONGO-843
	@SuppressWarnings("resource")
	public void auditingUsesFallbackMappingContextIfNoneConfigured() {
		new AnnotationConfigApplicationContext(SimpleConfig.class);
	}

	@Repository
	static interface AuditablePersonRepository extends MongoRepository<AuditablePerson, String> {}

	@Configuration
	@EnableMongoRepositories
	static class SimpleConfigWithRepositories extends SimpleConfig {}

	@Configuration
	@EnableMongoAuditing
	static class SimpleConfig extends AbstractMongoConfiguration {

		@Override
		public Mongo mongo() throws Exception {
			return new MongoClient();
		}

		@Override
		protected String getDatabaseName() {
			return "database";
		}
	}
}
