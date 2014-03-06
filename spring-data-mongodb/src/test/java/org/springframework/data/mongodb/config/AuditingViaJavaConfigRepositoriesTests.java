/*
 * Copyright 2013 the original author or authors.
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.mongodb.core.AuditablePerson;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
	static class Config {

		@Bean
		public MongoOperations mongoTemplate() throws Exception {
			return new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "database"));
		}

		@Bean
		public MongoMappingContext mappingContext() {
			return new MongoMappingContext();
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

	@Test
	public void basicAuditing() {

		doReturn(this.auditor).when(this.auditorAware).getCurrentAuditor();

		AuditablePerson user = new AuditablePerson("user");

		AuditablePerson savedUser = auditablePersonRepository.save(user);
		System.out.println(savedUser);

		AuditablePerson createdBy = savedUser.getCreatedBy();
		assertThat(createdBy, is(notNullValue()));
		assertThat(createdBy.getFirstname(), is(this.auditor.getFirstname()));
	}

	/**
	 * @see DATAMONGO-843
	 */
	@Test
	@SuppressWarnings("resource")
	public void defaultsMappingContextIfNoneConfigured() {
		new AnnotationConfigApplicationContext(SampleConfig.class);
	}

	@Repository
	static interface AuditablePersonRepository extends MongoRepository<AuditablePerson, String> {}

	@Configuration
	@EnableMongoAuditing
	static class SampleConfig {

	}
}
