/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.TestMongoConfiguration;
import org.springframework.data.mongodb.repository.aot.AotFragmentTestConfigurationSupport;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration tests for {@link PersonRepository} with mounted AOT-generated repository methods.
 *
 * @author Mark Paluch
 */
@ContextConfiguration(classes = AotPersonRepositoryIntegrationTests.Config.class)
class AotPersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	@Configuration
	@ImportResource("classpath:/org/springframework/data/mongodb/repository/PersonRepositoryIntegrationTests-infrastructure.xml")
	static class Config extends TestMongoConfiguration {

		@Bean
		static AotFragmentTestConfigurationSupport aot() {
			return new AotFragmentTestConfigurationSupport(PersonRepository.class, false);
		}

		@Bean
		PersonRepository personRepository(MongoOperations mongoOperations, ApplicationContext context) {

			MongoRepositoryFactory factory = new MongoRepositoryFactory(mongoOperations);
			factory.setBeanFactory(context);
			factory.setBeanClassLoader(context.getClassLoader());
			factory.setEnvironment(context.getEnvironment());

			Object aotFragment = context.getBean("fragment");

			return factory.getRepository(PersonRepository.class, RepositoryComposition.RepositoryFragments.just(aotFragment));
		}

	}

	@Test // DATAMONGO-1608
	@Disabled
	void findByFirstnameLikeWithNull() {
		super.findByFirstnameLikeWithNull();
	}

	@Test // GH-3395
	@Disabled
	void caseInSensitiveInClauseQuotesExpressions() {
		super.caseInSensitiveInClauseQuotesExpressions();
	}

	@Test // DATAMONGO-1608
	@Disabled
	void findByFirstNameIgnoreCaseWithNull() {
		super.findByFirstNameIgnoreCaseWithNull();
	}

	@Test // GH-3395
	@Disabled
	void caseSensitiveInClauseIgnoresExpressions() {
		super.caseSensitiveInClauseIgnoresExpressions();
	}

	@Test // DATAMONGO-990
	@Disabled
	void shouldFindByFirstnameAndCurrentUserWithCustomQuery() {
		super.shouldFindByFirstnameAndCurrentUserWithCustomQuery();
	}

	@Test // GH-3395, GH-4404
	@Disabled
	void caseInSensitiveInClause() {
		super.caseInSensitiveInClause();
	}

}
