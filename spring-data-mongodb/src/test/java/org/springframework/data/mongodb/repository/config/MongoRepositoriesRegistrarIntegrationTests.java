/*
 * Copyright 2012-2019 the original author or authors.
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
package org.springframework.data.mongodb.repository.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDbFactory;
import org.springframework.data.mongodb.repository.PersonRepository;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for {@link MongoRepositoriesRegistrar}.
 *
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoRepositoriesRegistrarIntegrationTests {

	@Configuration
	@EnableMongoRepositories(basePackages = "org.springframework.data.mongodb.repository")
	static class Config {

		@Bean
		public MongoOperations mongoTemplate() throws Exception {
			return new MongoTemplate(new SimpleMongoClientDbFactory(MongoTestUtils.client(), "database"));
		}
	}

	@Autowired PersonRepository personRepository;
	@Autowired ApplicationContext context;

	@Test
	public void testConfiguration() {}
}
