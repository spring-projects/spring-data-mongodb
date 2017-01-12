/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.data.mongodb.repository.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.repository.ReactivePersonRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Integration tests for {@link ReactiveMongoRepositoriesRegistrar}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ReactiveMongoRepositoriesRegistrarIntegrationTests {

	@Configuration
	@EnableReactiveMongoRepositories(basePackages = "org.springframework.data.mongodb.repository")
	static class Config {

		@Bean
		public ReactiveMongoTemplate reactiveMongoTemplate() throws Exception {
			return new ReactiveMongoTemplate(new SimpleReactiveMongoDatabaseFactory(MongoClients.create(), "database"));
		}
	}

	@Autowired ReactivePersonRepository personRepository;
	@Autowired ApplicationContext context;

	@Test // DATAMONGO-1444
	public void testConfiguration() {}
}
