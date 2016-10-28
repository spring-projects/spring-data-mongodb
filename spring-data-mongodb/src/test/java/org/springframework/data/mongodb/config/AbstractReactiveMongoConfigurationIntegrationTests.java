/*
 * Copyright 2016 the original author or authors.
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

import static org.assertj.core.api.AssertionsForInterfaceTypes.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Integration tests for {@link AbstractReactiveMongoConfiguration}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = AbstractReactiveMongoConfigurationIntegrationTests.ReactiveConfiguration.class)
public class AbstractReactiveMongoConfigurationIntegrationTests {

	@Autowired ApplicationContext context;

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void contextShouldContainTemplate() {

		assertThat(context.getBean(SimpleReactiveMongoDatabaseFactory.class)).isNotNull();
		assertThat(context.getBean(ReactiveMongoOperations.class)).isNotNull();
		assertThat(context.getBean(ReactiveMongoTemplate.class)).isNotNull();
	}

	@Configuration
	static class ReactiveConfiguration extends AbstractReactiveMongoConfiguration {

		@Override
		public MongoClient mongoClient() {
			return MongoClients.create();
		}

		@Override
		protected String getDatabaseName() {
			return "database";
		}
	}
}
