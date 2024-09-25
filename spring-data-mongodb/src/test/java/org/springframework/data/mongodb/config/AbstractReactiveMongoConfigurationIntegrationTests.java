/*
 * Copyright 2016-2024 the original author or authors.
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

import static org.assertj.core.api.AssertionsForInterfaceTypes.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration tests for {@link AbstractReactiveMongoConfiguration}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class AbstractReactiveMongoConfigurationIntegrationTests {

	@Autowired ApplicationContext context;

	@Test // DATAMONGO-1444
	public void contextShouldContainTemplate() {

		assertThat(context.getBean(SimpleReactiveMongoDatabaseFactory.class)).isNotNull();
		assertThat(context.getBean(ReactiveMongoOperations.class)).isNotNull();
		assertThat(context.getBean(ReactiveMongoTemplate.class)).isNotNull();
	}

	@Configuration
	static class ReactiveConfiguration extends AbstractReactiveMongoConfiguration {

		@Override
		public MongoClient reactiveMongoClient() {
			return Mockito.mock(MongoClient.class);
		}

		@Override
		protected String getDatabaseName() {
			return "database";
		}
	}
}
