/*
 * Copyright 2023 the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.config.LifecycleProxyFactoryBean;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;

/**
 * @author Oliver Drotbohm
 */
class AbstractMongoClientConfigurationIntegrationTests {

	@Test
	void rebootstrapsMongoClientOnLifecycleRestart() {

		try (ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(SampleConfiguration.class)) {

			MongoTemplate template = context.getBean(MongoTemplate.class);
			assertThatNoException().isThrownBy(() -> template.findAll(Sample.class));

			context.stop();
			context.start();

			assertThatNoException().isThrownBy(() -> template.findAll(Sample.class));
		}
	}

	static class Sample {}

	static class SampleConfiguration extends AbstractMongoClientConfiguration {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.config.MongoConfigurationSupport#getDatabaseName()
		 */
		@Override
		protected String getDatabaseName() {
			return "testDatabase";
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.config.AbstractMongoClientConfiguration#mongoClient()
		 */
		@Override
		@Bean
		public MongoClient mongoClient() {
			// TODO Auto-generated method stub
			return super.mongoClient();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.config.AbstractMongoClientConfiguration#createMongoClient(com.mongodb.MongoClientSettings)
		 */
		@Override
		protected MongoClient createMongoClient(MongoClientSettings settings) {

			return new LifecycleProxyFactoryBean<>(MongoClient.class, () -> super.createMongoClient(settings),
					it -> it.close()).getObject();
		}
	}
}
