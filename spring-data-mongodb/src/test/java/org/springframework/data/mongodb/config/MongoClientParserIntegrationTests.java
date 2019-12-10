/*
 * Copyright 2015-2019 the original author or authors.
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

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;

/**
 * Integration tests for {@link MongoClientParser}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoClientParserIntegrationTests {

	DefaultListableBeanFactory factory;
	BeanDefinitionReader reader;

	@Before
	public void setUp() {

		this.factory = new DefaultListableBeanFactory();
		this.reader = new XmlBeanDefinitionReader(factory);
	}

	@Test // DATAMONGO-1158
	public void createsMongoClientCorrectlyWhenGivenHostAndPort() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongoClient-bean.xml"));

		assertThat(factory.getBean("mongo-client-with-host-and-port")).isInstanceOf(MongoClient.class);
	}

	@Test // DATAMONGO-1158, DATAMONGO-2199
	public void createsMongoClientWithOptionsCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongoClient-bean.xml"));

		try (AbstractApplicationContext context = new GenericApplicationContext(factory)) {
			context.refresh();

			MongoClientSettings settings = extractClientSettingsFromBean(context,
					"mongo-client-with-options-for-write-concern-and-read-preference");
			assertThat(settings.getReadPreference()).isEqualTo(ReadPreference.secondary());
			assertThat(settings.getWriteConcern()).isEqualTo(WriteConcern.UNACKNOWLEDGED);
		}
	}

	@Test // DATAMONGO-1158
	public void createsMongoClientWithDefaultsCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongoClient-bean.xml"));

		try (AbstractApplicationContext context = new GenericApplicationContext(factory)) {

			context.refresh();

			MongoClient client = context.getBean("mongoClient", MongoClient.class);
			assertThat(client.getClusterDescription().getClusterSettings().getHosts()).containsExactly(new ServerAddress());
		}
	}

	@Test // DATAMONGO-1158
	public void createsMongoClientWithCredentialsCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongoClient-bean.xml"));

		try (AbstractApplicationContext context = new GenericApplicationContext(factory)) {

			context.refresh();

			MongoClientSettings settings = extractClientSettingsFromBean(context, "mongo-client-with-credentials");

			assertThat(settings.getCredential())
					.isEqualTo(MongoCredential.createPlainCredential("jon", "snow", "warg".toCharArray()));
		}
	}

	@Test // DATAMONGO-1620
	public void createsMongoClientWithServerSelectionTimeoutCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongoClient-bean.xml"));

		try (AbstractApplicationContext context = new GenericApplicationContext(factory)) {
			context.refresh();

			MongoClientSettings settings = extractClientSettingsFromBean(context,
					"mongo-client-with-server-selection-timeout");
			assertThat(settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS)).isEqualTo(100);
		}
	}

	private MongoClientSettings extractClientSettingsFromBean(AbstractApplicationContext context, String beanName) {
		return extractClientSettings(context.getBean(beanName, MongoClient.class));
	}

	private MongoClientSettings extractClientSettings(MongoClient client) {
		return (MongoClientSettings) ReflectionTestUtils.getField(client, "settings");
	}
}
