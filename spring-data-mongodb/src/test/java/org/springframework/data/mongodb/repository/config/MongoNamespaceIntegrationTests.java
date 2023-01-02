/*
 * Copyright 2012-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.AbstractPersonRepositoryIntegrationTests;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.repository.support.Repositories;
import org.springframework.test.context.ContextConfiguration;

/**
 * Test class using the namespace configuration to set up the repository instance.
 *
 * @author Oliver Gierke
 */
@ContextConfiguration
class MongoNamespaceIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	DefaultListableBeanFactory factory;
	BeanDefinitionReader reader;

	@Autowired ApplicationContext context;

	@BeforeEach
	public void setUp() throws InterruptedException {
		factory = new DefaultListableBeanFactory();
		reader = new XmlBeanDefinitionReader(factory);
	}

	@Test
	void assertDefaultMappingContextIsWired() {

		reader.loadBeanDefinitions(new ClassPathResource("MongoNamespaceIntegrationTests-context.xml", getClass()));
		BeanDefinition definition = factory.getBeanDefinition("personRepository");
		assertThat(definition).isNotNull();
	}

	@Test // DATAMONGO-581
	void exposesPersistentEntity() {

		Repositories repositories = new Repositories(context);
		PersistentEntity<?, ?> entity = repositories.getPersistentEntity(Person.class);
		assertThat(entity).isNotNull();
		assertThat(entity).isInstanceOf(MongoPersistentEntity.class);
	}
}
