/*
 * Copyright 2012 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
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
public class MongoNamespaceIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	DefaultListableBeanFactory factory;
	BeanDefinitionReader reader;

	@Autowired
	ApplicationContext context;

	@Before
	@Override
	public void setUp() throws InterruptedException {
		super.setUp();
		factory = new DefaultListableBeanFactory();
		reader = new XmlBeanDefinitionReader(factory);
	}

	@Test
	public void assertDefaultMappingContextIsWired() {

		reader.loadBeanDefinitions(new ClassPathResource("MongoNamespaceIntegrationTests-context.xml", getClass()));
		BeanDefinition definition = factory.getBeanDefinition("personRepository");
		assertThat(definition, is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-581
	 */
	@Test
	public void exposesPersistentEntity() {

		Repositories repositories = new Repositories(context);
		PersistentEntity<?, ?> entity = repositories.getPersistentEntity(Person.class);
		assertThat(entity, is(notNullValue()));
		assertThat(entity, is(instanceOf(MongoPersistentEntity.class)));
	}
}
