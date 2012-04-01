package org.springframework.data.mongodb.repository.config;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.repository.AbstractPersonRepositoryIntegrationTests;
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
}
