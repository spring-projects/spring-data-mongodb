package org.springframework.data.document.mongodb.repository.config;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.document.mongodb.repository.AbstractPersonRepositoryIntegrationTests;
import org.springframework.test.context.ContextConfiguration;

/**
 * Test class using the namespace configuration to set up the repository instance.
 * 
 * @author Oliver Gierke
 */
@ContextConfiguration
public class MongoNamespaceIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	@Test
	public void assertDefaultMappingContextIsWired() {

		XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("MongoNamespaceIntegrationTests-context.xml",
				getClass()));
		BeanDefinition definition = factory.getBeanDefinition("personRepository");
		assertThat(definition, is(notNullValue()));
	}
}
