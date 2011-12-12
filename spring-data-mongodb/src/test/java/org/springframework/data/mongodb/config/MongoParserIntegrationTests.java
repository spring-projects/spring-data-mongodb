/*
 * Copyright 2011 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;

import com.mongodb.Mongo;

/**
 * Integration tests for {@link MongoParser}.
 * 
 * @author Oliver Gierke
 */
public class MongoParserIntegrationTests {

	DefaultListableBeanFactory factory;
	BeanDefinitionReader reader;

	@Before
	public void setUp() {
		factory = new DefaultListableBeanFactory();
		reader = new XmlBeanDefinitionReader(factory);
	}

	@Test
	public void readsMongoAttributesCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-bean.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongo");

		List<PropertyValue> values = definition.getPropertyValues().getPropertyValueList();
		assertThat(values, hasItem(new PropertyValue("writeConcern", "SAFE")));

		factory.getBean("mongo");
	}

	/**
	 * @see DATAMONGO-343
	 */
	@Test
	public void readsServerAddressesCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-bean.xml"));

		GenericApplicationContext context = new GenericApplicationContext(factory);
		context.refresh();

		assertThat(context.getBean("mongo2", Mongo.class), is(notNullValue()));
	}
}
