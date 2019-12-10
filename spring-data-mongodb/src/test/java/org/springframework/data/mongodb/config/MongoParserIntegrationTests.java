/*
 * Copyright 2011-2019 the original author or authors.
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

import java.util.List;

import com.mongodb.client.MongoClient;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;


/**
 * Integration tests for {@link MongoClientParser}.
 *
 * @author Oliver Gierke
 */
public class MongoParserIntegrationTests {

	DefaultListableBeanFactory factory;
	BeanDefinitionReader reader;

	@Before
	public void setUp() {

		this.factory = new DefaultListableBeanFactory();
		this.reader = new XmlBeanDefinitionReader(factory);
	}

	@Test
	@Ignore
	public void readsMongoAttributesCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-bean.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongoClient");

		List<PropertyValue> values = definition.getPropertyValues().getPropertyValueList();

		values.forEach(System.out::println);
		assertThat(values.get(2).getValue()).isInstanceOf(BeanDefinition.class);
		BeanDefinition x = (BeanDefinition) values.get(2).getValue();

		assertThat(x.getPropertyValues().getPropertyValueList()).contains(new PropertyValue("writeConcern", "SAFE"));

		factory.getBean("mongoClient");
	}

	@Test // DATAMONGO-343
	public void readsServerAddressesCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-bean.xml"));

		AbstractApplicationContext context = new GenericApplicationContext(factory);
		context.refresh();

		assertThat(context.getBean("mongo2", MongoClient.class)).isNotNull();
		context.close();
	}
}
