/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.Test;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

import com.mongodb.Mongo;

/**
 * Integration tests for {@link MongoDbFactoryParser}.
 *
 * @author Oliver Gierke
 */
public class MongoDbFactoryParserIntegrationTests {

	@Test
	public void parsesWriteConcern() {
		XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("namespace/db-factory-bean.xml"));
		BeanDefinition definition = factory.getBeanDefinition("first");
		
		List<PropertyValue> values = definition.getPropertyValues().getPropertyValueList();
		assertThat(values, hasItem(new PropertyValue("writeConcern", "SAFE")));
	}
	
	@Test
	public void createsDbFactoryBean() {
		XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("namespace/db-factory-bean.xml"));
		factory.getBean("first");
	}
	
	/**
	 * @see DATADOC-280
	 */
	@Test
	public void parsesMaxAutoConnectRetryTimeCorrectly() {
		
		XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("namespace/db-factory-bean.xml"));
		Mongo mongo = factory.getBean(Mongo.class);
		assertThat(mongo.getMongoOptions().maxAutoConnectRetryTime, is(27L));
	}
}
