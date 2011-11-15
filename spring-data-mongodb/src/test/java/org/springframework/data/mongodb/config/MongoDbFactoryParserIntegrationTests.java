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
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.config.ConstructorArgumentValues.ValueHolder;
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.MongoDbFactory;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

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
	
	/**
	 * @see DATADOC-295
	 */
	@Test
	public void setsUpMongoDbFactoryUsingAMongoUri() {
		
		XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("namespace/mongo-uri.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongoDbFactory");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();
		
		assertThat(constructorArguments.getArgumentCount(), is(1));
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoURI.class);
		assertThat(argument, is(notNullValue()));
	}
	
	/**
	 * @see DATADOC-306
	 */
	@Test
	public void setsUpMongoDbFactoryUsingAMongoUriWithoutCredentials() {
		
		XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("namespace/mongo-uri-no-credentials.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongoDbFactory");		
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();
		
		assertThat(constructorArguments.getArgumentCount(), is(1));
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoURI.class);
		assertThat(argument, is(notNullValue()));
	
		MongoDbFactory dbFactory = factory.getBean("mongoDbFactory", MongoDbFactory.class);
		DB db = dbFactory.getDb();
		assertThat("database", is(db.getName()));
		
		
	}
	
	/**
	 * @see DATADOC-295
	 */
	@Test(expected = BeanDefinitionParsingException.class)
	public void rejectsUriPlusDetailedConfiguration() {
		new XmlBeanFactory(new ClassPathResource("namespace/mongo-uri-and-details.xml"));
	}
}
