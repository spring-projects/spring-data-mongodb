/*
 * Copyright 2011-2015 the original author or authors.
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
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.util.MongoClientVersion.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.config.ConstructorArgumentValues.ValueHolder;
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.ReflectiveMongoOptionsInvokerTestUtil;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import com.mongodb.WriteConcern;

/**
 * Integration tests for {@link MongoDbFactoryParser}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Viktor Khoroshko
 */
public class MongoDbFactoryParserIntegrationTests {

	DefaultListableBeanFactory factory;
	BeanDefinitionReader reader;

	@BeforeClass
	public static void validateMongoDriver() {
		assumeFalse(isMongo3Driver());
	}

	@Before
	public void setUp() {
		factory = new DefaultListableBeanFactory();
		reader = new XmlBeanDefinitionReader(factory);
	}

	@Test
	public void testWriteConcern() throws Exception {

		SimpleMongoDbFactory dbFactory = new SimpleMongoDbFactory(new MongoClient("localhost"), "database");
		dbFactory.setWriteConcern(WriteConcern.SAFE);
		dbFactory.getDb();

		assertThat(ReflectionTestUtils.getField(dbFactory, "writeConcern"), is((Object) WriteConcern.SAFE));
	}

	@Test
	public void parsesWriteConcern() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("namespace/db-factory-bean.xml");
		assertWriteConcern(ctx, WriteConcern.SAFE);
	}

	@Test
	public void parsesCustomWriteConcern() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
				"namespace/db-factory-bean-custom-write-concern.xml");
		assertWriteConcern(ctx, new WriteConcern("rack1"));
	}

	/**
	 * @see DATAMONGO-331
	 */
	@Test
	public void readsReplicasWriteConcernCorrectly() {

		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext(
				"namespace/db-factory-bean-custom-write-concern.xml");
		MongoDbFactory factory = ctx.getBean("second", MongoDbFactory.class);
		DB db = factory.getDb();

		assertThat(db.getWriteConcern(), is(WriteConcern.REPLICAS_SAFE));
		ctx.close();
	}

	// This test will fail since equals in WriteConcern uses == for _w and not .equals
	public void testWriteConcernEquality() {
		String s1 = new String("rack1");
		String s2 = new String("rack1");
		WriteConcern wc1 = new WriteConcern(s1);
		WriteConcern wc2 = new WriteConcern(s2);
		assertThat(wc1, is(wc2));
	}

	@Test
	public void createsDbFactoryBean() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/db-factory-bean.xml"));
		factory.getBean("first");
	}

	/**
	 * @see DATAMONGO-280
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void parsesMaxAutoConnectRetryTimeCorrectly() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/db-factory-bean.xml"));
		Mongo mongo = factory.getBean(Mongo.class);
		assertThat(ReflectiveMongoOptionsInvokerTestUtil.getMaxAutoConnectRetryTime(mongo.getMongoOptions()), is(27L));
	}

	/**
	 * @see DATAMONGO-295
	 */
	@Test
	public void setsUpMongoDbFactoryUsingAMongoUri() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-uri.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongoDbFactory");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount(), is(1));
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoURI.class);
		assertThat(argument, is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-306
	 */
	@Test
	public void setsUpMongoDbFactoryUsingAMongoUriWithoutCredentials() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-uri-no-credentials.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongoDbFactory");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount(), is(1));
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoURI.class);
		assertThat(argument, is(notNullValue()));

		MongoDbFactory dbFactory = factory.getBean("mongoDbFactory", MongoDbFactory.class);
		DB db = dbFactory.getDb();
		assertThat(db.getName(), is("database"));
	}

	/**
	 * @see DATAMONGO-295
	 */
	@Test(expected = BeanDefinitionParsingException.class)
	public void rejectsUriPlusDetailedConfiguration() {
		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-uri-and-details.xml"));
	}

	/**
	 * @see DATAMONGO-1218
	 */
	@Test
	public void setsUpMongoDbFactoryUsingAMongoClientUri() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-client-uri.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongoDbFactory");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount(), is(1));
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoClientURI.class);
		assertThat(argument, is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-1218
	 */
	@Test(expected = BeanDefinitionParsingException.class)
	public void rejectsClientUriPlusDetailedConfiguration() {
		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-client-uri-and-details.xml"));
	}

	/**
	 * @see DATAMONGO-1293
	 */
	@Test
	public void setsUpClientUriWithId() {
		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-client-uri-and-id.xml"));
		BeanDefinition definition = factory.getBeanDefinition("testMongo");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount(), is(1));
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoClientURI.class);
		assertThat(argument, is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-1293
	 */
	@Test
	public void setsUpUriWithId() {
		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-uri-and-id.xml"));
		BeanDefinition definition = factory.getBeanDefinition("testMongo");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount(), is(1));
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoClientURI.class);
		assertThat(argument, is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-1293
	 */
	@Test(expected = BeanDefinitionParsingException.class)
	public void rejectsClientUriPlusDetailedConfigurationAndWriteConcern() {
		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-client-uri-write-concern-and-details.xml"));
	}

	/**
	 * @see DATAMONGO-1293
	 */
	@Test(expected = BeanDefinitionParsingException.class)
	public void rejectsUriPlusDetailedConfigurationAndWriteConcern() {
		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-client-uri-write-concern-and-details.xml"));
	}

	private static void assertWriteConcern(ClassPathXmlApplicationContext ctx, WriteConcern expectedWriteConcern) {

		SimpleMongoDbFactory dbFactory = ctx.getBean("first", SimpleMongoDbFactory.class);
		DB db = dbFactory.getDb();
		assertThat(db.getName(), is("db"));

		WriteConcern configuredConcern = (WriteConcern) ReflectionTestUtils.getField(dbFactory, "writeConcern");

		MyWriteConcern myDbFactoryWriteConcern = new MyWriteConcern(configuredConcern);
		MyWriteConcern myDbWriteConcern = new MyWriteConcern(db.getWriteConcern());
		MyWriteConcern myExpectedWriteConcern = new MyWriteConcern(expectedWriteConcern);

		assertThat(myDbFactoryWriteConcern, is(myExpectedWriteConcern));
		assertThat(myDbWriteConcern, is(myExpectedWriteConcern));
		assertThat(myDbWriteConcern, is(myDbFactoryWriteConcern));
	}
}
