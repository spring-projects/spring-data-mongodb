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

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.config.ConstructorArgumentValues.ValueHolder;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

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

	@Before
	public void setUp() {
		factory = new DefaultListableBeanFactory();
		reader = new XmlBeanDefinitionReader(factory);
	}

	@Test // DATAMONGO-2199
	public void testWriteConcern() throws Exception {

		SimpleMongoDbFactory dbFactory = new SimpleMongoDbFactory(new MongoClient("localhost"), "database");
		dbFactory.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		dbFactory.getDb();

		assertThat(ReflectionTestUtils.getField(dbFactory, "writeConcern")).isEqualTo(WriteConcern.ACKNOWLEDGED);
	}

	@Test // DATAMONGO-2199
	public void parsesWriteConcern() {

		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("namespace/db-factory-bean.xml");
		assertWriteConcern(ctx, WriteConcern.ACKNOWLEDGED);
	}

	@Test // DATAMONGO-2199
	public void parsesCustomWriteConcern() {

		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
				"namespace/db-factory-bean-custom-write-concern.xml");
		assertWriteConcern(ctx, new WriteConcern("rack1"));
	}

	@Test // DATAMONGO-331
	public void readsReplicasWriteConcernCorrectly() {

		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext(
				"namespace/db-factory-bean-custom-write-concern.xml");
		MongoDbFactory factory = ctx.getBean("second", MongoDbFactory.class);
		MongoDatabase db = factory.getDb();

		assertThat(db.getWriteConcern()).isEqualTo(WriteConcern.W2);
		ctx.close();
	}

	// This test will fail since equals in WriteConcern uses == for _w and not .equals
	public void testWriteConcernEquality() {

		String s1 = new String("rack1");
		String s2 = new String("rack1");
		WriteConcern wc1 = new WriteConcern(s1);
		WriteConcern wc2 = new WriteConcern(s2);
		assertThat(wc1).isEqualTo(wc2);
	}

	@Test
	public void createsDbFactoryBean() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/db-factory-bean.xml"));
		factory.getBean("first");
	}

	@Test // DATAMONGO-306
	public void setsUpMongoDbFactoryUsingAMongoUriWithoutCredentials() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-uri-no-credentials.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongoDbFactory");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount()).isOne();
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoURI.class);
		assertThat(argument).isNotNull();

		MongoDbFactory dbFactory = factory.getBean("mongoDbFactory", MongoDbFactory.class);
		MongoDatabase db = dbFactory.getDb();
		assertThat(db.getName()).isEqualTo("database");
	}

	@Test // DATAMONGO-1218
	public void setsUpMongoDbFactoryUsingAMongoClientUri() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-client-uri.xml"));
		BeanDefinition definition = factory.getBeanDefinition("mongoDbFactory");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount()).isOne();
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoClientURI.class);
		assertThat(argument).isNotNull();
	}

	@Test // DATAMONGO-1293
	public void setsUpClientUriWithId() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-client-uri-and-id.xml"));
		BeanDefinition definition = factory.getBeanDefinition("testMongo");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount()).isOne();
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoClientURI.class);
		assertThat(argument).isNotNull();
	}

	@Test // DATAMONGO-1293
	public void setsUpUriWithId() {

		reader.loadBeanDefinitions(new ClassPathResource("namespace/mongo-uri-and-id.xml"));
		BeanDefinition definition = factory.getBeanDefinition("testMongo");
		ConstructorArgumentValues constructorArguments = definition.getConstructorArgumentValues();

		assertThat(constructorArguments.getArgumentCount()).isOne();
		ValueHolder argument = constructorArguments.getArgumentValue(0, MongoClientURI.class);
		assertThat(argument).isNotNull();
	}

	private static void assertWriteConcern(ClassPathXmlApplicationContext ctx, WriteConcern expectedWriteConcern) {

		SimpleMongoDbFactory dbFactory = ctx.getBean("first", SimpleMongoDbFactory.class);
		MongoDatabase db = dbFactory.getDb();
		assertThat(db.getName()).isEqualTo("db");

		WriteConcern configuredConcern = (WriteConcern) ReflectionTestUtils.getField(dbFactory, "writeConcern");

		MyWriteConcern myDbFactoryWriteConcern = new MyWriteConcern(configuredConcern);
		MyWriteConcern myDbWriteConcern = new MyWriteConcern(db.getWriteConcern());
		MyWriteConcern myExpectedWriteConcern = new MyWriteConcern(expectedWriteConcern);

		assertThat(myDbFactoryWriteConcern).isEqualTo(myExpectedWriteConcern);
		assertThat(myDbWriteConcern).isEqualTo(myExpectedWriteConcern);
		assertThat(myDbWriteConcern).isEqualTo(myDbFactoryWriteConcern);
	}
}
