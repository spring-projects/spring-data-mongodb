/*
 * Copyright 2010-2013 the original author or authors.
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

import static org.junit.Assert.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import javax.net.ssl.SSLSocketFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoFactoryBean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.WriteConcern;

/**
 * Integration tests for the MongoDB namespace.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Martin Baumgartner
 * @author Thomas Darimont
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoNamespaceTests {

	@Autowired private ApplicationContext ctx;

	@Test
	public void testMongoSingleton() throws Exception {
		assertTrue(ctx.containsBean("noAttrMongo"));
		MongoFactoryBean mfb = (MongoFactoryBean) ctx.getBean("&noAttrMongo");
		assertNull(getField(mfb, "host"));
		assertNull(getField(mfb, "port"));
	}

	@Test
	public void testMongoSingletonWithAttributes() throws Exception {

		assertTrue(ctx.containsBean("defaultMongo"));
		MongoFactoryBean mfb = (MongoFactoryBean) ctx.getBean("&defaultMongo");
		String host = (String) getField(mfb, "host");
		Integer port = (Integer) getField(mfb, "port");
		assertEquals("localhost", host);
		assertEquals(new Integer(27017), port);

		MongoOptions options = (MongoOptions) getField(mfb, "mongoOptions");
		assertFalse("By default socketFactory should not be a SSLSocketFactory",
				options.getSocketFactory() instanceof SSLSocketFactory);
	}

	/**
	 * @see DATAMONGO-764
	 */
	@Test
	public void testMongoSingletonWithSslEnabled() throws Exception {

		assertTrue(ctx.containsBean("mongoSsl"));
		MongoFactoryBean mfb = (MongoFactoryBean) ctx.getBean("&mongoSsl");

		MongoOptions options = (MongoOptions) getField(mfb, "mongoOptions");
		assertTrue("socketFactory should be a SSLSocketFactory", options.getSocketFactory() instanceof SSLSocketFactory);
	}

	/**
	 * @see DATAMONGO-764
	 */
	@Test
	public void testMongoSingletonWithSslEnabledAndCustomSslSocketFactory() throws Exception {

		assertTrue(ctx.containsBean("mongoSslWithCustomSslFactory"));
		MongoFactoryBean mfb = (MongoFactoryBean) ctx.getBean("&mongoSslWithCustomSslFactory");

		SSLSocketFactory customSslSocketFactory = ctx.getBean("customSslSocketFactory", SSLSocketFactory.class);
		MongoOptions options = (MongoOptions) getField(mfb, "mongoOptions");

		assertTrue("socketFactory should be a SSLSocketFactory", options.getSocketFactory() instanceof SSLSocketFactory);
		assertSame(customSslSocketFactory, options.getSocketFactory());
	}

	@Test
	public void testSecondMongoDbFactory() {
		assertTrue(ctx.containsBean("secondMongoDbFactory"));
		MongoDbFactory dbf = (MongoDbFactory) ctx.getBean("secondMongoDbFactory");
		Mongo mongo = (Mongo) getField(dbf, "mongo");
		assertEquals("localhost", mongo.getAddress().getHost());
		assertEquals(27017, mongo.getAddress().getPort());
		assertEquals(new UserCredentials("joe", "secret"), getField(dbf, "credentials"));
		assertEquals("database", getField(dbf, "databaseName"));
	}

	/**
	 * @see DATAMONGO-140
	 */
	@Test
	public void testMongoTemplateFactory() {
		assertTrue(ctx.containsBean("mongoTemplate"));
		MongoOperations operations = (MongoOperations) ctx.getBean("mongoTemplate");
		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "mongoDbFactory");
		assertEquals("database", getField(dbf, "databaseName"));
		MongoConverter converter = (MongoConverter) getField(operations, "mongoConverter");
		assertNotNull(converter);
	}

	/**
	 * @see DATAMONGO-140
	 */
	@Test
	public void testSecondMongoTemplateFactory() {
		assertTrue(ctx.containsBean("anotherMongoTemplate"));
		MongoOperations operations = (MongoOperations) ctx.getBean("anotherMongoTemplate");
		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "mongoDbFactory");
		assertEquals("database", getField(dbf, "databaseName"));
		WriteConcern writeConcern = (WriteConcern) getField(operations, "writeConcern");
		assertEquals(WriteConcern.SAFE, writeConcern);
	}

	/**
	 * @see DATAMONGO-628
	 */
	@Test
	public void testGridFsTemplateFactory() {
		assertTrue(ctx.containsBean("gridFsTemplate"));
		GridFsOperations operations = (GridFsOperations) ctx.getBean("gridFsTemplate");
		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "dbFactory");
		assertEquals("database", getField(dbf, "databaseName"));
		MongoConverter converter = (MongoConverter) getField(operations, "converter");
		assertNotNull(converter);
	}

	/**
	 * @see DATAMONGO-628
	 */
	@Test
	public void testSecondGridFsTemplateFactory() {
		assertTrue(ctx.containsBean("antoherGridFsTemplate"));
		GridFsOperations operations = (GridFsOperations) ctx.getBean("antoherGridFsTemplate");
		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "dbFactory");
		assertEquals("database", getField(dbf, "databaseName"));
		MongoConverter converter = (MongoConverter) getField(operations, "converter");
		assertNotNull(converter);
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testMongoSingletonWithPropertyPlaceHolders() throws Exception {
		assertTrue(ctx.containsBean("mongo"));
		MongoFactoryBean mfb = (MongoFactoryBean) ctx.getBean("&mongo");
		String host = (String) getField(mfb, "host");
		Integer port = (Integer) getField(mfb, "port");
		assertEquals("127.0.0.1", host);
		assertEquals(new Integer(27017), port);
		Mongo mongo = mfb.getObject();
		MongoOptions mongoOpts = mongo.getMongoOptions();
		assertEquals(8, mongoOpts.connectionsPerHost);
		assertEquals(1000, mongoOpts.connectTimeout);
		assertEquals(1500, mongoOpts.maxWaitTime);
		assertEquals(true, mongoOpts.autoConnectRetry);
		assertEquals(1500, mongoOpts.socketTimeout);
		assertEquals(4, mongoOpts.threadsAllowedToBlockForConnectionMultiplier);
		assertEquals(true, mongoOpts.socketKeepAlive);
		assertEquals(true, mongoOpts.fsync);
		assertEquals(true, mongoOpts.slaveOk);
		assertEquals(1, mongoOpts.getWriteConcern().getW());
		assertEquals(0, mongoOpts.getWriteConcern().getWtimeout());
		assertEquals(true, mongoOpts.getWriteConcern().fsync());
	}
}
