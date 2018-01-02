/*
 * Copyright 2010-2018 the original author or authors.
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
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.util.MongoClientVersion.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import javax.net.ssl.SSLSocketFactory;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoClientFactoryBean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoOptions;
import com.mongodb.WriteConcern;

/**
 * Integration tests for the MongoDB namespace.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Martin Baumgartner
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoNamespaceTests {

	@Autowired ApplicationContext ctx;

	@Test
	public void testMongoSingleton() throws Exception {

		assertTrue(ctx.containsBean("noAttrMongo"));
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&noAttrMongo");

		assertNull(getField(mfb, "host"));
		assertNull(getField(mfb, "port"));
	}

	@Test
	public void testMongoSingletonWithAttributes() throws Exception {

		assertTrue(ctx.containsBean("defaultMongo"));
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&defaultMongo");

		String host = (String) getField(mfb, "host");
		Integer port = (Integer) getField(mfb, "port");

		assertEquals("localhost", host);
		assertEquals(new Integer(27017), port);

		MongoClientOptions options = (MongoClientOptions) getField(mfb, "mongoClientOptions");
		assertFalse("By default socketFactory should not be a SSLSocketFactory",
				options.getSocketFactory() instanceof SSLSocketFactory);
	}

	@Test // DATAMONGO-764
	public void testMongoSingletonWithSslEnabled() throws Exception {

		assertTrue(ctx.containsBean("mongoSsl"));
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&mongoSsl");

		MongoClientOptions options = (MongoClientOptions) getField(mfb, "mongoClientOptions");
		assertTrue("socketFactory should be a SSLSocketFactory", options.getSocketFactory() instanceof SSLSocketFactory);
	}

	@Test // DATAMONGO-1490
	public void testMongoClientSingletonWithSslEnabled() {

		assertTrue(ctx.containsBean("mongoClientSsl"));
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&mongoClientSsl");

		MongoClientOptions options = (MongoClientOptions) getField(mfb, "mongoClientOptions");
		assertTrue("socketFactory should be a SSLSocketFactory", options.getSocketFactory() instanceof SSLSocketFactory);
	}

	@Test // DATAMONGO-764
	public void testMongoSingletonWithSslEnabledAndCustomSslSocketFactory() throws Exception {

		assertTrue(ctx.containsBean("mongoSslWithCustomSslFactory"));
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&mongoSslWithCustomSslFactory");

		SSLSocketFactory customSslSocketFactory = ctx.getBean("customSslSocketFactory", SSLSocketFactory.class);
		MongoClientOptions options = (MongoClientOptions) getField(mfb, "mongoClientOptions");

		assertTrue("socketFactory should be a SSLSocketFactory", options.getSocketFactory() instanceof SSLSocketFactory);
		assertSame(customSslSocketFactory, options.getSocketFactory());
	}

	@Test
	public void testSecondMongoDbFactory() {

		assertTrue(ctx.containsBean("secondMongoDbFactory"));
		MongoDbFactory dbf = (MongoDbFactory) ctx.getBean("secondMongoDbFactory");

		MongoClient mongo = (MongoClient) getField(dbf, "mongoClient");
		assertEquals("127.0.0.1", mongo.getAddress().getHost());
		assertEquals(27017, mongo.getAddress().getPort());
		assertEquals("database", getField(dbf, "databaseName"));
	}

	@Test // DATAMONGO-789
	public void testThirdMongoDbFactory() {

		assertTrue(ctx.containsBean("thirdMongoDbFactory"));

		MongoDbFactory dbf = (MongoDbFactory) ctx.getBean("thirdMongoDbFactory");
		MongoClient mongo = (MongoClient) getField(dbf, "mongoClient");

		assertEquals("127.0.0.1", mongo.getAddress().getHost());
		assertEquals(27017, mongo.getAddress().getPort());
		assertEquals("database", getField(dbf, "databaseName"));
	}

	@Test // DATAMONGO-140
	public void testMongoTemplateFactory() {

		assertTrue(ctx.containsBean("mongoTemplate"));
		MongoOperations operations = (MongoOperations) ctx.getBean("mongoTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "mongoDbFactory");
		assertEquals("database", getField(dbf, "databaseName"));

		MongoConverter converter = (MongoConverter) getField(operations, "mongoConverter");
		assertNotNull(converter);
	}

	@Test // DATAMONGO-140
	public void testSecondMongoTemplateFactory() {

		assertTrue(ctx.containsBean("anotherMongoTemplate"));
		MongoOperations operations = (MongoOperations) ctx.getBean("anotherMongoTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "mongoDbFactory");
		assertEquals("database", getField(dbf, "databaseName"));

		WriteConcern writeConcern = (WriteConcern) getField(operations, "writeConcern");
		assertEquals(WriteConcern.SAFE, writeConcern);
	}

	@Test // DATAMONGO-628
	public void testGridFsTemplateFactory() {

		assertTrue(ctx.containsBean("gridFsTemplate"));
		GridFsOperations operations = (GridFsOperations) ctx.getBean("gridFsTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "dbFactory");
		assertEquals("database", getField(dbf, "databaseName"));

		MongoConverter converter = (MongoConverter) getField(operations, "converter");
		assertNotNull(converter);
	}

	@Test // DATAMONGO-628
	public void testSecondGridFsTemplateFactory() {

		assertTrue(ctx.containsBean("secondGridFsTemplate"));
		GridFsOperations operations = (GridFsOperations) ctx.getBean("secondGridFsTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "dbFactory");
		assertEquals("database", getField(dbf, "databaseName"));
		assertEquals(null, getField(operations, "bucket"));

		MongoConverter converter = (MongoConverter) getField(operations, "converter");
		assertNotNull(converter);
	}

	@Test // DATAMONGO-823
	public void testThirdGridFsTemplateFactory() {

		assertTrue(ctx.containsBean("thirdGridFsTemplate"));
		GridFsOperations operations = (GridFsOperations) ctx.getBean("thirdGridFsTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "dbFactory");
		assertEquals("database", getField(dbf, "databaseName"));
		assertEquals("bucketString", getField(operations, "bucket"));

		MongoConverter converter = (MongoConverter) getField(operations, "converter");
		assertNotNull(converter);
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testMongoSingletonWithPropertyPlaceHolders() throws Exception {

		assertTrue(ctx.containsBean("mongoClient"));
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&mongoClient");

		String host = (String) getField(mfb, "host");
		Integer port = (Integer) getField(mfb, "port");

		assertEquals("127.0.0.1", host);
		assertEquals(new Integer(27017), port);

		MongoClient mongo = mfb.getObject();
		MongoClientOptions mongoOpts = mongo.getMongoClientOptions();

		assertEquals(8, mongoOpts.getConnectionsPerHost());
		assertEquals(1000, mongoOpts.getConnectTimeout());
		assertEquals(1500, mongoOpts.getMaxWaitTime());

		assertEquals(1500, mongoOpts.getSocketTimeout());
		assertEquals(4, mongoOpts.getThreadsAllowedToBlockForConnectionMultiplier());

		// TODO: check the damned defaults
//		assertEquals("w", mongoOpts.getWriteConcern().getW());
//		assertEquals(0, mongoOpts.getWriteConcern().getWtimeout());
//		assertEquals(true, mongoOpts.getWriteConcern().fsync());
	}
}
