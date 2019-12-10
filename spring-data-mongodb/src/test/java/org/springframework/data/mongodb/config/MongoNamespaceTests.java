/*
 * Copyright 2010-2019 the original author or authors.
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
import static org.springframework.test.util.ReflectionTestUtils.*;

import javax.net.ssl.SSLSocketFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoClientFactoryBean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;

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

		assertThat(ctx.containsBean("noAttrMongo")).isTrue();
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&noAttrMongo");

		assertThat(getField(mfb, "host")).isNull();
		assertThat(getField(mfb, "port")).isNull();
	}

	@Test
	public void testMongoSingletonWithAttributes() throws Exception {

		assertThat(ctx.containsBean("defaultMongo")).isTrue();
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&defaultMongo");

		String host = (String) getField(mfb, "host");
		Integer port = (Integer) getField(mfb, "port");

		assertThat(host).isEqualTo("localhost");
		assertThat(port).isEqualTo(new Integer(27017));

		MongoClientSettings options = (MongoClientSettings) getField(mfb, "mongoClientSettings");
		assertThat(options).isNull();
	}

	@Test // DATAMONGO-764
	public void testMongoSingletonWithSslEnabled() throws Exception {

		assertThat(ctx.containsBean("mongoSsl")).isTrue();
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&mongoSsl");

		MongoClientSettings options = (MongoClientSettings) getField(mfb, "mongoClientSettings");
		assertThat(options.getSslSettings().getContext().getSocketFactory() instanceof SSLSocketFactory)
				.as("socketFactory should be a SSLSocketFactory").isTrue();
	}

	@Test // DATAMONGO-1490
	public void testMongoClientSingletonWithSslEnabled() {

		assertThat(ctx.containsBean("mongoClientSsl")).isTrue();
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&mongoClientSsl");

		MongoClientSettings options = (MongoClientSettings) getField(mfb, "mongoClientSettings");
		assertThat(options.getSslSettings().getContext().getSocketFactory() instanceof SSLSocketFactory)
				.as("socketFactory should be a SSLSocketFactory").isTrue();
	}

	@Test // DATAMONGO-764
	public void testMongoSingletonWithSslEnabledAndCustomSslSocketFactory() throws Exception {

		assertThat(ctx.containsBean("mongoSslWithCustomSslFactory")).isTrue();
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&mongoSslWithCustomSslFactory");

		MongoClientSettings options = (MongoClientSettings) getField(mfb, "mongoClientSettings");

		assertThat(options.getSslSettings().getContext().getSocketFactory() instanceof SSLSocketFactory)
				.as("socketFactory should be a SSLSocketFactory").isTrue();
		assertThat(options.getSslSettings().getContext().getProvider().getName()).isEqualTo("SunJSSE");
	}

	@Test
	public void testSecondMongoDbFactory() {

		assertThat(ctx.containsBean("secondMongoDbFactory")).isTrue();
		MongoDbFactory dbf = (MongoDbFactory) ctx.getBean("secondMongoDbFactory");

		MongoClient mongo = (MongoClient) getField(dbf, "mongoClient");
		assertThat(mongo.getClusterDescription().getClusterSettings().getHosts()).containsExactly(new ServerAddress());
		assertThat(getField(dbf, "databaseName")).isEqualTo("database");
	}

	@Test // DATAMONGO-789
	public void testThirdMongoDbFactory() {

		assertThat(ctx.containsBean("thirdMongoDbFactory")).isTrue();

		MongoDbFactory dbf = (MongoDbFactory) ctx.getBean("thirdMongoDbFactory");
		MongoClient mongo = (MongoClient) getField(dbf, "mongoClient");

		assertThat(mongo.getClusterDescription().getClusterSettings().getHosts()).containsExactly(new ServerAddress());
		assertThat(getField(dbf, "databaseName")).isEqualTo("database");
	}

	@Test // DATAMONGO-140
	public void testMongoTemplateFactory() {

		assertThat(ctx.containsBean("mongoTemplate")).isTrue();
		MongoOperations operations = (MongoOperations) ctx.getBean("mongoTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "mongoDbFactory");
		assertThat(getField(dbf, "databaseName")).isEqualTo("database");

		MongoConverter converter = (MongoConverter) getField(operations, "mongoConverter");
		assertThat(converter).isNotNull();
	}

	@Test // DATAMONGO-140
	public void testSecondMongoTemplateFactory() {

		assertThat(ctx.containsBean("anotherMongoTemplate")).isTrue();
		MongoOperations operations = (MongoOperations) ctx.getBean("anotherMongoTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "mongoDbFactory");
		assertThat(getField(dbf, "databaseName")).isEqualTo("database");

		WriteConcern writeConcern = (WriteConcern) getField(operations, "writeConcern");
		assertThat(writeConcern).isEqualTo(WriteConcern.ACKNOWLEDGED);
	}

	@Test // DATAMONGO-628
	public void testGridFsTemplateFactory() {

		assertThat(ctx.containsBean("gridFsTemplate")).isTrue();
		GridFsOperations operations = (GridFsOperations) ctx.getBean("gridFsTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "dbFactory");
		assertThat(getField(dbf, "databaseName")).isEqualTo("database");

		MongoConverter converter = (MongoConverter) getField(operations, "converter");
		assertThat(converter).isNotNull();
	}

	@Test // DATAMONGO-628
	public void testSecondGridFsTemplateFactory() {

		assertThat(ctx.containsBean("secondGridFsTemplate")).isTrue();
		GridFsOperations operations = (GridFsOperations) ctx.getBean("secondGridFsTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "dbFactory");
		assertThat(getField(dbf, "databaseName")).isEqualTo("database");
		assertThat(getField(operations, "bucket")).isEqualTo(null);

		MongoConverter converter = (MongoConverter) getField(operations, "converter");
		assertThat(converter).isNotNull();
	}

	@Test // DATAMONGO-823
	public void testThirdGridFsTemplateFactory() {

		assertThat(ctx.containsBean("thirdGridFsTemplate")).isTrue();
		GridFsOperations operations = (GridFsOperations) ctx.getBean("thirdGridFsTemplate");

		MongoDbFactory dbf = (MongoDbFactory) getField(operations, "dbFactory");
		assertThat(getField(dbf, "databaseName")).isEqualTo("database");
		assertThat(getField(operations, "bucket")).isEqualTo("bucketString");

		MongoConverter converter = (MongoConverter) getField(operations, "converter");
		assertThat(converter).isNotNull();
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testMongoSingletonWithPropertyPlaceHolders() throws Exception {

		assertThat(ctx.containsBean("mongoClient")).isTrue();
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&mongoClient");

		String host = (String) getField(mfb, "host");
		Integer port = (Integer) getField(mfb, "port");

		assertThat(host).isEqualTo("127.0.0.1");
		assertThat(port).isEqualTo(new Integer(27017));

		// MongoClient mongo = mfb.getObject();
		// MongoClientSettings mongoOpts = mongo.getClusterDescription()..getMongoClientSettings();
		//
		// assertThat(mongoOpts.getConnectionsPerHost()).isEqualTo(8);
		// assertThat(mongoOpts.getConnectTimeout()).isEqualTo(1000);
		// assertThat(mongoOpts.getMaxWaitTime()).isEqualTo(1500);
		//
		// assertThat(mongoOpts.getSocketTimeout()).isEqualTo(1500);
		// assertThat(mongoOpts.getThreadsAllowedToBlockForConnectionMultiplier()).isEqualTo(4);

		// TODO: check the damned defaults
		// assertEquals("w", mongoOpts.getWriteConcern().getW());
		// assertEquals(0, mongoOpts.getWriteConcern().getWtimeout());
		// assertEquals(true, mongoOpts.getWriteConcern().fsync());
	}
}
