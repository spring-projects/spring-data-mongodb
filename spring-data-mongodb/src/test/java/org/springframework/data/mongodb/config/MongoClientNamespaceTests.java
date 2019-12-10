/*
 * Copyright 2019 the original author or authors.
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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoClientFactoryBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterType;

/**
 * Integration tests for the MongoDB namespace.
 *
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoClientNamespaceTests {

	@Autowired ApplicationContext ctx;

	@Test // DATAMONGO-2384
	public void clientWithJustHostAndPort() {

		assertThat(ctx.containsBean("client-with-just-host-port")).isTrue();
		MongoClientFactoryBean factoryBean = ctx.getBean("&client-with-just-host-port",
				MongoClientFactoryBean.class);

		assertThat(getField(factoryBean, "host")).isEqualTo("127.0.0.1");
		assertThat(getField(factoryBean, "port")).isEqualTo(27017);
		assertThat(getField(factoryBean, "connectionString")).isNull();
		assertThat(getField(factoryBean, "credential")).isNull();
		assertThat(getField(factoryBean, "replicaSet")).isNull();
		assertThat(getField(factoryBean, "mongoClientSettings")).isNull();
	}

	@Test // DATAMONGO-2384
	public void clientWithConnectionString() {

		assertThat(ctx.containsBean("client-with-connection-string")).isTrue();
		MongoClientFactoryBean factoryBean = ctx.getBean("&client-with-connection-string",
				MongoClientFactoryBean.class);

		assertThat(getField(factoryBean, "host")).isNull();
		assertThat(getField(factoryBean, "port")).isNull();
		assertThat(getField(factoryBean, "connectionString"))
				.isEqualTo(new ConnectionString("mongodb://127.0.0.1:27017/?replicaSet=rs0"));
		assertThat(getField(factoryBean, "credential")).isNull();
		assertThat(getField(factoryBean, "replicaSet")).isNull();
		assertThat(getField(factoryBean, "mongoClientSettings")).isNull();
	}

	@Test // DATAMONGO-2384
	public void clientWithReplicaSet() {

		assertThat(ctx.containsBean("client-with-replica-set")).isTrue();
		MongoClientFactoryBean factoryBean = ctx.getBean("&client-with-replica-set",
				MongoClientFactoryBean.class);

		assertThat(getField(factoryBean, "host")).isNull();
		assertThat(getField(factoryBean, "port")).isNull();
		assertThat(getField(factoryBean, "connectionString")).isNull();
		assertThat(getField(factoryBean, "credential")).isNull();
		assertThat(getField(factoryBean, "replicaSet")).isEqualTo("rs0");
		assertThat(getField(factoryBean, "mongoClientSettings")).isNull();
	}

	@Test // DATAMONGO-2384
	public void clientWithCredential() {

		assertThat(ctx.containsBean("client-with-auth")).isTrue();
		MongoClientFactoryBean factoryBean = ctx.getBean("&client-with-auth", MongoClientFactoryBean.class);

		assertThat(getField(factoryBean, "host")).isNull();
		assertThat(getField(factoryBean, "port")).isNull();
		assertThat(getField(factoryBean, "connectionString")).isNull();
		assertThat(getField(factoryBean, "credential")).isEqualTo(
				Collections.singletonList(MongoCredential.createPlainCredential("jon", "snow", "warg".toCharArray())));
		assertThat(getField(factoryBean, "replicaSet")).isNull();
		assertThat(getField(factoryBean, "mongoClientSettings")).isNull();
	}

	@Test // DATAMONGO-2384
	public void clientWithClusterSettings() {

		assertThat(ctx.containsBean("client-with-cluster-settings")).isTrue();
		MongoClientFactoryBean factoryBean = ctx.getBean("&client-with-cluster-settings",
				MongoClientFactoryBean.class);

		MongoClientSettings settings = (MongoClientSettings) getField(factoryBean, "mongoClientSettings");

		assertThat(settings.getClusterSettings().getRequiredClusterType()).isEqualTo(ClusterType.REPLICA_SET);
		assertThat(settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS)).isEqualTo(10);
		assertThat(settings.getClusterSettings().getLocalThreshold(TimeUnit.MILLISECONDS)).isEqualTo(5);
		assertThat(settings.getClusterSettings().getHosts()).contains(new ServerAddress("localhost", 27018),
				new ServerAddress("localhost", 27019), new ServerAddress("localhost", 27020));
	}

	@Test // DATAMONGO-2384
	public void clientWithConnectionPoolSettings() {

		assertThat(ctx.containsBean("client-with-connection-pool-settings")).isTrue();
		MongoClientFactoryBean factoryBean = ctx.getBean("&client-with-connection-pool-settings",
				MongoClientFactoryBean.class);

		MongoClientSettings settings = (MongoClientSettings) getField(factoryBean, "mongoClientSettings");

		assertThat(settings.getConnectionPoolSettings().getMaxConnectionLifeTime(TimeUnit.MILLISECONDS)).isEqualTo(10);
		assertThat(settings.getConnectionPoolSettings().getMinSize()).isEqualTo(10);
		assertThat(settings.getConnectionPoolSettings().getMaxSize()).isEqualTo(20);
		assertThat(settings.getConnectionPoolSettings().getMaintenanceFrequency(TimeUnit.MILLISECONDS)).isEqualTo(10);
		assertThat(settings.getConnectionPoolSettings().getMaintenanceInitialDelay(TimeUnit.MILLISECONDS)).isEqualTo(11);
		assertThat(settings.getConnectionPoolSettings().getMaxConnectionIdleTime(TimeUnit.MILLISECONDS)).isEqualTo(30);
		assertThat(settings.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS)).isEqualTo(15);
	}

}
