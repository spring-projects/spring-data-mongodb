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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClientSettings;
import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoClientFactoryBean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;

/**
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoNamespaceReplicaSetTests {

	@Autowired private ApplicationContext ctx;

	@Test
	@SuppressWarnings("unchecked")
	public void testParsingMongoWithReplicaSets() throws Exception {

		assertThat(ctx.containsBean("replicaSetMongo")).isTrue();
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&replicaSetMongo");

		MongoClientSettings settings = (MongoClientSettings) ReflectionTestUtils.getField(mfb, "mongoClientSettings");
		List<ServerAddress> replicaSetSeeds = settings.getClusterSettings().getHosts();

		assertThat(replicaSetSeeds).isNotNull();
		assertThat(replicaSetSeeds).contains(new ServerAddress(InetAddress.getByName("127.0.0.1"), 10001),
				new ServerAddress(InetAddress.getByName("localhost"), 10002));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testParsingWithPropertyPlaceHolder() throws Exception {

		assertThat(ctx.containsBean("manyReplicaSetMongo")).isTrue();
		MongoClientFactoryBean mfb = (MongoClientFactoryBean) ctx.getBean("&manyReplicaSetMongo");

		MongoClientSettings settings = (MongoClientSettings) ReflectionTestUtils.getField(mfb, "mongoClientSettings");
		List<ServerAddress> replicaSetSeeds = settings.getClusterSettings().getHosts();

		assertThat(replicaSetSeeds).isNotNull();
		assertThat(replicaSetSeeds).hasSize(3);

		List<Integer> ports = new ArrayList<Integer>();
		for (ServerAddress replicaSetSeed : replicaSetSeeds) {
			ports.add(replicaSetSeed.getPort());
		}

		assertThat(ports).contains(27017, 27018, 27019);
	}

	@Test
	@Ignore("CI infrastructure does not yet support replica sets")
	public void testMongoWithReplicaSets() {

		MongoClient mongo = ctx.getBean(MongoClient.class);
		assertThat(mongo.getClusterDescription().getClusterSettings().getHosts()).isEqualTo(2);
		List<ServerAddress> servers = mongo.getClusterDescription().getClusterSettings().getHosts();
		assertThat(servers.get(0).getHost()).isEqualTo("127.0.0.1");
		assertThat(servers.get(1).getHost()).isEqualTo("localhost");
		assertThat(servers.get(0).getPort()).isEqualTo(10001);
		assertThat(servers.get(1).getPort()).isEqualTo(10002);

		MongoTemplate template = new MongoTemplate(mongo, "admin");
		Document result = template.executeCommand("{replSetGetStatus : 1}");
		assertThat(result.get("set").toString()).isEqualTo("blort");
	}
}
