/*
 * Copyright 2010 the original author or authors.
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

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoFactoryBean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.CommandResult;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoNamespaceReplicaSetTests extends NamespaceTestSupport {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testParsingMongoWithReplicaSets() throws Exception {
		assertTrue(ctx.containsBean("replicaSetMongo"));
		MongoFactoryBean mfb = (MongoFactoryBean) ctx.getBean("&replicaSetMongo");

		List<ServerAddress> replicaSetSeeds = readField("replicaSetSeeds", mfb);
		assertNotNull(replicaSetSeeds);

		assertEquals("127.0.0.1", replicaSetSeeds.get(0).getHost());
		assertEquals(10001, replicaSetSeeds.get(0).getPort());

		assertEquals("localhost", replicaSetSeeds.get(1).getHost());
		assertEquals(10002, replicaSetSeeds.get(1).getPort());

	}

	@Test
	public void testParsingWithPropertyPlaceHolder() throws Exception {
		assertTrue(ctx.containsBean("manyReplicaSetMongo"));
		MongoFactoryBean mfb = (MongoFactoryBean) ctx.getBean("&manyReplicaSetMongo");

		List<ServerAddress> replicaSetSeeds = readField("replicaSetSeeds", mfb);
		assertNotNull(replicaSetSeeds);

		assertEquals("192.168.174.130", replicaSetSeeds.get(0).getHost());
		assertEquals(27017, replicaSetSeeds.get(0).getPort());
		assertEquals("192.168.174.130", replicaSetSeeds.get(1).getHost());
		assertEquals(27018, replicaSetSeeds.get(1).getPort());
		assertEquals("192.168.174.130", replicaSetSeeds.get(2).getHost());
		assertEquals(27019, replicaSetSeeds.get(2).getPort());

	}

	@Test
	@Ignore("CI infrastructure does not yet support replica sets")
	public void testMongoWithReplicaSets() {
		Mongo mongo = ctx.getBean(Mongo.class);
		assertEquals(2, mongo.getAllAddress().size());
		List<ServerAddress> servers = mongo.getAllAddress();
		assertEquals("127.0.0.1", servers.get(0).getHost());
		assertEquals("localhost", servers.get(1).getHost());
		assertEquals(10001, servers.get(0).getPort());
		assertEquals(10002, servers.get(1).getPort());

		MongoTemplate template = new MongoTemplate(mongo, "admin");
		CommandResult result = template.executeCommand("{replSetGetStatus : 1}");
		assertEquals("blort", result.getString("set"));

	}
}
