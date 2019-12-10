/*
 * Copyright 2002-2019 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.client.MongoClient;

/**
 * This test class assumes that you are already running the MongoDB server.
 *
 * @author Mark Pollack
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MongoAdminIntegrationTests {

	private static final Log logger = LogFactory.getLog(MongoAdminIntegrationTests.class);

	@Autowired MongoClient mongoClient;

	MongoAdmin mongoAdmin;

	@Before
	public void setUp() {
		mongoAdmin = new MongoAdmin(mongoClient);
	}

	@Test
	public void serverStats() {
		logger.info("stats = " + mongoAdmin.getServerStatus());
	}

	@Test
	public void databaseStats() {
		logger.info(mongoAdmin.getDatabaseStats("testAdminDb"));
	}
}
