/*
 * Copyright 2002-2011 the original author or authors.
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
package org.springframework.data.document.mongodb.monitor;

import com.mongodb.Mongo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * This test class assumes that you are already running the MongoDB server.
 * 
 * @author Mark Pollack
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoMonitorIntegrationTests {

	@Autowired
	Mongo mongo;

	@Test
	public void serverInfo() {
		ServerInfo serverInfo = new ServerInfo(mongo);
		String version = serverInfo.getVersion();
		Assert.isTrue(StringUtils.hasText("1."));
	}

	@Test
	public void operationCounters() {
		OperationCounters operationCounters = new OperationCounters(mongo);
		operationCounters.getInsertCount();
	}
}