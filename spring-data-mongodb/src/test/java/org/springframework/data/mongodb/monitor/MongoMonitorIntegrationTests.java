/*
 * Copyright 2002-2024 the original author or authors.
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
package org.springframework.data.mongodb.monitor;

import static org.assertj.core.api.Assertions.*;

import java.net.UnknownHostException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;

import com.mongodb.client.MongoClient;

/**
 * This test class assumes that you are already running the MongoDB server.
 *
 * @author Mark Pollack
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@ExtendWith(MongoClientExtension.class)
public class MongoMonitorIntegrationTests {

	static @Client MongoClient mongoClient;

	@Test
	public void serverInfo() {
		ServerInfo serverInfo = new ServerInfo(mongoClient);
		serverInfo.getVersion();
	}

	@Test // DATAMONGO-685
	public void getHostNameShouldReturnServerNameReportedByMongo() throws UnknownHostException {

		ServerInfo serverInfo = new ServerInfo(mongoClient);

		String hostName = null;
		try {
			hostName = serverInfo.getHostName();
		} catch (UnknownHostException e) {
			throw e;
		}

		assertThat(hostName).isNotNull();
		assertThat(hostName).isEqualTo("127.0.0.1:27017");
	}

	@Test
	public void operationCounters() {
		OperationCounters operationCounters = new OperationCounters(mongoClient);
		operationCounters.getInsertCount();
	}
}
