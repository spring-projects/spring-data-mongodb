/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.core;

import com.mongodb.MongoOptions;

/**
 * Helper class allowing to keep {@link ReflectiveMongoOptionsInvoker} within default visibility while using it publicly
 * across tests.
 * 
 * @author Christoph Strobl
 */
public class ReflectiveMongoOptionsInvokerTestUtil {

	public static void setAutoConnectRetry(MongoOptions options, boolean autoConnectRetry) {
		ReflectiveMongoOptionsInvoker.setAutoConnectRetry(options, autoConnectRetry);
	}

	public static void setMaxAutoConnectRetryTime(MongoOptions options, long maxAutoConnectRetryTime) {
		ReflectiveMongoOptionsInvoker.setMaxAutoConnectRetryTime(options, maxAutoConnectRetryTime);
	}

	public static void setSlaveOk(MongoOptions options, boolean slaveOk) {
		ReflectiveMongoOptionsInvoker.setSlaveOk(options, slaveOk);
	}

	public static boolean getSlaveOk(MongoOptions options) {
		return ReflectiveMongoOptionsInvoker.getSlaveOk(options);
	}

	public static boolean getAutoConnectRetry(MongoOptions options) {
		return ReflectiveMongoOptionsInvoker.getAutoConnectRetry(options);
	}

	public static long getMaxAutoConnectRetryTime(MongoOptions options) {
		return ReflectiveMongoOptionsInvoker.getMaxAutoConnectRetryTime(options);
	}

}
