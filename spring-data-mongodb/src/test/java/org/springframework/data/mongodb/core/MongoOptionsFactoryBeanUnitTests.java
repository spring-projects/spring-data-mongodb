/*
 * Copyright 2011-2018 the original author or authors.
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

import static org.junit.Assert.*;

import javax.net.ssl.SSLSocketFactory;

import com.mongodb.management.JMXConnectionPoolListener;
import org.junit.Test;

import com.mongodb.MongoClientOptions;

/**
 * Unit tests for {@link MongoOptionsFactoryBean}.
 *
 * @author Oliver Gierke
 * @author Mike Saavedra
 * @author Christoph Strobl
 * @author Stephen Tyler Conrad
 */
@SuppressWarnings("deprecation")
public class MongoOptionsFactoryBeanUnitTests {

	@Test // DATAMONGO-764
	public void testSslConnection() throws Exception {

		MongoClientOptionsFactoryBean bean = new MongoClientOptionsFactoryBean();
		bean.setSsl(true);
		bean.afterPropertiesSet();

		MongoClientOptions options = bean.getObject();
		assertNotNull(options.getSocketFactory());
		assertTrue(options.getSocketFactory() instanceof SSLSocketFactory);
	}

	@Test // DATAMONGO-2024
	public void testEnablingJmxConnectionListener() throws Exception {

		MongoClientOptionsFactoryBean bean = new MongoClientOptionsFactoryBean();
		bean.setEnableJmxConnectionPoolListener(true);
		bean.afterPropertiesSet();

		MongoClientOptions options = bean.getObject();
		assertFalse(options.getConnectionPoolListeners().isEmpty());
		assertTrue(options.getConnectionPoolListeners().get(0) instanceof JMXConnectionPoolListener);
	}

	@Test // DATAMONGO-2024
	public void testJmxConnectionListenerDisabledByDefault() throws Exception {

		MongoClientOptionsFactoryBean bean = new MongoClientOptionsFactoryBean();
		bean.afterPropertiesSet();

		MongoClientOptions options = bean.getObject();
		assertTrue(options.getConnectionPoolListeners().isEmpty());
	}
}
