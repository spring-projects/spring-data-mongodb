/*
 * Copyright 2011-2017 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.core.ReflectiveMongoOptionsInvoker.*;
import static org.springframework.data.mongodb.util.MongoClientVersion.*;

import javax.net.ssl.SSLSocketFactory;

import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoOptions;

/**
 * Unit tests for {@link MongoOptionsFactoryBean}.
 * 
 * @author Oliver Gierke
 * @author Mike Saavedra
 * @author Christoph Strobl
 */
@SuppressWarnings("deprecation")
public class MongoOptionsFactoryBeanUnitTests {

	@BeforeClass
	public static void validateMongoDriver() {
		assumeFalse(isMongo3Driver());
	}

	@Test // DATADOC-280
	public void setsMaxConnectRetryTime() throws Exception {

		MongoOptionsFactoryBean bean = new MongoOptionsFactoryBean();
		bean.setMaxAutoConnectRetryTime(27);
		bean.afterPropertiesSet();

		MongoOptions options = bean.getObject();
		assertThat(getMaxAutoConnectRetryTime(options), is(27L));
	}

	@Test // DATAMONGO-764
	public void testSslConnection() throws Exception {

		MongoOptionsFactoryBean bean = new MongoOptionsFactoryBean();
		bean.setSsl(true);
		bean.afterPropertiesSet();

		MongoOptions options = bean.getObject();
		assertNotNull(options.getSocketFactory());
		assertTrue(options.getSocketFactory() instanceof SSLSocketFactory);
	}
}
