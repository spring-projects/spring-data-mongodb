/*
 * Copyright 2011 the original author or authors.
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

import org.junit.Test;

import com.mongodb.MongoOptions;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link MongoOptionsFactoryBean}.
 *
 * @author Oliver Gierke
 */
public class MongoOptionsFactoryBeanUnitTests {

	/**
	 * @see DATADOC-280
	 */
	@Test
	public void setsMaxConnectRetryTime() {
		
		MongoOptionsFactoryBean bean = new MongoOptionsFactoryBean();
		bean.setMaxAutoConnectRetryTime(27);
		bean.afterPropertiesSet();
		
		MongoOptions options = bean.getObject();
		assertThat(options.maxAutoConnectRetryTime, is(27L));
	}
}
