/*
 * Copyright 2021-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;

/**
 * Integration tests for {@link MongoServerApiFactoryBean}.
 *
 * @author Christoph Strobl
 */
class MongoServerApiFactoryBeanTests {

	@Test // GH-3820
	void createsServerApiForVersionString() {

		RootBeanDefinition definition = new RootBeanDefinition(MongoServerApiFactoryBean.class);
		definition.getPropertyValues().addPropertyValue("version", "V1");
		definition.getPropertyValues().addPropertyValue("deprecationErrors", "true");

		DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
		factory.registerBeanDefinition("factory", definition);

		MongoServerApiFactoryBean bean = factory.getBean("&factory", MongoServerApiFactoryBean.class);
		assertThat(ReflectionTestUtils.getField(bean, "deprecationErrors")).isEqualTo(true);

		ServerApi target = factory.getBean(ServerApi.class);
		assertThat(target.getVersion()).isEqualTo(ServerApiVersion.V1);
		assertThat(target.getDeprecationErrors()).contains(true);
		assertThat(target.getStrict()).isNotPresent();
	}

	@Test // GH-3820
	void createsServerApiForVersionNumber() {

		RootBeanDefinition definition = new RootBeanDefinition(MongoServerApiFactoryBean.class);
		definition.getPropertyValues().addPropertyValue("version", "1");
		definition.getPropertyValues().addPropertyValue("strict", "true");

		DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
		factory.registerBeanDefinition("factory", definition);

		MongoServerApiFactoryBean bean = factory.getBean("&factory", MongoServerApiFactoryBean.class);
		assertThat(ReflectionTestUtils.getField(bean, "strict")).isEqualTo(true);

		ServerApi target = factory.getBean(ServerApi.class);
		assertThat(target.getVersion()).isEqualTo(ServerApiVersion.V1);
		assertThat(target.getDeprecationErrors()).isNotPresent();
		assertThat(target.getStrict()).contains(true);
	}
}
