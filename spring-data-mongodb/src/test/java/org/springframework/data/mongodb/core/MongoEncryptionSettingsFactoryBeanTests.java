/*
 * Copyright 2019-2023 the original author or authors.
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

import com.mongodb.AutoEncryptionSettings;

/**
 * Integration tests for {@link MongoEncryptionSettingsFactoryBean}.
 *
 * @author Christoph Strobl
 */
public class MongoEncryptionSettingsFactoryBeanTests {

	@Test // DATAMONGO-2306
	public void createsAutoEncryptionSettings() {

		RootBeanDefinition definition = new RootBeanDefinition(MongoEncryptionSettingsFactoryBean.class);
		definition.getPropertyValues().addPropertyValue("bypassAutoEncryption", true);
		definition.getPropertyValues().addPropertyValue("keyVaultNamespace", "ns");

		DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
		factory.registerBeanDefinition("factory", definition);

		MongoEncryptionSettingsFactoryBean bean = factory.getBean("&factory", MongoEncryptionSettingsFactoryBean.class);
		assertThat(ReflectionTestUtils.getField(bean, "bypassAutoEncryption")).isEqualTo(true);

		AutoEncryptionSettings target = factory.getBean(AutoEncryptionSettings.class);
		assertThat(target.getKeyVaultNamespace()).isEqualTo("ns");
	}
}
