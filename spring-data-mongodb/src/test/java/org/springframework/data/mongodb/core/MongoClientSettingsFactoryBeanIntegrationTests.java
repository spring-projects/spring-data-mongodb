/*
 * Copyright 2015-2024 the original author or authors.
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
import org.springframework.data.mongodb.config.ReadPreferencePropertyEditor;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ReadPreference;

/**
 * Integration tests for {@link MongoClientSettingsFactoryBean}.
 *
 * @author Christoph Strobl
 */
public class MongoClientSettingsFactoryBeanIntegrationTests {

	@Test // DATAMONGO-1158
	public void convertsReadPreferenceConcernCorrectly() {

		RootBeanDefinition definition = new RootBeanDefinition(MongoClientSettingsFactoryBean.class);
		definition.getPropertyValues().addPropertyValue("readPreference", "NEAREST");

		DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
		factory.registerCustomEditor(ReadPreference.class, ReadPreferencePropertyEditor.class);

		factory.registerBeanDefinition("factory", definition);

		MongoClientSettingsFactoryBean bean = factory.getBean("&factory", MongoClientSettingsFactoryBean.class);
		assertThat(ReflectionTestUtils.getField(bean, "readPreference")).isEqualTo((Object) ReadPreference.nearest());
	}
}
