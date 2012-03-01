/*
 * Copyright 2012 the original author or authors.
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

import org.junit.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.data.mongodb.config.WriteConcernPropertyEditor;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.WriteConcern;

/**
 * Integration tests for {@link MongoFactoryBean}.
 * 
 * @author Oliver Gierke
 */
public class MongoFactoryBeanIntegrationTest {

	/**
	 * @see DATAMONGO-408
	 */
	@Test
	public void convertsWriteConcernCorrectly() {

		RootBeanDefinition definition = new RootBeanDefinition(MongoFactoryBean.class);
		definition.getPropertyValues().addPropertyValue("writeConcern", "SAFE");

		DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
		factory.registerCustomEditor(WriteConcern.class, WriteConcernPropertyEditor.class);
		factory.registerBeanDefinition("factory", definition);

		MongoFactoryBean bean = factory.getBean("&factory", MongoFactoryBean.class);
		assertThat(ReflectionTestUtils.getField(bean, "writeConcern"), is((Object) WriteConcern.SAFE));
	}
}
