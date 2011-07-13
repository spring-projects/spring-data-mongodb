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
package org.springframework.data.mongodb.repository;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.repository.MongoRepositoryFactoryBean;
import org.springframework.data.mongodb.repository.MongoRepositoryFactoryBean.IndexEnsuringQueryCreationListener;
import org.springframework.data.repository.core.support.QueryCreationListener;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link MongoRepositoryFactoryBean}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoRepositoryFactoryBeanUnitTests {

	@Mock
	MongoTemplate template;

	@Mock
	MongoConverter converter;

	@Mock
	@SuppressWarnings("rawtypes")
	MappingContext context;

	@Test
	@SuppressWarnings("rawtypes")
	public void addsIndexEnsuringQueryCreationListenerIfConfigured() {

		MongoRepositoryFactoryBean factory = new MongoRepositoryFactoryBean();
		factory.setCreateIndexesForQueryMethods(true);

		List<QueryCreationListener<?>> listeners = getListenersFromFactory(factory);
		assertThat(listeners.size(), is(1));
		assertThat(listeners.get(0), is(instanceOf(IndexEnsuringQueryCreationListener.class)));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void doesNotAddIndexEnsuringQueryCreationListenerByDefault() {

		List<QueryCreationListener<?>> listeners = getListenersFromFactory(new MongoRepositoryFactoryBean());
		assertThat(listeners.isEmpty(), is(true));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<QueryCreationListener<?>> getListenersFromFactory(MongoRepositoryFactoryBean factoryBean) {

		when(template.getConverter()).thenReturn(converter);
		when(converter.getMappingContext()).thenReturn(context);

		factoryBean.setTemplate(template);
		factoryBean.afterPropertiesSet();

		RepositoryFactorySupport factory = factoryBean.createRepositoryFactory();
		return (List<QueryCreationListener<?>>) ReflectionTestUtils.getField(factory, "queryPostProcessors");
	}
}
