/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.repository.ContactRepository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link MongoRepositoryFactoryBean}.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
public class MongoRepositoryFactoryBeanUnitTests {

	@Mock MongoOperations operations;
	@Mock MongoConverter converter;
	@Mock @SuppressWarnings("rawtypes") MappingContext context;

	@Test
	@SuppressWarnings("rawtypes")
	public void addsIndexEnsuringQueryCreationListenerIfConfigured() {

		MongoRepositoryFactoryBean factory = new MongoRepositoryFactoryBean(ContactRepository.class);
		factory.setCreateIndexesForQueryMethods(true);

		List<Object> listeners = getListenersFromFactory(factory);
		assertThat(listeners.isEmpty()).isFalse();
		assertThat(listeners.stream().filter(IndexEnsuringQueryCreationListener.class::isInstance)).isNotEmpty();
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void doesNotAddIndexEnsuringQueryCreationListenerByDefault() {

		List<Object> listeners = getListenersFromFactory(new MongoRepositoryFactoryBean(ContactRepository.class));
		assertThat(listeners.size()).isEqualTo(1);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<Object> getListenersFromFactory(MongoRepositoryFactoryBean factoryBean) {

		when(operations.getConverter()).thenReturn(converter);
		when(converter.getMappingContext()).thenReturn(context);

		factoryBean.setLazyInit(true);
		factoryBean.setMongoOperations(operations);
		factoryBean.afterPropertiesSet();

		RepositoryFactorySupport factory = factoryBean.createRepositoryFactory();
		return (List<Object>) ReflectionTestUtils.getField(factory, "queryPostProcessors");
	}
}
