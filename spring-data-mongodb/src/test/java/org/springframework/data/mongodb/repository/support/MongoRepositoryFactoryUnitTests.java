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
package org.springframework.data.mongodb.repository.support;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;

/**
 * Unit test for {@link MongoRepositoryFactory}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoRepositoryFactoryUnitTests {

	@Mock
	MongoTemplate template;

	@Mock
	MongoConverter converter;

	@Mock
	MappingContext<MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	@Mock
	@SuppressWarnings("rawtypes")
	MongoPersistentEntity entity;

	@Before
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setUp() {
		when(template.getConverter()).thenReturn(converter);
		when(converter.getMappingContext()).thenReturn((MappingContext) mappingContext);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void usesMappingMongoEntityInformationIfMappingContextSet() {

		when(mappingContext.getPersistentEntity(Person.class)).thenReturn(entity);
		when(entity.getType()).thenReturn(Person.class);

		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		MongoEntityInformation<Person, Serializable> entityInformation = factory.getEntityInformation(Person.class);
		assertTrue(entityInformation instanceof MappingMongoEntityInformation);
	}
}
