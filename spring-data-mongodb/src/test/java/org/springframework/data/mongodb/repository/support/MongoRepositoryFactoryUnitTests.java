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

import java.io.Serializable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;

/**
 * Unit test for {@link MongoRepositoryFactory}.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MongoRepositoryFactoryUnitTests {

	@Mock MongoOperations template;

	@Mock MongoConverter converter;

	@Mock @SuppressWarnings("rawtypes") MappingContext mappingContext;

	@Mock @SuppressWarnings("rawtypes") MongoPersistentEntity entity;

	@BeforeEach
	@SuppressWarnings("unchecked")
	public void setUp() {
		when(template.getConverter()).thenReturn(converter);
		when(converter.getMappingContext()).thenReturn(mappingContext);
		when(converter.getProjectionFactory()).thenReturn(new SpelAwareProxyProjectionFactory());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void usesMappingMongoEntityInformationIfMappingContextSet() {

		when(mappingContext.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		MongoEntityInformation<Person, Serializable> entityInformation = factory.getEntityInformation(Person.class);
		assertThat(entityInformation instanceof MappingMongoEntityInformation).isTrue();
	}

	@Test // DATAMONGO-385
	@SuppressWarnings("unchecked")
	public void createsRepositoryWithIdTypeLong() {

		when(mappingContext.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);
		assertThat(repository).isNotNull();
	}

	interface MyPersonRepository extends Repository<Person, Long> {

	}
}
