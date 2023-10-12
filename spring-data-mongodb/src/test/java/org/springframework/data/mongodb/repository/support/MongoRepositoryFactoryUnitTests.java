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
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.data.repository.Repository;

/**
 * Unit test for {@link MongoRepositoryFactory}.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MongoRepositoryFactoryUnitTests {

	@Mock MongoOperations template;

	MongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, new MongoMappingContext());

	@BeforeEach
	public void setUp() {
		when(template.getConverter()).thenReturn(converter);
	}

	@Test
	public void usesMappingMongoEntityInformationIfMappingContextSet() {

		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		MongoEntityInformation<Person, Serializable> entityInformation = factory.getEntityInformation(Person.class);
		assertThat(entityInformation instanceof MappingMongoEntityInformation).isTrue();
	}

	@Test // DATAMONGO-385
	public void createsRepositoryWithIdTypeLong() {

		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);
		assertThat(repository).isNotNull();
	}

	@Test // GH-2971
	void considersCrudMethodMetadata() {

		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);
		repository.findById(42L);

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(template).findOne(captor.capture(), eq(Person.class), eq("person"));

		Query value = captor.getValue();
		assertThat(value.getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondary());
	}

	@Test // GH-2971
	void ignoresCrudMethodMetadataOnNonAnnotatedMethods() {

		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);
		repository.findAllById(Set.of(42L));

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(template).find(captor.capture(), eq(Person.class), eq("person"));

		Query value = captor.getValue();
		assertThat(value.getReadPreference()).isNull();
	}

	interface MyPersonRepository extends ListCrudRepository<Person, Long> {

		@ReadPreference("secondary")
		Optional<Person> findById(Long id);
	}
}
